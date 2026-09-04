use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, SchemaRef};
use dogpaddle_store::Cell;
use thiserror::Error;

use super::{
    TECHNICAL_HASH, TECHNICAL_ID,
    runtime::{SqliteSinkCompiled, SqliteSinkOperation},
};
use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::{DataName, Sealed as SealedDefinition},
    operation::Operation,
};

pub(crate) const TAG: u16 = 10;
const MAX_LOGICAL_COLUMNS: usize = 1_998;
const NEXT_ID: DataName<Cell<u64>> = DataName::new("sqlite_sink.next_id");
const PENDING: DataName<Cell<Vec<u8>>> = DataName::new("sqlite_sink.pending");
const DATA: &[DataDeclaration] = &[NEXT_ID.declaration(), PENDING.declaration()];

/// Pure definition of a sink that materializes its input relation in `SQLite`.
///
/// The definition only stores the absolute database path and target table
/// name. Binding is pure, and neither opens the database nor creates the table;
/// those effects begin on the materialized operation's first turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteSinkDefinition {
    database_path: PathBuf,
    table_name: String,
}

/// Failure while constructing a [`SqliteSinkDefinition`].
#[derive(Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SqliteSinkDefinitionError {
    /// The database path is not valid UTF-8 and cannot be persisted canonically.
    #[error("SQLite sink database path is not valid UTF-8")]
    DatabasePathNotUtf8,
    /// The special in-memory `SQLite` database cannot survive process restart.
    #[error("SQLite sink does not accept an in-memory database")]
    InMemoryDatabase,
    /// The database path is not absolute and would depend on the process directory.
    #[error("SQLite sink database path must be absolute")]
    DatabasePathNotAbsolute,
    /// The database path contains a NUL byte rejected by `SQLite`.
    #[error("SQLite sink database path contains a NUL byte")]
    DatabasePathContainsNul,
    /// The database path cannot fit the stable v1 definition format.
    #[error("SQLite sink database path is too long for the stable format")]
    DatabasePathTooLong,
    /// `SQLite` target table names must not be empty.
    #[error("SQLite sink table name must not be empty")]
    EmptyTableName,
    /// The target table name contains a NUL byte rejected by `SQLite`.
    #[error("SQLite sink table name contains a NUL byte")]
    TableNameContainsNul,
    /// `SQLite` reserves names beginning with `sqlite_` for internal objects.
    #[error("SQLite sink table name must not use the sqlite_ prefix")]
    ReservedTableName,
    /// The target table name cannot fit the stable v1 definition format.
    #[error("SQLite sink table name is too long for the stable format")]
    TableNameTooLong,
}

/// `SQLite`-specific failure while binding an exact logical input Schema.
#[derive(Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SqliteSinkSchemaError {
    /// The logical columns plus two technical columns exceed `SQLite`'s v1 limit.
    #[error("SQLite sink input has {actual} logical columns, exceeding the maximum of {maximum}")]
    TooManyColumns {
        /// Number of top-level logical columns supplied by the input Schema.
        actual: usize,
        /// Maximum number of top-level logical columns supported by this sink.
        maximum: usize,
    },
    /// A top-level field name contains a NUL byte rejected by `SQLite`.
    #[error("SQLite sink field {field} name contains a NUL byte")]
    FieldNameContainsNul {
        /// Zero-based index of the rejected top-level field.
        field: usize,
    },
    /// A logical name collides with a sink-owned technical column under
    /// `SQLite`'s ASCII case-insensitive identifier matching.
    #[error("SQLite sink field {field} name {name:?} conflicts with a technical column")]
    TechnicalColumnCollision {
        /// Zero-based index of the rejected top-level field.
        field: usize,
        /// Rejected logical field name.
        name: String,
    },
    /// Two logical columns collide under `SQLite`'s ASCII case-insensitive
    /// identifier matching.
    #[error(
        "SQLite sink fields {first} and {second} collide as ASCII case-insensitive identifiers"
    )]
    CaseInsensitiveFieldCollision {
        /// Zero-based index of the first top-level field.
        first: usize,
        /// Zero-based index of the later conflicting top-level field.
        second: usize,
    },
    /// A future `DogPaddle` type reached `SQLite` before its storage mapping existed.
    #[error("SQLite sink has no storage mapping for field {field:?} with type {data_type}")]
    UnsupportedType {
        /// Name of the unsupported top-level field.
        field: String,
        /// Arrow type without a `SQLite` v1 representation.
        data_type: DataType,
    },
}

impl SqliteSinkDefinition {
    /// Creates a persistent `SQLite` sink definition.
    ///
    /// The database path must be an absolute UTF-8 file path. `SQLite` in-memory
    /// databases are intentionally rejected because they cannot participate in
    /// the sink's crash-replay protocol.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteSinkDefinitionError`] when the path or table name cannot
    /// be represented safely and canonically by `SQLiteSink` v1.
    pub fn try_new(
        database_path: impl Into<PathBuf>,
        table_name: impl Into<String>,
    ) -> Result<Self, SqliteSinkDefinitionError> {
        let database_path = database_path.into();
        let table_name = table_name.into();
        validate_definition(&database_path, &table_name)?;
        Ok(Self {
            database_path,
            table_name,
        })
    }

    /// Returns the absolute `SQLite` database file path.
    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Returns the target `SQLite` table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl SealedDefinition for SqliteSinkDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces SQLiteSink input arity");
        validate_input_schema(input_schema)
            .map_err(|source| -> OperationSchemaError { Box::new(source) })?;

        let compiled = SqliteSinkCompiled::try_new(
            self.database_path.clone(),
            self.table_name.clone(),
            Arc::clone(input_schema),
        )
        .map_err(|source| -> OperationSchemaError { Box::new(source) })?;
        Ok(OperationBinding::new(
            None,
            move |data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                let next_id = data.take(&NEXT_ID)?;
                let pending = data.take(&PENDING)?;
                Ok(Box::new(SqliteSinkOperation::new_bound(
                    compiled, next_id, pending,
                )))
            },
        ))
    }
}

impl OperationDefinition for SqliteSinkDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Sink(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        let database_path = self
            .database_path
            .to_str()
            .expect("SqliteSinkDefinition::try_new accepted only UTF-8 paths");
        encode_string(database_path, output);
        encode_string(&self.table_name, output);
    }
}

pub(super) fn validate_input_schema(input_schema: &SchemaRef) -> Result<(), SqliteSinkSchemaError> {
    let actual = input_schema.fields().len();
    if actual > MAX_LOGICAL_COLUMNS {
        return Err(SqliteSinkSchemaError::TooManyColumns {
            actual,
            maximum: MAX_LOGICAL_COLUMNS,
        });
    }

    let mut identifiers = BTreeMap::new();
    for (field, logical_field) in input_schema.fields().iter().enumerate() {
        let name = logical_field.name();
        if name.contains('\0') {
            return Err(SqliteSinkSchemaError::FieldNameContainsNul { field });
        }
        let normalized = name.to_ascii_lowercase();
        if normalized == TECHNICAL_ID || normalized == TECHNICAL_HASH {
            return Err(SqliteSinkSchemaError::TechnicalColumnCollision {
                field,
                name: name.clone(),
            });
        }
        if let Some(&first) = identifiers.get(&normalized) {
            return Err(SqliteSinkSchemaError::CaseInsensitiveFieldCollision {
                first,
                second: field,
            });
        }
        identifiers.insert(normalized, field);
    }
    Ok(())
}

fn validate_definition(
    database_path: &Path,
    table_name: &str,
) -> Result<(), SqliteSinkDefinitionError> {
    let database_path = database_path
        .to_str()
        .ok_or(SqliteSinkDefinitionError::DatabasePathNotUtf8)?;
    if database_path == ":memory:" {
        return Err(SqliteSinkDefinitionError::InMemoryDatabase);
    }
    if !Path::new(database_path).is_absolute() {
        return Err(SqliteSinkDefinitionError::DatabasePathNotAbsolute);
    }
    if database_path.contains('\0') {
        return Err(SqliteSinkDefinitionError::DatabasePathContainsNul);
    }
    if u32::try_from(database_path.len()).is_err() {
        return Err(SqliteSinkDefinitionError::DatabasePathTooLong);
    }

    if table_name.is_empty() {
        return Err(SqliteSinkDefinitionError::EmptyTableName);
    }
    if table_name.contains('\0') {
        return Err(SqliteSinkDefinitionError::TableNameContainsNul);
    }
    if table_name.to_ascii_lowercase().starts_with("sqlite_") {
        return Err(SqliteSinkDefinitionError::ReservedTableName);
    }
    if u32::try_from(table_name.len()).is_err() {
        return Err(SqliteSinkDefinitionError::TableNameTooLong);
    }
    Ok(())
}

fn encode_string(value: &str, output: &mut Vec<u8>) {
    let length = u32::try_from(value.len())
        .expect("SqliteSinkDefinition::try_new validated stable string lengths");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let database_path = decode_string(
        &mut cursor,
        "SQLite sink database-path length is invalid",
        "SQLite sink database path is invalid UTF-8",
    )?;
    let table_name = decode_string(
        &mut cursor,
        "SQLite sink table-name length is invalid",
        "SQLite sink table name is invalid UTF-8",
    )?;
    cursor.finish()?;
    let definition = SqliteSinkDefinition::try_new(PathBuf::from(database_path), table_name)
        .map_err(|_| DefinitionCodecError::InvalidPayload("SQLite sink definition is invalid"))?;
    Ok(Box::new(definition))
}

fn decode_string(
    cursor: &mut PayloadCursor<'_>,
    invalid_length: &'static str,
    invalid_utf8: &'static str,
) -> Result<String, DefinitionCodecError> {
    let length = usize::try_from(cursor.read_u32()?)
        .map_err(|_| DefinitionCodecError::InvalidPayload(invalid_length))?;
    let bytes = cursor.read_bytes(length)?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| DefinitionCodecError::InvalidPayload(invalid_utf8))?;
    Ok(value.to_owned())
}
