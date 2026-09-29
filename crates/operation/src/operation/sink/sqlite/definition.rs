use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, SchemaRef};
use thiserror::Error;

use super::{TECHNICAL_HASH, TECHNICAL_ID, buffered, target::SqliteTarget};
use crate::{
    ConstructedOperation, DefinitionCodecError,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::schema_error,
};

pub(crate) const TAG: u16 = 10;
const MAX_LOGICAL_COLUMNS: usize = 1_998;

/// Pure definition of a sink that materializes its input relation in `SQLite`.
///
/// The definition only stores the absolute database path and target table
/// name. Binding is pure, and neither opens the database nor creates the table;
/// those effects are deferred to lazy runtime initialization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SqliteSinkDefinition {
    database_path: PathBuf,
    table_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    database_path: PathBuf,
    table_name: String,
}

impl Payload {
    fn into_definition(self) -> Result<SqliteSinkDefinition, &'static str> {
        SqliteSinkDefinition::try_new(self.database_path, self.table_name)
            .map_err(|_| "SQLite sink definition is invalid")
    }
}

impl<'de> Deserialize<'de> for SqliteSinkDefinition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Payload::deserialize(deserializer)?
            .into_definition()
            .map_err(D::Error::custom)
    }
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

impl SqliteSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        validate_input_schema(&inputs[0])?;
        Ok(())
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces SQLiteSink input arity");
        validate_input_schema(input_schema).map_err(schema_error)?;

        let input_schema = Arc::clone(input_schema);
        let target = SqliteTarget::try_new(
            self.database_path.clone(),
            self.table_name.clone(),
            Arc::clone(&input_schema),
        )
        .map_err(schema_error)?;
        buffered::construct(input_schema, target, data)
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

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<SqliteSinkDefinition>, DefinitionCodecError> {
    let definition = parse_json_payload::<Payload>(payload)?
        .into_definition()
        .map_err(DefinitionCodecError::InvalidPayload)?;
    require_canonical_json_payload(&definition, payload, "invalid SQLite sink payload")?;
    Ok(Box::new(definition))
}
