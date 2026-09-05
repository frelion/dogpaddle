use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};

use super::error::PostgresSinkSchemaError;

pub(super) const TECHNICAL_HASH: &str = "$dogpaddle.hash";
pub(super) const TECHNICAL_ID: &str = "$dogpaddle.id";
pub(super) const MAX_LOGICAL_COLUMNS: usize = 1_598;
const SYSTEM_COLUMNS: &[&str] = &["tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"];

/// Pure `PostgreSQL` storage layout compiled from one exact logical Schema.
#[derive(Debug)]
pub(super) struct PostgresLayout {
    schema: SchemaRef,
    columns: Box<[ColumnLayout]>,
}

impl PostgresLayout {
    pub(super) fn try_new(schema: SchemaRef) -> Result<Self, PostgresSinkSchemaError> {
        validate_identifiers(&schema)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| ColumnLayout::try_new(field))
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        Ok(Self { schema, columns })
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn columns(&self) -> &[ColumnLayout] {
        &self.columns
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StorageType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Bytes(Option<usize>),
}

impl StorageType {
    pub(super) const fn sql(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int16 => "smallint",
            Self::Int32 => "integer",
            Self::Int64 => "bigint",
            Self::Bytes(_) => "bytea",
        }
    }
}

/// One target column and the checks required for a lossless Arrow mapping.
#[derive(Debug)]
pub(super) struct ColumnLayout {
    name: String,
    storage: StorageType,
    nullable: bool,
    check: Option<&'static str>,
}

impl ColumnLayout {
    fn try_new(field: &Field) -> Result<Self, PostgresSinkSchemaError> {
        let (storage, check) = match field.data_type() {
            DataType::Null => (StorageType::Bytes(None), Some("IS NULL")),
            DataType::Boolean => (StorageType::Boolean, None),
            DataType::Int8 => (StorageType::Int16, Some("BETWEEN -128 AND 127")),
            DataType::Int16 => (StorageType::Int16, None),
            DataType::Int32 | DataType::Date32 => (StorageType::Int32, None),
            DataType::Int64 | DataType::Timestamp(_, _) => (StorageType::Int64, None),
            DataType::UInt8 => (StorageType::Int16, Some("BETWEEN 0 AND 255")),
            DataType::UInt16 => (StorageType::Int32, Some("BETWEEN 0 AND 65535")),
            DataType::UInt32 => (StorageType::Int64, Some("BETWEEN 0 AND 4294967295")),
            DataType::UInt64 | DataType::Float64 => (StorageType::Bytes(Some(8)), None),
            DataType::Float32 => (StorageType::Bytes(Some(4)), None),
            DataType::Decimal128(_, _) => (StorageType::Bytes(Some(16)), None),
            DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Struct(_) => {
                (StorageType::Bytes(None), None)
            }
            unsupported => {
                return Err(PostgresSinkSchemaError::UnsupportedType {
                    field: field.name().clone(),
                    data_type: unsupported.clone(),
                });
            }
        };
        Ok(Self {
            name: field.name().clone(),
            storage,
            nullable: field.is_nullable() || matches!(field.data_type(), DataType::Null),
            check,
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) const fn storage(&self) -> StorageType {
        self.storage
    }

    pub(super) const fn nullable(&self) -> bool {
        self.nullable
    }

    pub(super) const fn check(&self) -> Option<&'static str> {
        self.check
    }
}

fn validate_identifiers(schema: &SchemaRef) -> Result<(), PostgresSinkSchemaError> {
    let actual = schema.fields().len();
    if actual > MAX_LOGICAL_COLUMNS {
        return Err(PostgresSinkSchemaError::TooManyColumns {
            actual,
            maximum: MAX_LOGICAL_COLUMNS,
        });
    }

    let mut names = BTreeMap::new();
    for (field, logical) in schema.fields().iter().enumerate() {
        let name = logical.name();
        if name.is_empty() || name.len() > 63 || name.contains('\0') {
            return Err(PostgresSinkSchemaError::InvalidFieldName {
                field,
                name: name.clone(),
            });
        }
        if name == TECHNICAL_ID || name == TECHNICAL_HASH {
            return Err(PostgresSinkSchemaError::TechnicalColumnCollision {
                field,
                name: name.clone(),
            });
        }
        if SYSTEM_COLUMNS.contains(&name.as_str()) {
            return Err(PostgresSinkSchemaError::SystemColumnCollision {
                field,
                name: name.clone(),
            });
        }
        if let Some(first) = names.insert(name.clone(), field) {
            return Err(PostgresSinkSchemaError::DuplicateFieldName {
                first,
                second: field,
                name: name.clone(),
            });
        }
    }
    Ok(())
}
