use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};

use super::error::ClickHouseSinkSchemaError;

pub(super) const TECHNICAL_DELETED: &str = "$dogpaddle.deleted";
pub(super) const TECHNICAL_HASH: &str = "$dogpaddle.hash";
pub(super) const TECHNICAL_ID: &str = "$dogpaddle.id";
pub(super) const TECHNICAL_VERSION: &str = "$dogpaddle.version";
pub(super) const TECHNICAL_HASH_INDEX: &str = "$dogpaddle.hash.index";
pub(super) const MAX_LOGICAL_COLUMNS: usize = 4_092;

#[derive(Debug)]
pub(super) struct ClickHouseLayout {
    schema: SchemaRef,
    columns: Box<[ColumnLayout]>,
}

impl ClickHouseLayout {
    pub(super) fn try_new(schema: SchemaRef) -> Result<Self, ClickHouseSinkSchemaError> {
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
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
}

impl StorageType {
    pub(super) const fn sql(self) -> &'static str {
        match self {
            Self::UInt8 => "UInt8",
            Self::UInt16 => "UInt16",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::String => "String",
        }
    }
}

#[derive(Debug)]
pub(super) struct ColumnLayout {
    name: String,
    storage: StorageType,
    nullable: bool,
}

impl ColumnLayout {
    fn try_new(field: &Field) -> Result<Self, ClickHouseSinkSchemaError> {
        let storage = match field.data_type() {
            DataType::Boolean | DataType::UInt8 => StorageType::UInt8,
            DataType::Int8 => StorageType::Int8,
            DataType::Int16 => StorageType::Int16,
            DataType::Int32 | DataType::Date32 => StorageType::Int32,
            DataType::Int64 | DataType::Timestamp(_, _) => StorageType::Int64,
            DataType::UInt16 => StorageType::UInt16,
            DataType::UInt32 => StorageType::UInt32,
            DataType::UInt64 => StorageType::UInt64,
            DataType::Null
            | DataType::Utf8
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Binary
            | DataType::List(_)
            | DataType::Struct(_) => StorageType::String,
            unsupported => {
                return Err(ClickHouseSinkSchemaError::UnsupportedType {
                    field: field.name().clone(),
                    data_type: unsupported.clone(),
                });
            }
        };
        Ok(Self {
            name: field.name().clone(),
            storage,
            nullable: field.is_nullable() || matches!(field.data_type(), DataType::Null),
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) const fn encoded(&self) -> bool {
        matches!(self.storage, StorageType::String)
    }

    pub(super) fn sql_type(&self) -> String {
        if self.nullable {
            format!("Nullable({})", self.storage.sql())
        } else {
            self.storage.sql().to_owned()
        }
    }
}

fn validate_identifiers(schema: &SchemaRef) -> Result<(), ClickHouseSinkSchemaError> {
    let actual = schema.fields().len();
    if actual > MAX_LOGICAL_COLUMNS {
        return Err(ClickHouseSinkSchemaError::TooManyColumns {
            actual,
            maximum: MAX_LOGICAL_COLUMNS,
        });
    }
    let mut names = BTreeMap::new();
    for (field, logical) in schema.fields().iter().enumerate() {
        let name = logical.name();
        if name.is_empty() || name.len() > 255 || name.contains('\0') {
            return Err(ClickHouseSinkSchemaError::InvalidFieldName {
                field,
                name: name.clone(),
            });
        }
        if [
            TECHNICAL_ID,
            TECHNICAL_HASH,
            TECHNICAL_VERSION,
            TECHNICAL_DELETED,
        ]
        .contains(&name.as_str())
        {
            return Err(ClickHouseSinkSchemaError::TechnicalColumnCollision {
                field,
                name: name.clone(),
            });
        }
        if let Some(first) = names.insert(name.clone(), field) {
            return Err(ClickHouseSinkSchemaError::DuplicateFieldName {
                first,
                second: field,
                name: name.clone(),
            });
        }
    }
    Ok(())
}
