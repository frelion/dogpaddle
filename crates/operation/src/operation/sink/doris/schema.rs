use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};

use super::error::DorisSinkSchemaError;

pub(super) const TECHNICAL_DELETED: &str = "__dogpaddle_deleted";
pub(super) const TECHNICAL_HASH: &str = "__dogpaddle_hash";
pub(super) const TECHNICAL_ID: &str = "__dogpaddle_id";
pub(super) const PUBLIC_TECHNICAL_HASH: &str = "$dogpaddle.hash";
pub(super) const PUBLIC_TECHNICAL_ID: &str = "$dogpaddle.id";
pub(super) const TECHNICAL_HASH_INDEX: &str = "__dogpaddle_hash_idx";
pub(super) const MAX_LOGICAL_COLUMNS: usize = 1_597;

#[derive(Debug)]
pub(super) struct DorisLayout {
    schema: SchemaRef,
    columns: Box<[ColumnLayout]>,
}

impl DorisLayout {
    pub(super) fn try_new(schema: SchemaRef) -> Result<Self, DorisSinkSchemaError> {
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
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Utf8,
    Encoded,
    Null,
}

impl StorageType {
    pub(super) const fn sql(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::TinyInt => "TINYINT",
            Self::SmallInt => "SMALLINT",
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Utf8 | Self::Encoded | Self::Null => "STRING",
        }
    }

    pub(super) const fn catalog_type(self) -> &'static str {
        match self {
            Self::Boolean => "tinyint(1)",
            Self::TinyInt => "tinyint(4)",
            Self::SmallInt => "smallint(6)",
            Self::Int => "int(11)",
            Self::BigInt => "bigint(20)",
            Self::Utf8 | Self::Encoded | Self::Null => "string",
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
    fn try_new(field: &Field) -> Result<Self, DorisSinkSchemaError> {
        let storage = match field.data_type() {
            DataType::Null => StorageType::Null,
            DataType::Boolean => StorageType::Boolean,
            DataType::Int8 => StorageType::TinyInt,
            DataType::Int16 | DataType::UInt8 => StorageType::SmallInt,
            DataType::Int32 | DataType::UInt16 | DataType::Date32 => StorageType::Int,
            DataType::Int64 | DataType::UInt32 | DataType::Timestamp(_, _) => StorageType::BigInt,
            DataType::Utf8 => StorageType::Utf8,
            DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Binary
            | DataType::List(_)
            | DataType::Struct(_) => StorageType::Encoded,
            unsupported => {
                return Err(DorisSinkSchemaError::UnsupportedType {
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

    pub(super) const fn storage(&self) -> StorageType {
        self.storage
    }

    pub(super) const fn nullable(&self) -> bool {
        self.nullable
    }
}

fn validate_identifiers(schema: &SchemaRef) -> Result<(), DorisSinkSchemaError> {
    let actual = schema.fields().len();
    if actual > MAX_LOGICAL_COLUMNS {
        return Err(DorisSinkSchemaError::TooManyColumns {
            actual,
            maximum: MAX_LOGICAL_COLUMNS,
        });
    }
    let mut names = BTreeMap::new();
    for (field, logical) in schema.fields().iter().enumerate() {
        let name = logical.name();
        if name.is_empty() || name.len() > 64 || name.contains('\0') {
            return Err(DorisSinkSchemaError::InvalidFieldName {
                field,
                name: name.clone(),
            });
        }
        if [
            TECHNICAL_ID,
            TECHNICAL_HASH,
            TECHNICAL_DELETED,
            PUBLIC_TECHNICAL_ID,
            PUBLIC_TECHNICAL_HASH,
        ]
        .iter()
        .any(|technical| name.eq_ignore_ascii_case(technical))
        {
            return Err(DorisSinkSchemaError::TechnicalColumnCollision {
                field,
                name: name.clone(),
            });
        }
        if let Some(first) = names.insert(name.to_ascii_lowercase(), field) {
            return Err(DorisSinkSchemaError::DuplicateFieldName {
                first,
                second: field,
                name: name.clone(),
            });
        }
    }
    Ok(())
}
