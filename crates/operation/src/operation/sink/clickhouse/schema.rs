use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};

use super::error::ClickHouseSinkSchemaError;

pub(super) const TECHNICAL_DELETED: &str = "$dogpaddle.deleted";
pub(super) const TECHNICAL_HASH: &str = "$dogpaddle.hash";
pub(super) const TECHNICAL_ID: &str = "$dogpaddle.id";
pub(super) const TECHNICAL_VERSION: &str = "$dogpaddle.version";
pub(super) const TECHNICAL_HASH_INDEX: &str = "$dogpaddle.hash.index";
pub(super) const MAX_LOGICAL_COLUMNS: usize = 4_092;

/// Checks all identifiers before checking the backend's supported type mapping.
pub(super) fn validate(schema: &SchemaRef) -> Result<(), ClickHouseSinkSchemaError> {
    validate_identifiers(schema)?;
    for field in schema.fields() {
        if storage_type(field.data_type()).is_none() {
            return Err(ClickHouseSinkSchemaError::UnsupportedType {
                field: field.name().clone(),
                data_type: field.data_type().clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn nullable(field: &Field) -> bool {
    field.is_nullable() || matches!(field.data_type(), DataType::Null)
}

pub(super) fn sql_type(field: &Field) -> String {
    let storage =
        storage_type(field.data_type()).expect("the bound Schema has a ClickHouse storage mapping");
    if nullable(field) {
        format!("Nullable({storage})")
    } else {
        storage.to_owned()
    }
}

fn storage_type(data_type: &DataType) -> Option<&'static str> {
    Some(match data_type {
        DataType::Boolean | DataType::UInt8 => "UInt8",
        DataType::Int8 => "Int8",
        DataType::Int16 => "Int16",
        DataType::Int32 | DataType::Date32 => "Int32",
        DataType::Int64 | DataType::Timestamp(_, _) => "Int64",
        DataType::UInt16 => "UInt16",
        DataType::UInt32 => "UInt32",
        DataType::UInt64 => "UInt64",
        DataType::Null
        | DataType::Utf8
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => "String",
        _ => return None,
    })
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
