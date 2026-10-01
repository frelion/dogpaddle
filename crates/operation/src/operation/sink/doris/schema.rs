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

/// Checks all identifiers before checking the backend's supported type mapping.
pub(super) fn validate(schema: &SchemaRef) -> Result<(), DorisSinkSchemaError> {
    validate_identifiers(schema)?;
    for field in schema.fields() {
        if storage_type(field.data_type()).is_none() {
            return Err(DorisSinkSchemaError::UnsupportedType {
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

pub(super) fn sql_type(data_type: &DataType) -> &'static str {
    storage_type(data_type)
        .expect("the bound Schema has a Doris storage mapping")
        .0
}

pub(super) fn catalog_type(data_type: &DataType) -> &'static str {
    storage_type(data_type)
        .expect("the bound Schema has a Doris storage mapping")
        .1
}

fn storage_type(data_type: &DataType) -> Option<(&'static str, &'static str)> {
    Some(match data_type {
        DataType::Boolean => ("BOOLEAN", "tinyint(1)"),
        DataType::Int8 => ("TINYINT", "tinyint(4)"),
        DataType::Int16 | DataType::UInt8 => ("SMALLINT", "smallint(6)"),
        DataType::Int32 | DataType::UInt16 | DataType::Date32 => ("INT", "int(11)"),
        DataType::Int64 | DataType::UInt32 | DataType::Timestamp(_, _) => ("BIGINT", "bigint(20)"),
        DataType::Null
        | DataType::Utf8
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => ("STRING", "string"),
        _ => return None,
    })
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
