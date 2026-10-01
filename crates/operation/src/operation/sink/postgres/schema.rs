use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};

use super::error::PostgresSinkSchemaError;

pub(super) const TECHNICAL_HASH: &str = "$dogpaddle.hash";
pub(super) const TECHNICAL_ID: &str = "$dogpaddle.id";
pub(super) const MAX_LOGICAL_COLUMNS: usize = 1_598;
const SYSTEM_COLUMNS: &[&str] = &["tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"];

/// Checks all identifiers before checking the backend's supported type mapping.
pub(super) fn validate(schema: &SchemaRef) -> Result<(), PostgresSinkSchemaError> {
    validate_identifiers(schema)?;
    for field in schema.fields() {
        if storage_type(field.data_type()).is_none() {
            return Err(PostgresSinkSchemaError::UnsupportedType {
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
    storage_type(data_type).expect("the bound Schema has a PostgreSQL storage mapping")
}

fn storage_type(data_type: &DataType) -> Option<&'static str> {
    Some(match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "smallint",
        DataType::Int32 | DataType::Date32 | DataType::UInt16 => "integer",
        DataType::Int64 | DataType::Timestamp(_, _) | DataType::UInt32 => "bigint",
        DataType::Null
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Utf8
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => "bytea",
        _ => return None,
    })
}

pub(super) fn column_check(data_type: &DataType, name: &str) -> Option<String> {
    let check = match data_type {
        DataType::Null => "IS NULL",
        DataType::Int8 => "BETWEEN -128 AND 127",
        DataType::UInt8 => "BETWEEN 0 AND 255",
        DataType::UInt16 => "BETWEEN 0 AND 65535",
        DataType::UInt32 => "BETWEEN 0 AND 4294967295",
        DataType::UInt64 | DataType::Float64 => return Some(format!("octet_length({name}) = 8")),
        DataType::Float32 => return Some(format!("octet_length({name}) = 4")),
        DataType::Decimal128(_, _) => return Some(format!("octet_length({name}) = 16")),
        _ => return None,
    };
    Some(format!("{name} {check}"))
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
