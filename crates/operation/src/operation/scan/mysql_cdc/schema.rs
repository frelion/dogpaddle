use arrow_schema::{DataType, Fields, SchemaRef};
use serde::Deserializer;

use super::MySqlCdcScanError;
use crate::operation::scan::cdc_convert::{deserialize_flat_fields, source_schema, valid_decimal};

pub(super) fn compile(columns: &Fields) -> Result<SchemaRef, MySqlCdcScanError> {
    // Reject recursive or unsupported Arrow types before definition serialization.
    source_schema(columns, |data_type| match data_type {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary => true,
        DataType::Decimal128(precision, scale) => valid_decimal(*precision, *scale),
        _ => false,
    })
    .map_err(MySqlCdcScanError::InvalidDefinition)
}

// Restore the supported source-field domain before a raw plan can be encoded.
pub(super) fn deserialize_fields<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Fields, D::Error> {
    let fields = deserialize_flat_fields(decoder)?;
    compile(&fields).map_err(serde::de::Error::custom)?;
    Ok(fields)
}
