use arrow_schema::{DataType, Fields, SchemaRef, TimeUnit};
use serde::Deserializer;

use super::PostgresCdcScanError;
use crate::operation::scan::cdc_convert::{deserialize_flat_fields, source_schema, valid_decimal};

pub(super) fn compile(columns: &Fields) -> Result<SchemaRef, PostgresCdcScanError> {
    // Reject recursive or unsupported Arrow types before definition serialization.
    source_schema(columns, |data_type| match data_type {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Boolean
        | DataType::Float32
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, None) => true,
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => zone.as_ref() == "UTC",
        DataType::Decimal128(precision, scale) => valid_decimal(*precision, *scale),
        _ => false,
    })
    .map_err(PostgresCdcScanError::InvalidDefinition)
}

// Restore the supported source-field domain before a raw plan can be encoded.
pub(super) fn deserialize_fields<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Fields, D::Error> {
    let fields = deserialize_flat_fields(decoder)?;
    compile(&fields).map_err(serde::de::Error::custom)?;
    Ok(fields)
}
