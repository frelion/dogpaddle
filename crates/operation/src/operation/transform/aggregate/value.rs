use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use super::AggregateError;

pub(super) fn null(data_type: &DataType) -> Result<ScalarValue, AggregateError> {
    ScalarValue::try_from(data_type).map_err(AggregateError::DataFusion)
}

pub(super) fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::List(child) => contains_float(child.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        _ => false,
    }
}
