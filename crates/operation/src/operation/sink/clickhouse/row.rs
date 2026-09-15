use arrow_array::{
    Array, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Number, Value};

use super::{
    error::ClickHouseSinkError,
    schema::{ClickHouseLayout, ColumnLayout},
};
use crate::operation::sink::relation::{RowError, encode_canonical, row_hash};

#[derive(Debug)]
pub(super) struct ClickHouseRowCodec {
    layout: ClickHouseLayout,
}

impl ClickHouseRowCodec {
    pub(super) const fn new(layout: ClickHouseLayout) -> Self {
        Self { layout }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.layout.schema()
    }

    pub(super) const fn layout(&self) -> &ClickHouseLayout {
        &self.layout
    }

    pub(super) fn encode_row(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<EncodedRow, RowError> {
        if batch.schema_ref().as_ref() != self.schema().as_ref() {
            return Err(RowError::SchemaMismatch);
        }
        if row_index >= batch.num_rows() {
            return Err(RowError::RowOutOfBounds {
                row_index,
                rows: batch.num_rows(),
            });
        }
        let mut canonical = Vec::new();
        let mut values = Vec::with_capacity(self.schema().fields().len());
        for ((field, array), column) in self
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(self.layout.columns())
        {
            let start = canonical.len();
            encode_canonical(
                field,
                array.as_ref(),
                row_index,
                field.name(),
                &mut canonical,
            )?;
            values.push(clickhouse_value(
                field,
                array.as_ref(),
                row_index,
                column,
                &canonical[start..],
            )?);
        }
        Ok(EncodedRow {
            hash: hex(&row_hash(&canonical)),
            values,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct EncodedRow {
    pub(super) hash: String,
    pub(super) values: Vec<Value>,
}

fn clickhouse_value(
    field: &Field,
    array: &dyn Array,
    index: usize,
    column: &ColumnLayout,
    canonical: &[u8],
) -> Result<Value, RowError> {
    if column.always_null() || array.is_null(index) {
        return Ok(Value::Null);
    }
    if column.encoded() {
        return Ok(Value::String(STANDARD.encode(canonical)));
    }
    Ok(match field.data_type() {
        DataType::Null => unreachable!("handled by null branch"),
        DataType::Boolean => Value::Number(Number::from(u8::from(
            downcast::<BooleanArray>(array, field)?.value(index),
        ))),
        DataType::Int8 => Value::Number(Number::from(
            downcast::<Int8Array>(array, field)?.value(index),
        )),
        DataType::Int16 => Value::Number(Number::from(
            downcast::<Int16Array>(array, field)?.value(index),
        )),
        DataType::Int32 => Value::Number(Number::from(
            downcast::<Int32Array>(array, field)?.value(index),
        )),
        DataType::Int64 => Value::Number(Number::from(
            downcast::<Int64Array>(array, field)?.value(index),
        )),
        DataType::UInt8 => Value::Number(Number::from(
            downcast::<UInt8Array>(array, field)?.value(index),
        )),
        DataType::UInt16 => Value::Number(Number::from(
            downcast::<UInt16Array>(array, field)?.value(index),
        )),
        DataType::UInt32 => Value::Number(Number::from(
            downcast::<UInt32Array>(array, field)?.value(index),
        )),
        DataType::UInt64 => Value::Number(Number::from(
            downcast::<UInt64Array>(array, field)?.value(index),
        )),
        DataType::Date32 => Value::Number(Number::from(
            downcast::<Date32Array>(array, field)?.value(index),
        )),
        DataType::Timestamp(unit, _) => Value::Number(Number::from(match unit {
            TimeUnit::Second => downcast::<TimestampSecondArray>(array, field)?.value(index),
            TimeUnit::Millisecond => {
                downcast::<TimestampMillisecondArray>(array, field)?.value(index)
            }
            TimeUnit::Microsecond => {
                downcast::<TimestampMicrosecondArray>(array, field)?.value(index)
            }
            TimeUnit::Nanosecond => {
                downcast::<TimestampNanosecondArray>(array, field)?.value(index)
            }
        })),
        DataType::Utf8
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => unreachable!("encoded columns returned above"),
        unsupported => {
            return Err(RowError::ArrayTypeMismatch {
                field: field.name().clone(),
                expected: unsupported.clone(),
                actual: array.data_type().clone(),
            });
        }
    })
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    field: &Field,
) -> Result<&'a T, RowError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| RowError::ArrayTypeMismatch {
            field: field.name().clone(),
            expected: field.data_type().clone(),
            actual: array.data_type().clone(),
        })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

impl From<RowError> for ClickHouseSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
