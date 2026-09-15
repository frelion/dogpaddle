use arrow_array::{
    Array, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use mysql::Value;

use super::{
    error::DorisSinkError,
    schema::{DorisLayout, StorageType},
};
use crate::operation::sink::relation::{RowError, encode_canonical, row_hash};

#[derive(Debug)]
pub(super) struct DorisRowCodec {
    layout: DorisLayout,
}

impl DorisRowCodec {
    pub(super) const fn new(layout: DorisLayout) -> Self {
        Self { layout }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.layout.schema()
    }

    pub(super) const fn layout(&self) -> &DorisLayout {
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
            values.push(doris_value(
                field,
                array.as_ref(),
                row_index,
                column.storage(),
                &canonical[start..],
            )?);
        }
        Ok(EncodedRow {
            hash: hex(&row_hash(&canonical)),
            values,
        })
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct EncodedRow {
    pub(super) hash: Vec<u8>,
    pub(super) values: Vec<Value>,
}

fn doris_value(
    field: &Field,
    array: &dyn Array,
    index: usize,
    storage: StorageType,
    canonical: &[u8],
) -> Result<Value, RowError> {
    if matches!(storage, StorageType::Null) || array.is_null(index) {
        return Ok(Value::NULL);
    }
    Ok(match field.data_type() {
        DataType::Null => unreachable!("handled by null branch"),
        DataType::Boolean => Value::Int(i64::from(
            downcast::<BooleanArray>(array, field)?.value(index),
        )),
        DataType::Int8 => Value::Int(i64::from(downcast::<Int8Array>(array, field)?.value(index))),
        DataType::Int16 => Value::Int(i64::from(
            downcast::<Int16Array>(array, field)?.value(index),
        )),
        DataType::Int32 => Value::Int(i64::from(
            downcast::<Int32Array>(array, field)?.value(index),
        )),
        DataType::Int64 => Value::Int(downcast::<Int64Array>(array, field)?.value(index)),
        DataType::UInt8 => Value::UInt(u64::from(
            downcast::<UInt8Array>(array, field)?.value(index),
        )),
        DataType::UInt16 => Value::UInt(u64::from(
            downcast::<UInt16Array>(array, field)?.value(index),
        )),
        DataType::UInt32 => Value::UInt(u64::from(
            downcast::<UInt32Array>(array, field)?.value(index),
        )),
        DataType::Date32 => Value::Int(i64::from(
            downcast::<Date32Array>(array, field)?.value(index),
        )),
        DataType::Timestamp(unit, _) => Value::Int(match unit {
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
        }),
        DataType::Utf8 => Value::Bytes(
            downcast::<StringArray>(array, field)?
                .value(index)
                .as_bytes()
                .to_vec(),
        ),
        DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => Value::Bytes(STANDARD.encode(canonical).into_bytes()),
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

fn hex(bytes: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)]);
        output.push(DIGITS[usize::from(byte & 0x0f)]);
    }
    output
}

impl From<RowError> for DorisSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
