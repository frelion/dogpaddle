//! Exact row identity is independent of the target SQL representation.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use thiserror::Error;

use crate::operation::OperationError;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RowError {
    #[error("record batch Schema differs from the bound Schema")]
    SchemaMismatch,
    #[error("row index {row_index} is outside a record batch with {rows} rows")]
    RowOutOfBounds { row_index: usize, rows: usize },
    #[error("array for field {field:?} has type {actual}, expected {expected}")]
    ArrayTypeMismatch {
        field: String,
        expected: DataType,
        actual: DataType,
    },
    #[error("non-nullable field {field:?} contains a null value")]
    UnexpectedNull { field: String },
    #[error("nested value length cannot be represented by the canonical row format")]
    LengthOverflow,
}

pub(crate) fn canonical_row(batch: &RecordBatch, index: usize) -> Result<Vec<u8>, OperationError> {
    if index >= batch.num_rows() {
        return Err(Box::new(RowError::RowOutOfBounds {
            row_index: index,
            rows: batch.num_rows(),
        }));
    }
    let mut bytes = Vec::new();
    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        encode_canonical(field, array.as_ref(), index, field.name(), &mut bytes)?;
    }
    Ok(bytes)
}

pub(crate) fn row_hash(bytes: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dogpaddle.relation-row.v1\0");
    hasher.update(bytes);
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("the truncated hash is exactly 16 bytes")
}

#[allow(clippy::too_many_lines)]
pub(crate) fn encode_canonical(
    field: &Field,
    array: &dyn Array,
    index: usize,
    path: &str,
    output: &mut Vec<u8>,
) -> Result<(), RowError> {
    if array.data_type() != field.data_type() {
        return Err(RowError::ArrayTypeMismatch {
            field: path.to_owned(),
            expected: field.data_type().clone(),
            actual: array.data_type().clone(),
        });
    }
    if index >= array.len() {
        return Err(RowError::RowOutOfBounds {
            row_index: index,
            rows: array.len(),
        });
    }
    let null_type = matches!(field.data_type(), DataType::Null);
    if null_type || array.is_null(index) {
        if !field.is_nullable() && !null_type {
            return Err(RowError::UnexpectedNull {
                field: path.to_owned(),
            });
        }
        output.push(0);
        return Ok(());
    }
    output.push(1);
    macro_rules! fixed {
        ($array:ty) => {
            output.extend_from_slice(
                &downcast::<$array>(array, field, path)?
                    .value(index)
                    .to_be_bytes(),
            )
        };
    }
    match field.data_type() {
        DataType::Null => unreachable!("null values are handled above"),
        DataType::Boolean => output.push(u8::from(
            downcast::<BooleanArray>(array, field, path)?.value(index),
        )),
        DataType::Int8 => fixed!(Int8Array),
        DataType::Int16 => fixed!(Int16Array),
        DataType::Int32 => fixed!(Int32Array),
        DataType::Int64 => fixed!(Int64Array),
        DataType::UInt8 => fixed!(UInt8Array),
        DataType::UInt16 => fixed!(UInt16Array),
        DataType::UInt32 => fixed!(UInt32Array),
        DataType::UInt64 => fixed!(UInt64Array),
        DataType::Float32 => output.extend_from_slice(
            &downcast::<Float32Array>(array, field, path)?
                .value(index)
                .to_bits()
                .to_be_bytes(),
        ),
        DataType::Float64 => output.extend_from_slice(
            &downcast::<Float64Array>(array, field, path)?
                .value(index)
                .to_bits()
                .to_be_bytes(),
        ),
        DataType::Date32 => fixed!(Date32Array),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => fixed!(TimestampSecondArray),
            TimeUnit::Millisecond => fixed!(TimestampMillisecondArray),
            TimeUnit::Microsecond => fixed!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => fixed!(TimestampNanosecondArray),
        },
        DataType::Decimal128(_, _) => fixed!(Decimal128Array),
        DataType::Utf8 => encode_bytes(
            downcast::<StringArray>(array, field, path)?
                .value(index)
                .as_bytes(),
            output,
        )?,
        DataType::Binary => encode_bytes(
            downcast::<BinaryArray>(array, field, path)?.value(index),
            output,
        )?,
        DataType::List(child) => {
            let values = downcast::<ListArray>(array, field, path)?.value(index);
            encode_length(values.len(), output)?;
            let path = format!("{path}.{}", child.name());
            for index in 0..values.len() {
                encode_canonical(child, values.as_ref(), index, &path, output)?;
            }
        }
        DataType::Struct(fields) => {
            let structure = downcast::<StructArray>(array, field, path)?;
            for (child, values) in fields.iter().zip(structure.columns()) {
                encode_canonical(
                    child,
                    values.as_ref(),
                    index,
                    &format!("{path}.{}", child.name()),
                    output,
                )?;
            }
        }
        other => {
            return Err(RowError::ArrayTypeMismatch {
                field: path.to_owned(),
                expected: other.clone(),
                actual: array.data_type().clone(),
            });
        }
    }
    Ok(())
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    field: &Field,
    path: &str,
) -> Result<&'a T, RowError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| RowError::ArrayTypeMismatch {
            field: path.to_owned(),
            expected: field.data_type().clone(),
            actual: array.data_type().clone(),
        })
}

fn encode_bytes(bytes: &[u8], output: &mut Vec<u8>) -> Result<(), RowError> {
    encode_length(bytes.len(), output)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_length(length: usize, output: &mut Vec<u8>) -> Result<(), RowError> {
    output.extend_from_slice(
        &u64::try_from(length)
            .map_err(|_| RowError::LengthOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}
