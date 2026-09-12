//! Exact row identity shared by stateful relational operations.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array, new_empty_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion_common::ScalarValue;
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
    #[error("canonical row is truncated")]
    Truncated,
    #[error("canonical row contains an invalid null marker")]
    InvalidNullMarker,
    #[error("canonical row contains an invalid boolean value")]
    InvalidBoolean,
    #[error("canonical row contains invalid UTF-8")]
    InvalidUtf8,
    #[error("canonical row contains trailing bytes")]
    TrailingBytes,
    #[error("canonical row could not be reconstructed as Arrow values")]
    InvalidValue,
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

pub(crate) fn decode_canonical_row(
    schema: &Schema,
    encoded: &[u8],
) -> Result<Vec<ScalarValue>, RowError> {
    let mut cursor = RowCursor::new(encoded);
    let values = schema
        .fields()
        .iter()
        .map(|field| decode_canonical(field, &mut cursor))
        .collect::<Result<Vec<_>, _>>()?;
    cursor.finish()?;
    Ok(values)
}

fn decode_canonical(field: &Field, cursor: &mut RowCursor<'_>) -> Result<ScalarValue, RowError> {
    match cursor.u8()? {
        0 => {
            if !field.is_nullable() && !matches!(field.data_type(), DataType::Null) {
                return Err(RowError::UnexpectedNull {
                    field: field.name().to_owned(),
                });
            }
            ScalarValue::try_from(field.data_type()).map_err(|_| RowError::InvalidValue)
        }
        1 => decode_present(field, cursor),
        _ => Err(RowError::InvalidNullMarker),
    }
}

fn decode_present(field: &Field, cursor: &mut RowCursor<'_>) -> Result<ScalarValue, RowError> {
    macro_rules! fixed {
        ($variant:ident, $type:ty) => {
            ScalarValue::$variant(Some(<$type>::from_be_bytes(cursor.take()?)))
        };
    }
    let value = match field.data_type() {
        DataType::Null => return Err(RowError::InvalidNullMarker),
        DataType::Boolean => match cursor.u8()? {
            0 => ScalarValue::Boolean(Some(false)),
            1 => ScalarValue::Boolean(Some(true)),
            _ => return Err(RowError::InvalidBoolean),
        },
        DataType::Int8 => fixed!(Int8, i8),
        DataType::Int16 => fixed!(Int16, i16),
        DataType::Int32 => fixed!(Int32, i32),
        DataType::Int64 => fixed!(Int64, i64),
        DataType::UInt8 => fixed!(UInt8, u8),
        DataType::UInt16 => fixed!(UInt16, u16),
        DataType::UInt32 => fixed!(UInt32, u32),
        DataType::UInt64 => fixed!(UInt64, u64),
        DataType::Float32 => {
            ScalarValue::Float32(Some(f32::from_bits(u32::from_be_bytes(cursor.take()?))))
        }
        DataType::Float64 => {
            ScalarValue::Float64(Some(f64::from_bits(u64::from_be_bytes(cursor.take()?))))
        }
        DataType::Date32 => fixed!(Date32, i32),
        DataType::Timestamp(unit, timezone) => {
            let value = Some(i64::from_be_bytes(cursor.take()?));
            let timezone = timezone.as_ref().map(Arc::clone);
            match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(value, timezone),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, timezone),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, timezone),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, timezone),
            }
        }
        DataType::Decimal128(precision, scale) => ScalarValue::Decimal128(
            Some(i128::from_be_bytes(cursor.take()?)),
            *precision,
            *scale,
        ),
        DataType::Utf8 => ScalarValue::Utf8(Some(
            std::str::from_utf8(cursor.bytes()?)
                .map_err(|_| RowError::InvalidUtf8)?
                .to_owned(),
        )),
        DataType::Binary => ScalarValue::Binary(Some(cursor.bytes()?.to_vec())),
        DataType::List(child) => decode_list(child, cursor)?,
        DataType::Struct(fields) => {
            let values = fields
                .iter()
                .map(|child| decode_canonical(child, cursor))
                .collect::<Result<Vec<_>, _>>()?;
            let structure = if fields.is_empty() {
                StructArray::new_empty_fields(1, None)
            } else {
                let arrays = values
                    .iter()
                    .map(|value| value.to_array_of_size(1))
                    .collect::<Result<Vec<ArrayRef>, _>>()
                    .map_err(|_| RowError::InvalidValue)?;
                StructArray::new(fields.clone(), arrays, None)
            };
            ScalarValue::Struct(Arc::new(structure))
        }
        _ => return Err(RowError::InvalidValue),
    };
    Ok(value)
}

fn decode_list(field: &Arc<Field>, cursor: &mut RowCursor<'_>) -> Result<ScalarValue, RowError> {
    let length = cursor.length()?;
    let end = i32::try_from(length).map_err(|_| RowError::LengthOverflow)?;
    if length > cursor.remaining_len() {
        return Err(RowError::Truncated);
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        values.push(decode_canonical(field, cursor)?);
    }
    let values = if values.is_empty() {
        new_empty_array(field.data_type())
    } else {
        ScalarValue::iter_to_array(values).map_err(|_| RowError::InvalidValue)?
    };
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, end]));
    Ok(ScalarValue::List(Arc::new(ListArray::new(
        Arc::clone(field),
        offsets,
        values,
        None,
    ))))
}

struct RowCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> RowCursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn u8(&mut self) -> Result<u8, RowError> {
        Ok(self.take::<1>()?[0])
    }

    fn length(&mut self) -> Result<usize, RowError> {
        usize::try_from(u64::from_be_bytes(self.take()?)).map_err(|_| RowError::LengthOverflow)
    }

    fn bytes(&mut self) -> Result<&'a [u8], RowError> {
        let length = self.length()?;
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(RowError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], RowError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(RowError::Truncated)?;
        self.remaining = remaining;
        Ok(*value)
    }

    const fn finish(self) -> Result<(), RowError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(RowError::TrailingBytes)
        }
    }
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

#[cfg(test)]
mod tests {
    use arrow_array::{Array, ListArray, RecordBatch, StringArray, StructArray, types::UInt64Type};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::*;

    #[test]
    fn canonical_decoder_round_trips_empty_lists_and_nullable_nested_values() {
        let lists = ListArray::from_iter_primitive::<UInt64Type, _, _>([
            Some(Vec::<Option<u64>>::new()),
            Some(vec![Some(7), None]),
            None,
        ]);
        let struct_fields = Fields::from(vec![Field::new("text", DataType::Utf8, true)]);
        let structures = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(StringArray::from(vec![
                None,
                Some("value"),
                Some("hidden"),
            ]))],
            Some(NullBuffer::from(vec![true, true, false])),
        );
        let empty_structures =
            StructArray::new_empty_fields(3, Some(NullBuffer::from(vec![true, true, false])));
        let schema = Arc::new(Schema::new(vec![
            Field::new("items", lists.data_type().clone(), true),
            Field::new("structure", DataType::Struct(struct_fields), true),
            Field::new(
                "empty_structure",
                empty_structures.data_type().clone(),
                true,
            ),
        ]));
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(lists),
                Arc::new(structures),
                Arc::new(empty_structures),
            ],
        )
        .unwrap();

        for row in 0..records.num_rows() {
            let encoded = canonical_row(&records, row).unwrap();
            let decoded = decode_canonical_row(&schema, &encoded).unwrap();
            let columns = decoded
                .iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect();
            let rebuilt = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
            assert_eq!(canonical_row(&rebuilt, 0).unwrap(), encoded);
        }
    }

    #[test]
    fn canonical_list_decoder_rejects_impossible_lengths_before_allocation() {
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        )]);
        let mut too_wide = vec![1];
        too_wide.extend_from_slice(&(u64::from(i32::MAX.cast_unsigned()) + 1).to_be_bytes());
        assert_eq!(
            decode_canonical_row(&schema, &too_wide),
            Err(RowError::LengthOverflow)
        );

        let mut beyond_remaining = vec![1];
        beyond_remaining.extend_from_slice(&2_u64.to_be_bytes());
        beyond_remaining.push(0);
        assert_eq!(
            decode_canonical_row(&schema, &beyond_remaining),
            Err(RowError::Truncated)
        );
    }
}
