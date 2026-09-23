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
    #[error("canonical row exceeds its {max_bytes}-byte limit")]
    SizeLimit { max_bytes: usize },
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
    canonical_row_with_limit(batch, index, None)
}

pub(crate) fn canonical_row_bounded(
    batch: &RecordBatch,
    index: usize,
    max_bytes: usize,
) -> Result<Vec<u8>, OperationError> {
    canonical_row_with_limit(batch, index, Some(max_bytes))
}

pub(crate) fn canonical_row_size_bounded(
    batch: &RecordBatch,
    index: usize,
    max_bytes: usize,
) -> Result<usize, OperationError> {
    if index >= batch.num_rows() {
        return Err(Box::new(RowError::RowOutOfBounds {
            row_index: index,
            rows: batch.num_rows(),
        }));
    }
    let mut size = 0;
    let mut path = Vec::new();
    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        path.push(field.name().as_str());
        let result = canonical_size_inner(
            field,
            array.as_ref(),
            index,
            &mut path,
            &mut size,
            max_bytes,
        );
        path.pop();
        result?;
    }
    Ok(size)
}

fn canonical_row_with_limit(
    batch: &RecordBatch,
    index: usize,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>, OperationError> {
    let mut bytes = Vec::new();
    if index >= batch.num_rows() {
        return Err(Box::new(RowError::RowOutOfBounds {
            row_index: index,
            rows: batch.num_rows(),
        }));
    }
    let mut path = Vec::new();
    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        path.push(field.name().as_str());
        let result = encode_canonical_inner(
            field,
            array.as_ref(),
            index,
            &mut path,
            &mut bytes,
            max_bytes,
        );
        path.pop();
        result?;
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn canonical_size_inner<'a>(
    field: &'a Field,
    array: &dyn Array,
    index: usize,
    path: &mut Vec<&'a str>,
    size: &mut usize,
    max_bytes: usize,
) -> Result<(), RowError> {
    if array.data_type() != field.data_type() {
        return Err(RowError::ArrayTypeMismatch {
            field: path.join("."),
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
                field: path.join("."),
            });
        }
        add_size(size, 1, max_bytes)?;
        return Ok(());
    }

    add_size(size, 1, max_bytes)?;
    if let Some(width) = fixed_canonical_width(field, array, path)? {
        return add_size(size, width, max_bytes);
    }
    match field.data_type() {
        DataType::Utf8 => {
            let bytes = downcast::<StringArray>(array, field, path)?.value(index);
            add_size(size, 8, max_bytes)?;
            add_size(size, bytes.len(), max_bytes)
        }
        DataType::Binary => {
            let bytes = downcast::<BinaryArray>(array, field, path)?.value(index);
            add_size(size, 8, max_bytes)?;
            add_size(size, bytes.len(), max_bytes)
        }
        DataType::List(child) => {
            let values = downcast::<ListArray>(array, field, path)?.value(index);
            add_size(size, 8, max_bytes)?;
            if values.len() > max_bytes - *size {
                return Err(RowError::SizeLimit { max_bytes });
            }
            if let Some(child_size) = constant_canonical_size(child, values.as_ref()) {
                let children = values
                    .len()
                    .checked_mul(child_size)
                    .ok_or(RowError::LengthOverflow)?;
                return add_size(size, children, max_bytes);
            }
            path.push(child.name());
            for index in 0..values.len() {
                canonical_size_inner(child, values.as_ref(), index, path, size, max_bytes)?;
            }
            path.pop();
            Ok(())
        }
        DataType::Struct(fields) => {
            let structure = downcast::<StructArray>(array, field, path)?;
            for (child, values) in fields.iter().zip(structure.columns()) {
                path.push(child.name());
                let result =
                    canonical_size_inner(child, values.as_ref(), index, path, size, max_bytes);
                path.pop();
                result?;
            }
            Ok(())
        }
        DataType::Null => unreachable!("null values are handled above"),
        other => Err(RowError::ArrayTypeMismatch {
            field: path.join("."),
            expected: other.clone(),
            actual: array.data_type().clone(),
        }),
    }
}

fn fixed_canonical_width(
    field: &Field,
    array: &dyn Array,
    path: &[&str],
) -> Result<Option<usize>, RowError> {
    macro_rules! width {
        ($type:ty, $size:expr) => {{
            downcast::<$type>(array, field, path)?;
            Some($size)
        }};
    }
    Ok(match field.data_type() {
        DataType::Boolean => width!(BooleanArray, 1),
        DataType::Int8 => width!(Int8Array, 1),
        DataType::Int16 => width!(Int16Array, 2),
        DataType::Int32 => width!(Int32Array, 4),
        DataType::Int64 => width!(Int64Array, 8),
        DataType::UInt8 => width!(UInt8Array, 1),
        DataType::UInt16 => width!(UInt16Array, 2),
        DataType::UInt32 => width!(UInt32Array, 4),
        DataType::UInt64 => width!(UInt64Array, 8),
        DataType::Float32 => width!(Float32Array, 4),
        DataType::Float64 => width!(Float64Array, 8),
        DataType::Date32 => width!(Date32Array, 4),
        DataType::Timestamp(TimeUnit::Second, _) => width!(TimestampSecondArray, 8),
        DataType::Timestamp(TimeUnit::Millisecond, _) => width!(TimestampMillisecondArray, 8),
        DataType::Timestamp(TimeUnit::Microsecond, _) => width!(TimestampMicrosecondArray, 8),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => width!(TimestampNanosecondArray, 8),
        DataType::Decimal128(_, _) => width!(Decimal128Array, 16),
        _ => None,
    })
}

fn constant_canonical_size(field: &Field, array: &dyn Array) -> Option<usize> {
    if array.data_type() != field.data_type() {
        return None;
    }
    if matches!(field.data_type(), DataType::Null) {
        return Some(1);
    }
    let value_size = if let Some(width) = fixed_canonical_width(field, array, &[]).ok()? {
        width + 1
    } else if let DataType::Struct(fields) = field.data_type() {
        let structure = array.as_any().downcast_ref::<StructArray>()?;
        fields
            .iter()
            .zip(structure.columns())
            .try_fold(1_usize, |size, (child, values)| {
                size.checked_add(constant_canonical_size(child, values.as_ref())?)
            })?
    } else {
        return None;
    };
    if array.null_count() == 0 || value_size == 1 {
        Some(value_size)
    } else {
        None
    }
}

fn add_size(size: &mut usize, additional: usize, max_bytes: usize) -> Result<(), RowError> {
    let next = size
        .checked_add(additional)
        .ok_or(RowError::LengthOverflow)?;
    if next > max_bytes {
        Err(RowError::SizeLimit { max_bytes })
    } else {
        *size = next;
        Ok(())
    }
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
pub(crate) fn encode_canonical<'a>(
    field: &'a Field,
    array: &dyn Array,
    index: usize,
    path: &'a str,
    output: &mut Vec<u8>,
) -> Result<(), RowError> {
    encode_canonical_inner(field, array, index, &mut vec![path], output, None)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn encode_canonical_bounded<'a>(
    field: &'a Field,
    array: &dyn Array,
    index: usize,
    path: &'a str,
    output: &mut Vec<u8>,
    max_bytes: usize,
) -> Result<(), RowError> {
    encode_canonical_inner(
        field,
        array,
        index,
        &mut vec![path],
        output,
        Some(max_bytes),
    )
}

#[allow(clippy::too_many_lines)]
fn encode_canonical_inner<'a>(
    field: &'a Field,
    array: &dyn Array,
    index: usize,
    path: &mut Vec<&'a str>,
    output: &mut Vec<u8>,
    max_bytes: Option<usize>,
) -> Result<(), RowError> {
    if array.data_type() != field.data_type() {
        return Err(RowError::ArrayTypeMismatch {
            field: path.join("."),
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
                field: path.join("."),
            });
        }
        push_byte(output, 0, max_bytes)?;
        return Ok(());
    }
    push_byte(output, 1, max_bytes)?;
    macro_rules! fixed {
        ($array:ty) => {
            append_bytes(
                output,
                &downcast::<$array>(array, field, path)?
                    .value(index)
                    .to_be_bytes(),
                max_bytes,
            )?
        };
    }
    match field.data_type() {
        DataType::Null => unreachable!("null values are handled above"),
        DataType::Boolean => push_byte(
            output,
            u8::from(downcast::<BooleanArray>(array, field, path)?.value(index)),
            max_bytes,
        )?,
        DataType::Int8 => fixed!(Int8Array),
        DataType::Int16 => fixed!(Int16Array),
        DataType::Int32 => fixed!(Int32Array),
        DataType::Int64 => fixed!(Int64Array),
        DataType::UInt8 => fixed!(UInt8Array),
        DataType::UInt16 => fixed!(UInt16Array),
        DataType::UInt32 => fixed!(UInt32Array),
        DataType::UInt64 => fixed!(UInt64Array),
        DataType::Float32 => append_bytes(
            output,
            &downcast::<Float32Array>(array, field, path)?
                .value(index)
                .to_bits()
                .to_be_bytes(),
            max_bytes,
        )?,
        DataType::Float64 => append_bytes(
            output,
            &downcast::<Float64Array>(array, field, path)?
                .value(index)
                .to_bits()
                .to_be_bytes(),
            max_bytes,
        )?,
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
            max_bytes,
        )?,
        DataType::Binary => encode_bytes(
            downcast::<BinaryArray>(array, field, path)?.value(index),
            output,
            max_bytes,
        )?,
        DataType::List(child) => {
            let values = downcast::<ListArray>(array, field, path)?.value(index);
            encode_length(values.len(), output, max_bytes)?;
            require_capacity(output, values.len(), max_bytes)?;
            path.push(child.name());
            for index in 0..values.len() {
                encode_canonical_inner(child, values.as_ref(), index, path, output, max_bytes)?;
            }
            path.pop();
        }
        DataType::Struct(fields) => {
            let structure = downcast::<StructArray>(array, field, path)?;
            for (child, values) in fields.iter().zip(structure.columns()) {
                path.push(child.name());
                let result =
                    encode_canonical_inner(child, values.as_ref(), index, path, output, max_bytes);
                path.pop();
                result?;
            }
        }
        other => {
            return Err(RowError::ArrayTypeMismatch {
                field: path.join("."),
                expected: other.clone(),
                actual: array.data_type().clone(),
            });
        }
    }
    Ok(())
}

fn downcast<'array, T: Array + 'static>(
    array: &'array dyn Array,
    field: &Field,
    path: &[&str],
) -> Result<&'array T, RowError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| RowError::ArrayTypeMismatch {
            field: path.join("."),
            expected: field.data_type().clone(),
            actual: array.data_type().clone(),
        })
}

fn encode_bytes(
    bytes: &[u8],
    output: &mut Vec<u8>,
    max_bytes: Option<usize>,
) -> Result<(), RowError> {
    encode_length(bytes.len(), output, max_bytes)?;
    append_bytes(output, bytes, max_bytes)
}

fn encode_length(
    length: usize,
    output: &mut Vec<u8>,
    max_bytes: Option<usize>,
) -> Result<(), RowError> {
    append_bytes(
        output,
        &u64::try_from(length)
            .map_err(|_| RowError::LengthOverflow)?
            .to_be_bytes(),
        max_bytes,
    )
}

fn push_byte(output: &mut Vec<u8>, byte: u8, max_bytes: Option<usize>) -> Result<(), RowError> {
    require_capacity(output, 1, max_bytes)?;
    output.push(byte);
    Ok(())
}

fn append_bytes(
    output: &mut Vec<u8>,
    bytes: &[u8],
    max_bytes: Option<usize>,
) -> Result<(), RowError> {
    require_capacity(output, bytes.len(), max_bytes)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn require_capacity(
    output: &[u8],
    additional: usize,
    max_bytes: Option<usize>,
) -> Result<(), RowError> {
    let next = output
        .len()
        .checked_add(additional)
        .ok_or(RowError::LengthOverflow)?;
    if let Some(max_bytes) = max_bytes
        && next > max_bytes
    {
        return Err(RowError::SizeLimit { max_bytes });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BooleanArray, Decimal128Array, ListArray, RecordBatch, StringArray, StructArray,
        TimestampMicrosecondArray, types::UInt64Type,
    };
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
            assert_eq!(
                canonical_row_size_bounded(&records, row, encoded.len()).unwrap(),
                encoded.len()
            );
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
    fn canonical_sizes_match_encoded_fixed_nullable_decimal_and_timestamp_values() {
        let records = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("flag", DataType::Boolean, true),
                Field::new(
                    "when",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("decimal", DataType::Decimal128(12, 2), true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(-7), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(-123), None])
                        .with_precision_and_scale(12, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        for index in 0..records.num_rows() {
            let encoded = canonical_row(&records, index).unwrap();
            assert_eq!(
                canonical_row_size_bounded(&records, index, encoded.len()).unwrap(),
                encoded.len()
            );
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
