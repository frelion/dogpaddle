//! Exact row identity shared by stateful relational operations.

use std::{mem::size_of, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
        ListBuilder, NullBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use dogpaddle_change::Change;
use thiserror::Error;

use crate::operation::{OperationError, StepBudget};

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

pub(crate) fn canonical_row_bounded(
    batch: &RecordBatch,
    index: usize,
    max_bytes: usize,
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
            Some(max_bytes),
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

/// Step-local Arrow output; fragments retain their source Schema validation.
#[derive(Default)]
pub(crate) struct ArrowOutput {
    builders: Vec<Box<dyn ArrayBuilder>>,
    differences: Vec<i64>,
}

impl ArrowOutput {
    pub(crate) fn push(
        &mut self,
        schemas: &[SchemaRef],
        fragments: [Option<&[u8]>; 2],
        difference: i64,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        self.extend(
            schemas,
            std::iter::once((fragments, difference)).map(Ok),
            budget,
        )
    }

    // This replayable iterator only reads borrowed fragments and checks diffs.
    // Admit its complete page before appending any of that page's values.
    pub(crate) fn extend<'row>(
        &mut self,
        schemas: &[SchemaRef],
        rows: impl Iterator<Item = Result<([Option<&'row [u8]>; 2], i64), OperationError>> + Clone,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        let first = self.differences.is_empty();
        let mut count = 0;
        for row in rows.clone() {
            let (fragments, _) = row?;
            if first && count == 0 {
                for schema in schemas {
                    admit_batch_shape(schema, budget)?;
                }
            }
            for (schema, fragment) in schemas.iter().zip(fragments) {
                if let Some(encoded) = fragment {
                    admit_row(schema, encoded, budget)?;
                } else {
                    for field in schema.fields() {
                        let mut size = 0;
                        decoded_array_shape(
                            field.data_type(),
                            1,
                            &mut size,
                            budget.remaining_bytes(),
                        )
                        .map_err(decode_error)?;
                        budget.charge(size)?;
                    }
                }
            }
            budget.charge(size_of::<i64>())?;
            count += 1;
        }
        if first && count != 0 {
            self.builders = schemas
                .iter()
                .flat_map(|schema| schema.fields())
                .map(|field| row_builder(field.data_type(), count))
                .collect::<Result<_, _>>()?;
        }
        for row in rows {
            let (fragments, difference) = row?;
            append_row(schemas, fragments, &mut self.builders)?;
            self.differences.push(difference);
        }
        Ok(())
    }

    pub(crate) fn records(self, schema: &SchemaRef) -> Result<RecordBatch, OperationError> {
        finish_builders(schema, self.builders, self.differences.len())
    }

    pub(crate) fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, OperationError> {
        if self.differences.is_empty() {
            return Ok(None);
        }
        let records = finish_builders(schema, self.builders, self.differences.len())?;
        Ok(Some(Change::try_new(
            records,
            Int64Array::from(self.differences),
        )?))
    }
}

/// Checks all framing, values and Arrow buffer costs before creating builders.
pub(crate) fn decode_canonical_rows_bounded<T: AsRef<[u8]>>(
    schema: &SchemaRef,
    rows: &[T],
    budget: &mut StepBudget,
) -> Result<RecordBatch, OperationError> {
    admit_batch_shape(schema, budget)?;
    for row in rows {
        admit_row(schema, row.as_ref(), budget)?;
    }
    decode_rows(schema, rows.iter())
}

fn admit_batch_shape(schema: &SchemaRef, budget: &mut StepBudget) -> Result<(), OperationError> {
    budget.charge(
        schema
            .fields()
            .len()
            .saturating_mul(2 * size_of::<ArrayRef>()),
    )?;
    for field in schema.fields() {
        let mut size = 0;
        decoded_array_shape(field.data_type(), 0, &mut size, budget.remaining_bytes())
            .map_err(decode_error)?;
        budget.charge(size)?;
    }
    Ok(())
}

fn decode_error(error: RowError) -> OperationError {
    if matches!(error, RowError::SizeLimit { .. }) {
        crate::operation::BudgetExceeded.into()
    } else {
        error.into()
    }
}

fn admit_row(
    schema: &SchemaRef,
    encoded: &[u8],
    budget: &mut StepBudget,
) -> Result<(), OperationError> {
    let size = decoded_size(schema, encoded, budget.remaining_bytes()).map_err(decode_error)?;
    budget.charge(size)?;
    Ok(())
}

fn decoded_size(schema: &SchemaRef, encoded: &[u8], max_bytes: usize) -> Result<usize, RowError> {
    let mut cursor = RowCursor::new(encoded);
    let mut size = 0;
    for field in schema.fields() {
        check_value(field, &mut cursor, &mut size, max_bytes)?;
    }
    cursor.finish()?;
    Ok(size)
}

fn check_value(
    field: &Field,
    cursor: &mut RowCursor<'_>,
    size: &mut usize,
    max_bytes: usize,
) -> Result<(), RowError> {
    match cursor.u8()? {
        0 => {
            if !field.is_nullable() && !matches!(field.data_type(), DataType::Null) {
                return Err(RowError::UnexpectedNull {
                    field: field.name().to_owned(),
                });
            }
            decoded_array_shape(field.data_type(), 1, size, max_bytes)?;
        }
        1 => match field.data_type() {
            DataType::Null => return Err(RowError::InvalidNullMarker),
            DataType::Boolean => {
                if cursor.u8()? > 1 {
                    return Err(RowError::InvalidBoolean);
                }
                add_size(size, 2, max_bytes)?;
            }
            DataType::Utf8 | DataType::Binary => {
                let value = cursor.bytes()?;
                i32::try_from(value.len()).map_err(|_| RowError::LengthOverflow)?;
                if matches!(field.data_type(), DataType::Utf8) {
                    std::str::from_utf8(value).map_err(|_| RowError::InvalidUtf8)?;
                }
                add_size(size, value.len().saturating_add(5), max_bytes)?;
            }
            DataType::List(child) => {
                let length = cursor.length()?;
                i32::try_from(length).map_err(|_| RowError::LengthOverflow)?;
                if length > cursor.remaining_len() {
                    return Err(RowError::Truncated);
                }
                add_size(size, 5, max_bytes)?;
                for _ in 0..length {
                    check_value(child, cursor, size, max_bytes)?;
                }
            }
            DataType::Struct(fields) => {
                add_size(size, 1, max_bytes)?;
                for child in fields {
                    check_value(child, cursor, size, max_bytes)?;
                }
            }
            data_type => {
                let width = data_type.primitive_width().ok_or(RowError::InvalidValue)?;
                cursor.skip(width)?;
                add_size(size, width + 1, max_bytes)?;
            }
        },
        _ => return Err(RowError::InvalidNullMarker),
    }
    Ok(())
}

fn decoded_array_shape(
    data_type: &DataType,
    rows: usize,
    size: &mut usize,
    max_bytes: usize,
) -> Result<(), RowError> {
    match data_type {
        DataType::Null => return Ok(()),
        DataType::List(child) => {
            add_size(size, rows.max(1).saturating_mul(4), max_bytes)?;
            if rows == 0 {
                add_size(size, 2 * size_of::<ArrayRef>(), max_bytes)?;
                decoded_array_shape(child.data_type(), 0, size, max_bytes)?;
            }
        }
        DataType::Struct(fields) => {
            if rows == 0 {
                add_size(
                    size,
                    fields.len().saturating_mul(2 * size_of::<ArrayRef>()),
                    max_bytes,
                )?;
            }
            for child in fields {
                decoded_array_shape(child.data_type(), rows, size, max_bytes)?;
            }
        }
        DataType::Utf8 | DataType::Binary => {
            add_size(size, rows.max(1).saturating_mul(4), max_bytes)?;
        }
        data_type => {
            let width = if matches!(data_type, DataType::Boolean) {
                1
            } else {
                data_type.primitive_width().ok_or(RowError::InvalidValue)?
            };
            add_size(size, rows.saturating_mul(width), max_bytes)?;
        }
    }
    add_size(size, rows.div_ceil(8), max_bytes)
}

fn row_builder(data_type: &DataType, rows: usize) -> Result<Box<dyn ArrayBuilder>, RowError> {
    Ok(match data_type {
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(rows, 0)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(rows, 0)),
        DataType::List(child) => Box::new(
            ListBuilder::with_capacity(row_builder(child.data_type(), 0)?, rows)
                .with_field(Arc::clone(child)),
        ),
        DataType::Struct(fields) => Box::new(StructBuilder::new(
            fields.clone(),
            fields
                .iter()
                .map(|field| row_builder(field.data_type(), rows))
                .collect::<Result<_, _>>()?,
        )),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Timestamp(_, _)
        | DataType::Decimal128(_, _) => arrow_array::builder::make_builder(data_type, rows),
        _ => return Err(RowError::InvalidValue),
    })
}

// The bounded slice entry point admits every complete row before entering here.
fn decode_rows<T: AsRef<[u8]>>(
    schema: &SchemaRef,
    rows: impl ExactSizeIterator<Item = T>,
) -> Result<RecordBatch, OperationError> {
    let count = rows.len();
    let mut builders = schema
        .fields()
        .iter()
        .map(|field| row_builder(field.data_type(), count))
        .collect::<Result<Vec<_>, _>>()?;
    for row in rows {
        append_row(
            std::slice::from_ref(schema),
            [Some(row.as_ref()), None],
            &mut builders,
        )?;
    }
    finish_builders(schema, builders, count)
}

fn append_row(
    schemas: &[SchemaRef],
    fragments: [Option<&[u8]>; 2],
    builders: &mut [Box<dyn ArrayBuilder>],
) -> Result<(), RowError> {
    let mut builders = builders.iter_mut();
    for (schema, fragment) in schemas.iter().zip(fragments) {
        let mut cursor = RowCursor::new(fragment.unwrap_or_default());
        for field in schema.fields() {
            append_value(
                field.data_type(),
                &mut cursor,
                builders.next().ok_or(RowError::InvalidValue)?.as_mut(),
                fragment.is_none(),
            )?;
        }
        cursor.finish()?;
    }
    if builders.next().is_some() {
        return Err(RowError::InvalidValue);
    }
    Ok(())
}

fn finish_builders(
    schema: &SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>,
    count: usize,
) -> Result<RecordBatch, OperationError> {
    let columns = builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect();
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(count)),
    )?)
}

#[allow(clippy::too_many_lines)]
fn append_value(
    data_type: &DataType,
    cursor: &mut RowCursor<'_>,
    builder: &mut dyn ArrayBuilder,
    parent_null: bool,
) -> Result<(), RowError> {
    let present = !parent_null && cursor.u8()? == 1;
    macro_rules! target {
        ($builder:ty) => {
            builder
                .as_any_mut()
                .downcast_mut::<$builder>()
                .ok_or(RowError::InvalidValue)?
        };
    }
    macro_rules! fixed {
        ($builder:ty, $type:ty) => {
            target!($builder).append_option(if present {
                Some(<$type>::from_be_bytes(cursor.take()?))
            } else {
                None
            })
        };
    }
    match data_type {
        DataType::Null => target!(NullBuilder).append_null(),
        DataType::Boolean => target!(BooleanBuilder).append_option(if present {
            Some(cursor.u8()? != 0)
        } else {
            None
        }),
        DataType::Int8 => fixed!(Int8Builder, i8),
        DataType::Int16 => fixed!(Int16Builder, i16),
        DataType::Int32 => fixed!(Int32Builder, i32),
        DataType::Int64 => fixed!(Int64Builder, i64),
        DataType::UInt8 => fixed!(UInt8Builder, u8),
        DataType::UInt16 => fixed!(UInt16Builder, u16),
        DataType::UInt32 => fixed!(UInt32Builder, u32),
        DataType::UInt64 => fixed!(UInt64Builder, u64),
        DataType::Float32 => fixed!(Float32Builder, f32),
        DataType::Float64 => fixed!(Float64Builder, f64),
        DataType::Date32 => fixed!(Date32Builder, i32),
        DataType::Timestamp(TimeUnit::Second, _) => fixed!(TimestampSecondBuilder, i64),
        DataType::Timestamp(TimeUnit::Millisecond, _) => fixed!(TimestampMillisecondBuilder, i64),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fixed!(TimestampMicrosecondBuilder, i64),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => fixed!(TimestampNanosecondBuilder, i64),
        DataType::Decimal128(_, _) => fixed!(Decimal128Builder, i128),
        DataType::Utf8 | DataType::Binary => {
            let value = if present { Some(cursor.bytes()?) } else { None };
            if let Some(value) = value {
                let current = if matches!(data_type, DataType::Utf8) {
                    target!(StringBuilder).values_slice().len()
                } else {
                    target!(BinaryBuilder).values_slice().len()
                };
                i32::try_from(
                    current
                        .checked_add(value.len())
                        .ok_or(RowError::LengthOverflow)?,
                )
                .map_err(|_| RowError::LengthOverflow)?;
            }
            if matches!(data_type, DataType::Utf8) {
                target!(StringBuilder).append_option(
                    value
                        .map(std::str::from_utf8)
                        .transpose()
                        .map_err(|_| RowError::InvalidUtf8)?,
                );
            } else {
                target!(BinaryBuilder).append_option(value);
            }
        }
        DataType::List(child) => {
            let builder = target!(ListBuilder<Box<dyn ArrayBuilder>>);
            if present {
                let length = cursor.length()?;
                i32::try_from(
                    builder
                        .values()
                        .len()
                        .checked_add(length)
                        .ok_or(RowError::LengthOverflow)?,
                )
                .map_err(|_| RowError::LengthOverflow)?;
                for _ in 0..length {
                    append_value(child.data_type(), cursor, builder.values().as_mut(), false)?;
                }
            }
            builder.append(present);
        }
        DataType::Struct(fields) => {
            let builder = target!(StructBuilder);
            for (field, child) in fields.iter().zip(builder.field_builders_mut()) {
                append_value(field.data_type(), cursor, child.as_mut(), !present)?;
            }
            builder.append(present);
        }
        _ => return Err(RowError::InvalidValue),
    }
    Ok(())
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

    fn skip(&mut self, length: usize) -> Result<(), RowError> {
        let (_, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(RowError::Truncated)?;
        self.remaining = remaining;
        Ok(())
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
    use super::*;
    use arrow_array::{
        Array, BooleanArray, Decimal128Array, ListArray, RecordBatch, StringArray, StructArray,
        TimestampMicrosecondArray, types::UInt64Type,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};

    #[test]
    fn canonical_decoder_round_trips_empty_lists_and_nullable_nested_values() {
        let lists = ListArray::from_iter_primitive::<UInt64Type, _, _>([
            Some(Vec::<Option<u64>>::new()),
            Some(vec![Some(7), None]),
            None,
        ]);
        let fields = Fields::from(vec![Field::new("text", DataType::Utf8, false)]);
        let structures = StructArray::new(
            fields.clone(),
            vec![Arc::new(StringArray::from(vec!["first", "hidden", "last"]))],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let empty =
            StructArray::new_empty_fields(3, Some(NullBuffer::from(vec![true, false, true])));
        let schema = Arc::new(Schema::new(vec![
            Field::new("items", lists.data_type().clone(), true),
            Field::new("structure", DataType::Struct(fields), true),
            Field::new("empty", empty.data_type().clone(), true),
        ]));
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(lists), Arc::new(structures), Arc::new(empty)],
        )
        .unwrap();
        let rows = (0..3)
            .map(|row| canonical_row_bounded(&records, row, usize::MAX).unwrap())
            .collect::<Vec<_>>();
        let rebuilt =
            decode_canonical_rows_bounded(&schema, &rows, &mut StepBudget::new(3, 4096)).unwrap();
        for (row, encoded) in rows.iter().enumerate() {
            assert_eq!(
                canonical_row_size_bounded(&records, row, encoded.len()).unwrap(),
                encoded.len()
            );
            assert_eq!(
                canonical_row_bounded(&rebuilt, row, usize::MAX).unwrap(),
                *encoded
            );
        }
        let structure = rebuilt
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(structure.column(0).len(), 3);
        assert!(structure.is_null(1));
        assert_eq!(
            structure
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(2),
            "last"
        );
    }

    #[test]
    fn empty_list_child_shapes_are_admitted_once_before_arrow_reconstruction() {
        let fields = Fields::from(
            (0..64)
                .map(|index| Field::new(format!("f{index}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
            false,
        )]));
        let encoded = [1, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut tight = StepBudget::new(1, 2048);
        let error = decode_canonical_rows_bounded(&schema, &[encoded], &mut tight).unwrap_err();
        assert!(error.is::<crate::operation::BudgetExceeded>());
        let mut enough = StepBudget::new(3, 4096);
        let batch = decode_canonical_rows_bounded(&schema, &[encoded; 3], &mut enough).unwrap();
        assert!(
            4096 - enough.remaining_bytes() < 2200,
            "empty child arrays exist once per batch"
        );
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            canonical_row_bounded(&batch, 2, usize::MAX).unwrap(),
            encoded
        );
    }

    #[test]
    fn a_rejected_output_batch_allocates_no_arrow_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let mut encoded = vec![1];
        encoded.extend_from_slice(&(128 * 1024_u64).to_be_bytes());
        encoded.resize(encoded.len() + 128 * 1024, b'x');
        encoded.extend_from_slice(&[1, 1]);
        let mut malformed = encoded.clone();
        *malformed.last_mut().unwrap() = 2;
        for (second, bytes) in [
            (encoded.as_slice(), 192 * 1024),
            (malformed.as_slice(), 1024 * 1024),
        ] {
            let mut output = ArrowOutput::default();
            let rows = [encoded.as_slice(), second]
                .into_iter()
                .map(|row| Ok(([Some(row), None], 1)));
            let error = output
                .extend(&[Arc::clone(&schema)], rows, &mut StepBudget::new(2, bytes))
                .unwrap_err();
            if bytes == 192 * 1024 {
                assert!(error.is::<crate::operation::BudgetExceeded>());
            } else {
                assert_eq!(
                    error.downcast_ref::<RowError>(),
                    Some(&RowError::InvalidBoolean)
                );
            }
            assert!(output.builders.is_empty());
            assert!(output.differences.is_empty());
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
            let encoded = canonical_row_bounded(&records, index, usize::MAX).unwrap();
            assert_eq!(
                canonical_row_size_bounded(&records, index, encoded.len()).unwrap(),
                encoded.len()
            );
        }
    }

    #[test]
    fn canonical_list_decoder_rejects_impossible_lengths_before_allocation() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        )]));
        let mut too_wide = vec![1];
        too_wide.extend_from_slice(&(u64::from(i32::MAX.cast_unsigned()) + 1).to_be_bytes());
        let mut truncated = vec![1];
        truncated.extend_from_slice(&2_u64.to_be_bytes());
        truncated.push(0);
        for (encoded, expected) in [
            (&too_wide, RowError::LengthOverflow),
            (&truncated, RowError::Truncated),
        ] {
            let error =
                decode_canonical_rows_bounded(&schema, &[encoded], &mut StepBudget::new(1, 4096))
                    .unwrap_err();
            assert_eq!(error.downcast_ref::<RowError>(), Some(&expected));
        }
    }

    #[test]
    fn compact_null_lists_need_no_per_child_owned_scalar_or_array_vectors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
            false,
        )]));
        let length = 128 * 1024_usize;
        let mut encoded = vec![1];
        encoded.extend_from_slice(&u64::try_from(length).unwrap().to_be_bytes());
        encoded.resize(encoded.len() + length, 0);
        let mut budget = StepBudget::new(1, 1024);
        let batch = decode_canonical_rows_bounded(&schema, &[&encoded], &mut budget).unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value(0).len(), length);
        assert!(1024 - budget.remaining_bytes() < 128);
        encoded[9] = 7;
        let error =
            decode_canonical_rows_bounded(&schema, &[&encoded], &mut StepBudget::new(1, 1024))
                .unwrap_err();
        assert_eq!(
            error.downcast_ref::<RowError>(),
            Some(&RowError::InvalidNullMarker)
        );
    }

    #[test]
    fn late_boolean_and_trailing_bytes_are_checked_before_any_batch_builder() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let mut encoded = vec![1];
        encoded.extend_from_slice(&131_072_u64.to_be_bytes());
        encoded.resize(encoded.len() + 131_072, b'x');
        encoded.extend_from_slice(&[1, 2]);
        let error = decode_canonical_rows_bounded(
            &schema,
            &[&encoded],
            &mut StepBudget::new(1, 1024 * 1024),
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<RowError>(),
            Some(&RowError::InvalidBoolean)
        );
        *encoded.last_mut().unwrap() = 1;
        encoded.push(0);
        let error = decode_canonical_rows_bounded(
            &schema,
            &[&encoded],
            &mut StepBudget::new(1, 1024 * 1024),
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<RowError>(),
            Some(&RowError::TrailingBytes)
        );
    }
}
