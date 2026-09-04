use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, SchemaRef, TimeUnit};
use rusqlite::{
    Row,
    types::{Value, ValueRef},
};
use thiserror::Error;

const HASH_DOMAIN: &[u8] = b"dogpaddle.sqlite-row.v1\0";
const HASH_LENGTH: usize = 16;

/// A schema-bound encoder for `SQLite` sink rows.
#[derive(Debug)]
pub(super) struct RowCodec {
    schema: SchemaRef,
}

impl RowCodec {
    /// Captures a Schema already validated by the common Operation binding and
    /// the SQLite-specific identifier checks.
    pub(super) const fn new_validated(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Encodes one logical row into its stable identity and `SQLite` values.
    pub(super) fn encode_row(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<EncodedRow, RowError> {
        debug_assert_eq!(batch.schema_ref().as_ref(), self.schema.as_ref());
        if row_index >= batch.num_rows() {
            return Err(RowError::RowOutOfBounds {
                row_index,
                rows: batch.num_rows(),
            });
        }

        let mut canonical = Vec::new();
        let mut values = Vec::with_capacity(self.schema.fields().len());
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            values.push(encode_value(
                field,
                array.as_ref(),
                row_index,
                field.name(),
                &mut canonical,
            )?);
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(HASH_DOMAIN);
        hasher.update(&canonical);
        let mut hash = [0_u8; HASH_LENGTH];
        hash.copy_from_slice(&hasher.finalize().as_bytes()[..HASH_LENGTH]);

        Ok(EncodedRow {
            canonical,
            hash,
            values,
        })
    }
}

/// Stable row identity plus the values bound to logical `SQLite` columns.
#[derive(Debug, PartialEq)]
pub(super) struct EncodedRow {
    pub(super) canonical: Vec<u8>,
    pub(super) hash: [u8; HASH_LENGTH],
    pub(super) values: Vec<Value>,
}

impl EncodedRow {
    pub(super) fn take_canonical(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.canonical)
    }

    /// Compares the logical columns in a `SQLite` result row exactly.
    pub(super) fn matches(&self, row: &Row<'_>, logical_offset: usize) -> rusqlite::Result<bool> {
        for (offset, expected) in self.values.iter().enumerate() {
            if !value_matches(row.get_ref(logical_offset + offset)?, expected) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// A failure while encoding one logical row.
#[derive(Debug, Error)]
pub(super) enum RowError {
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

fn encode_value(
    field: &Field,
    array: &dyn Array,
    index: usize,
    path: &str,
    canonical: &mut Vec<u8>,
) -> Result<Value, RowError> {
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
    let is_null_type = matches!(field.data_type(), DataType::Null);
    if is_null_type || array.is_null(index) {
        if !field.is_nullable() && !is_null_type {
            return Err(RowError::UnexpectedNull {
                field: path.to_owned(),
            });
        }
        canonical.push(0);
        return Ok(Value::Null);
    }

    let canonical_start = canonical.len();
    canonical.push(1);
    match field.data_type() {
        DataType::Null => unreachable!("handled before the nullable value path"),
        DataType::Boolean => {
            let value = downcast::<BooleanArray>(array, path, field.data_type())?.value(index);
            canonical.push(u8::from(value));
            Ok(Value::Integer(i64::from(value)))
        }
        DataType::Int8
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
        | DataType::Decimal128(_, _) => encode_fixed_width(field, array, index, path, canonical),
        DataType::Utf8 => {
            let value = downcast::<StringArray>(array, path, field.data_type())?.value(index);
            encode_bytes(value.as_bytes(), canonical)?;
            Ok(Value::Text(value.to_owned()))
        }
        DataType::Binary => {
            let value = downcast::<BinaryArray>(array, path, field.data_type())?.value(index);
            encode_bytes(value, canonical)?;
            Ok(Value::Blob(value.to_vec()))
        }
        DataType::List(child) => {
            encode_list(field, child, array, index, path, canonical)?;
            Ok(Value::Blob(canonical[canonical_start..].to_vec()))
        }
        DataType::Struct(fields) => {
            encode_struct(field, fields, array, index, path, canonical)?;
            Ok(Value::Blob(canonical[canonical_start..].to_vec()))
        }
        other => Err(RowError::ArrayTypeMismatch {
            field: path.to_owned(),
            expected: other.clone(),
            actual: array.data_type().clone(),
        }),
    }
}

fn encode_fixed_width(
    field: &Field,
    array: &dyn Array,
    index: usize,
    path: &str,
    canonical: &mut Vec<u8>,
) -> Result<Value, RowError> {
    macro_rules! integer {
        ($array:ty) => {{
            let value = downcast::<$array>(array, path, field.data_type())?.value(index);
            canonical.extend_from_slice(&value.to_be_bytes());
            Ok(Value::Integer(i64::from(value)))
        }};
    }

    macro_rules! timestamp {
        ($array:ty) => {{
            let value = downcast::<$array>(array, path, field.data_type())?.value(index);
            canonical.extend_from_slice(&value.to_be_bytes());
            Ok(Value::Integer(value))
        }};
    }

    match field.data_type() {
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => {
            let value = downcast::<UInt64Array>(array, path, field.data_type())?.value(index);
            let bytes = value.to_be_bytes();
            canonical.extend_from_slice(&bytes);
            Ok(Value::Blob(bytes.to_vec()))
        }
        DataType::Float32 => {
            let bits = downcast::<Float32Array>(array, path, field.data_type())?
                .value(index)
                .to_bits();
            let bytes = bits.to_be_bytes();
            canonical.extend_from_slice(&bytes);
            Ok(Value::Blob(bytes.to_vec()))
        }
        DataType::Float64 => {
            let bits = downcast::<Float64Array>(array, path, field.data_type())?
                .value(index)
                .to_bits();
            let bytes = bits.to_be_bytes();
            canonical.extend_from_slice(&bytes);
            Ok(Value::Blob(bytes.to_vec()))
        }
        DataType::Date32 => integer!(Date32Array),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => timestamp!(TimestampSecondArray),
            TimeUnit::Millisecond => timestamp!(TimestampMillisecondArray),
            TimeUnit::Microsecond => timestamp!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => timestamp!(TimestampNanosecondArray),
        },
        DataType::Decimal128(_, _) => {
            let value = downcast::<Decimal128Array>(array, path, field.data_type())?.value(index);
            let bytes = value.to_be_bytes();
            canonical.extend_from_slice(&bytes);
            Ok(Value::Blob(bytes.to_vec()))
        }
        _ => unreachable!("encode_fixed_width receives only fixed-width v1 fields"),
    }
}

fn encode_list(
    field: &Field,
    child: &Field,
    array: &dyn Array,
    index: usize,
    path: &str,
    canonical: &mut Vec<u8>,
) -> Result<(), RowError> {
    let list = downcast::<ListArray>(array, path, field.data_type())?;
    let values = list.value(index);
    encode_length(values.len(), canonical)?;
    for child_index in 0..values.len() {
        let _ = encode_value(
            child,
            values.as_ref(),
            child_index,
            &join_path(path, child.name()),
            canonical,
        )?;
    }
    Ok(())
}

fn encode_struct(
    field: &Field,
    fields: &Fields,
    array: &dyn Array,
    index: usize,
    path: &str,
    canonical: &mut Vec<u8>,
) -> Result<(), RowError> {
    let structure = downcast::<StructArray>(array, path, field.data_type())?;
    for (child, child_array) in fields.iter().zip(structure.columns()) {
        let _ = encode_value(
            child,
            child_array.as_ref(),
            index,
            &join_path(path, child.name()),
            canonical,
        )?;
    }
    Ok(())
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    path: &str,
    expected: &DataType,
) -> Result<&'a T, RowError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| RowError::ArrayTypeMismatch {
            field: path.to_owned(),
            expected: expected.clone(),
            actual: array.data_type().clone(),
        })
}

fn encode_bytes(bytes: &[u8], output: &mut Vec<u8>) -> Result<(), RowError> {
    encode_length(bytes.len(), output)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_length(length: usize, output: &mut Vec<u8>) -> Result<(), RowError> {
    let length = u64::try_from(length).map_err(|_| RowError::LengthOverflow)?;
    output.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn value_matches(actual: ValueRef<'_>, expected: &Value) -> bool {
    match (actual, expected) {
        (ValueRef::Null, Value::Null) => true,
        (ValueRef::Integer(actual), Value::Integer(expected)) => actual == *expected,
        (ValueRef::Text(actual), Value::Text(expected)) => actual == expected.as_bytes(),
        (ValueRef::Blob(actual), Value::Blob(expected)) => actual == expected,
        _ => false,
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}.{name}")
    }
}
