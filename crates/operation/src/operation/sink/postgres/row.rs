use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, SchemaRef, TimeUnit};
use postgres::types::ToSql;
use thiserror::Error;

use super::schema::{PostgresLayout, StorageType};

const HASH_DOMAIN: &[u8] = b"dogpaddle.postgres-row.v1\0";
pub(super) const HASH_LENGTH: usize = 16;

/// Schema-bound encoder for one `PostgreSQL` relation target.
#[derive(Debug)]
pub(super) struct PostgresRowCodec {
    layout: PostgresLayout,
}

impl PostgresRowCodec {
    pub(super) const fn new(layout: PostgresLayout) -> Self {
        Self { layout }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.layout.schema()
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
            values.push(postgres_value(
                field,
                array.as_ref(),
                row_index,
                column.storage(),
                &canonical[start..],
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

/// Stable row identity and values ready for postgres parameter binding.
#[derive(Debug, PartialEq)]
pub(crate) struct EncodedRow {
    pub(super) canonical: Vec<u8>,
    pub(super) hash: [u8; HASH_LENGTH],
    pub(super) values: Vec<PostgresValue>,
}

/// One owned, correctly typed `PostgreSQL` parameter, including typed NULLs.
#[derive(Debug, PartialEq)]
pub(super) enum PostgresValue {
    Boolean(Option<bool>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Bytes(Option<Vec<u8>>),
}

impl PostgresValue {
    pub(super) fn as_parameter(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Boolean(value) => value,
            Self::Int16(value) => value,
            Self::Int32(value) => value,
            Self::Int64(value) => value,
            Self::Bytes(value) => value,
        }
    }
}

/// Failure while encoding one logical Arrow row.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(super) enum RowError {
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

macro_rules! fixed {
    ($output:expr, $value:expr) => {{
        $output.extend_from_slice(&$value.to_be_bytes());
    }};
}

fn postgres_value(
    field: &Field,
    array: &dyn Array,
    index: usize,
    storage: StorageType,
    canonical: &[u8],
) -> Result<PostgresValue, RowError> {
    if matches!(field.data_type(), DataType::Null) || array.is_null(index) {
        return Ok(null_value(storage));
    }
    Ok(match field.data_type() {
        DataType::Null => unreachable!("handled by the null branch"),
        DataType::Boolean => PostgresValue::Boolean(Some(
            downcast::<BooleanArray>(array, field.name(), field.data_type())?.value(index),
        )),
        DataType::Int8 => PostgresValue::Int16(Some(i16::from(
            downcast::<Int8Array>(array, field.name(), field.data_type())?.value(index),
        ))),
        DataType::Int16 => PostgresValue::Int16(Some(
            downcast::<Int16Array>(array, field.name(), field.data_type())?.value(index),
        )),
        DataType::Int32 => PostgresValue::Int32(Some(
            downcast::<Int32Array>(array, field.name(), field.data_type())?.value(index),
        )),
        DataType::Int64 => PostgresValue::Int64(Some(
            downcast::<Int64Array>(array, field.name(), field.data_type())?.value(index),
        )),
        DataType::UInt8 => PostgresValue::Int16(Some(i16::from(
            downcast::<UInt8Array>(array, field.name(), field.data_type())?.value(index),
        ))),
        DataType::UInt16 => PostgresValue::Int32(Some(i32::from(
            downcast::<UInt16Array>(array, field.name(), field.data_type())?.value(index),
        ))),
        DataType::UInt32 => PostgresValue::Int64(Some(i64::from(
            downcast::<UInt32Array>(array, field.name(), field.data_type())?.value(index),
        ))),
        DataType::UInt64 => PostgresValue::Bytes(Some(
            downcast::<UInt64Array>(array, field.name(), field.data_type())?
                .value(index)
                .to_be_bytes()
                .to_vec(),
        )),
        DataType::Float32 => PostgresValue::Bytes(Some(
            downcast::<Float32Array>(array, field.name(), field.data_type())?
                .value(index)
                .to_bits()
                .to_be_bytes()
                .to_vec(),
        )),
        DataType::Float64 => PostgresValue::Bytes(Some(
            downcast::<Float64Array>(array, field.name(), field.data_type())?
                .value(index)
                .to_bits()
                .to_be_bytes()
                .to_vec(),
        )),
        DataType::Date32 => PostgresValue::Int32(Some(
            downcast::<Date32Array>(array, field.name(), field.data_type())?.value(index),
        )),
        DataType::Timestamp(unit, _) => PostgresValue::Int64(Some(match unit {
            TimeUnit::Second => {
                downcast::<TimestampSecondArray>(array, field.name(), field.data_type())?
                    .value(index)
            }
            TimeUnit::Millisecond => {
                downcast::<TimestampMillisecondArray>(array, field.name(), field.data_type())?
                    .value(index)
            }
            TimeUnit::Microsecond => {
                downcast::<TimestampMicrosecondArray>(array, field.name(), field.data_type())?
                    .value(index)
            }
            TimeUnit::Nanosecond => {
                downcast::<TimestampNanosecondArray>(array, field.name(), field.data_type())?
                    .value(index)
            }
        })),
        DataType::Decimal128(_, _) => PostgresValue::Bytes(Some(
            downcast::<Decimal128Array>(array, field.name(), field.data_type())?
                .value(index)
                .to_be_bytes()
                .to_vec(),
        )),
        DataType::Utf8 => PostgresValue::Bytes(Some(
            downcast::<StringArray>(array, field.name(), field.data_type())?
                .value(index)
                .as_bytes()
                .to_vec(),
        )),
        DataType::Binary => PostgresValue::Bytes(Some(
            downcast::<BinaryArray>(array, field.name(), field.data_type())?
                .value(index)
                .to_vec(),
        )),
        DataType::List(_) | DataType::Struct(_) => PostgresValue::Bytes(Some(canonical.to_vec())),
        unsupported => {
            return Err(RowError::ArrayTypeMismatch {
                field: field.name().clone(),
                expected: unsupported.clone(),
                actual: array.data_type().clone(),
            });
        }
    })
}

const fn null_value(storage: StorageType) -> PostgresValue {
    match storage {
        StorageType::Boolean => PostgresValue::Boolean(None),
        StorageType::Int16 => PostgresValue::Int16(None),
        StorageType::Int32 => PostgresValue::Int32(None),
        StorageType::Int64 => PostgresValue::Int64(None),
        StorageType::Bytes(_) => PostgresValue::Bytes(None),
    }
}

#[allow(clippy::too_many_lines)]
fn encode_canonical(
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
    let is_null_type = matches!(field.data_type(), DataType::Null);
    if is_null_type || array.is_null(index) {
        if !field.is_nullable() && !is_null_type {
            return Err(RowError::UnexpectedNull {
                field: path.to_owned(),
            });
        }
        output.push(0);
        return Ok(());
    }

    output.push(1);
    match field.data_type() {
        DataType::Null => unreachable!("handled before the non-null value path"),
        DataType::Boolean => output.push(u8::from(
            downcast::<BooleanArray>(array, path, field.data_type())?.value(index),
        )),
        DataType::Int8 => fixed!(
            output,
            downcast::<Int8Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Int16 => fixed!(
            output,
            downcast::<Int16Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Int32 => fixed!(
            output,
            downcast::<Int32Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Int64 => fixed!(
            output,
            downcast::<Int64Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::UInt8 => fixed!(
            output,
            downcast::<UInt8Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::UInt16 => fixed!(
            output,
            downcast::<UInt16Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::UInt32 => fixed!(
            output,
            downcast::<UInt32Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::UInt64 => fixed!(
            output,
            downcast::<UInt64Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Float32 => fixed!(
            output,
            downcast::<Float32Array>(array, path, field.data_type())?
                .value(index)
                .to_bits()
        ),
        DataType::Float64 => fixed!(
            output,
            downcast::<Float64Array>(array, path, field.data_type())?
                .value(index)
                .to_bits()
        ),
        DataType::Date32 => fixed!(
            output,
            downcast::<Date32Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => fixed!(
                output,
                downcast::<TimestampSecondArray>(array, path, field.data_type())?.value(index)
            ),
            TimeUnit::Millisecond => fixed!(
                output,
                downcast::<TimestampMillisecondArray>(array, path, field.data_type())?.value(index)
            ),
            TimeUnit::Microsecond => fixed!(
                output,
                downcast::<TimestampMicrosecondArray>(array, path, field.data_type())?.value(index)
            ),
            TimeUnit::Nanosecond => fixed!(
                output,
                downcast::<TimestampNanosecondArray>(array, path, field.data_type())?.value(index)
            ),
        },
        DataType::Decimal128(_, _) => fixed!(
            output,
            downcast::<Decimal128Array>(array, path, field.data_type())?.value(index)
        ),
        DataType::Utf8 => encode_bytes(
            downcast::<StringArray>(array, path, field.data_type())?
                .value(index)
                .as_bytes(),
            output,
        )?,
        DataType::Binary => encode_bytes(
            downcast::<BinaryArray>(array, path, field.data_type())?.value(index),
            output,
        )?,
        DataType::List(child) => {
            let list = downcast::<ListArray>(array, path, field.data_type())?;
            let values = list.value(index);
            encode_length(values.len(), output)?;
            let child_path = join_path(path, child.name());
            for child_index in 0..values.len() {
                encode_canonical(child, values.as_ref(), child_index, &child_path, output)?;
            }
        }
        DataType::Struct(fields) => {
            encode_struct(field, fields, array, index, path, output)?;
        }
        unsupported => {
            return Err(RowError::ArrayTypeMismatch {
                field: path.to_owned(),
                expected: unsupported.clone(),
                actual: array.data_type().clone(),
            });
        }
    }
    Ok(())
}

fn encode_struct(
    field: &Field,
    fields: &Fields,
    array: &dyn Array,
    index: usize,
    path: &str,
    output: &mut Vec<u8>,
) -> Result<(), RowError> {
    let structure = downcast::<StructArray>(array, path, field.data_type())?;
    for (child, child_array) in fields.iter().zip(structure.columns()) {
        encode_canonical(
            child,
            child_array.as_ref(),
            index,
            &join_path(path, child.name()),
            output,
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

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}.{name}")
    }
}
