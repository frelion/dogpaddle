use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use postgres::types::ToSql;

use super::{
    error::PostgresSinkError,
    schema::{PostgresLayout, StorageType},
};
use crate::operation::sink::relation::{RowError, encode_canonical, row_hash};

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

        Ok(EncodedRow {
            hash: row_hash(&canonical),
            values,
        })
    }
}

/// Stable row identity and values ready for postgres parameter binding.
#[derive(Debug, PartialEq)]
pub(crate) struct EncodedRow {
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

impl From<RowError> for PostgresSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
