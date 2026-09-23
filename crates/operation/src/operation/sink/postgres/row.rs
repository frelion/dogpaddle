use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef};
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
            values.push(postgres_value(field, column.storage(), &canonical[start..]));
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

fn postgres_value(field: &Field, storage: StorageType, canonical: &[u8]) -> PostgresValue {
    // encode_canonical validated the marker, width, and array type before this conversion.
    if canonical[0] == 0 {
        return null_value(storage);
    }
    let bytes = &canonical[1..];
    macro_rules! integer {
        ($source:ty, $target:ty, $variant:ident) => {
            PostgresValue::$variant(Some(<$target>::from(<$source>::from_be_bytes(
                bytes.try_into().expect("validated canonical width"),
            ))))
        };
    }
    match field.data_type() {
        DataType::Boolean => PostgresValue::Boolean(Some(bytes[0] == 1)),
        DataType::Int8 => integer!(i8, i16, Int16),
        DataType::Int16 => integer!(i16, i16, Int16),
        DataType::Int32 | DataType::Date32 => integer!(i32, i32, Int32),
        DataType::Int64 | DataType::Timestamp(_, _) => integer!(i64, i64, Int64),
        DataType::UInt8 => integer!(u8, i16, Int16),
        DataType::UInt16 => integer!(u16, i32, Int32),
        DataType::UInt32 => integer!(u32, i64, Int64),
        DataType::UInt64 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            PostgresValue::Bytes(Some(bytes.to_vec()))
        }
        DataType::Utf8 | DataType::Binary => PostgresValue::Bytes(Some(bytes[8..].to_vec())),
        DataType::List(_) | DataType::Struct(_) => PostgresValue::Bytes(Some(canonical.to_vec())),
        _ => unreachable!("layout and canonical encoding accept only supported DogPaddle types"),
    }
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

impl From<RowError> for PostgresSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
