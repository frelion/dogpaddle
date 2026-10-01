use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Number, Value};

use super::{
    error::{ClickHouseSinkError, ClickHouseSinkSchemaError},
    schema,
};
use crate::operation::sink::relation::{RowError, encode_target_values, row_hash};

#[derive(Debug)]
pub(super) struct ClickHouseRowCodec {
    schema: SchemaRef,
}

impl ClickHouseRowCodec {
    pub(super) fn try_new(schema: SchemaRef) -> Result<Self, ClickHouseSinkSchemaError> {
        schema::validate(&schema)?;
        Ok(Self { schema })
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn encode_row(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<EncodedRow, RowError> {
        let (canonical, values) =
            encode_target_values(self.schema(), batch, row_index, |field, bytes| {
                clickhouse_value(field.data_type(), bytes)
            })?;
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

fn clickhouse_value(data_type: &DataType, canonical: &[u8]) -> Value {
    if canonical[0] == 0 {
        return Value::Null;
    }
    // encode_canonical already checked the Arrow type and nullability. Fixed-width
    // values follow the non-null marker in big-endian order; encoded columns keep
    // the entire canonical value (including its marker) for exact row comparison.
    let bytes = &canonical[1..];
    macro_rules! number {
        ($kind:ty) => {
            Value::Number(Number::from(<$kind>::from_be_bytes(
                bytes.try_into().expect("validated canonical width"),
            )))
        };
    }
    match data_type {
        DataType::Boolean | DataType::UInt8 => number!(u8),
        DataType::Int8 => number!(i8),
        DataType::Int16 => number!(i16),
        DataType::Int32 | DataType::Date32 => number!(i32),
        DataType::Int64 | DataType::Timestamp(_, _) => number!(i64),
        DataType::UInt16 => number!(u16),
        DataType::UInt32 => number!(u32),
        DataType::UInt64 => number!(u64),
        DataType::Utf8
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => Value::String(STANDARD.encode(canonical)),
        _ => unreachable!("binding and canonical encoding accept only supported DogPaddle types"),
    }
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
