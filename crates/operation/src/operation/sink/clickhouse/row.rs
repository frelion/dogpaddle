use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Number, Value};

use super::{
    error::ClickHouseSinkError,
    schema::{ClickHouseLayout, ColumnLayout},
};
use crate::operation::sink::relation::{RowError, encode_canonical, row_hash};

#[derive(Debug)]
pub(super) struct ClickHouseRowCodec {
    layout: ClickHouseLayout,
}

impl ClickHouseRowCodec {
    pub(super) const fn new(layout: ClickHouseLayout) -> Self {
        Self { layout }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.layout.schema()
    }

    pub(super) const fn layout(&self) -> &ClickHouseLayout {
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
            values.push(clickhouse_value(
                field.data_type(),
                column,
                &canonical[start..],
            ));
        }
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

fn clickhouse_value(data_type: &DataType, column: &ColumnLayout, canonical: &[u8]) -> Value {
    if canonical[0] == 0 {
        return Value::Null;
    }
    if column.encoded() {
        return Value::String(STANDARD.encode(canonical));
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
