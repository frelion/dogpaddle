use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use mysql::Value;

use super::{error::DorisSinkError, schema::DorisLayout};
use crate::operation::sink::relation::{RowError, encode_canonical, row_hash};

#[derive(Debug)]
pub(super) struct DorisRowCodec {
    layout: DorisLayout,
}

impl DorisRowCodec {
    pub(super) const fn new(layout: DorisLayout) -> Self {
        Self { layout }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.layout.schema()
    }

    pub(super) const fn layout(&self) -> &DorisLayout {
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
        for (field, array) in self.schema().fields().iter().zip(batch.columns()) {
            let start = canonical.len();
            encode_canonical(
                field,
                array.as_ref(),
                row_index,
                field.name(),
                &mut canonical,
            )?;
            values.push(doris_value(field.data_type(), &canonical[start..]));
        }
        Ok(EncodedRow {
            hash: hex(&row_hash(&canonical)),
            values,
        })
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct EncodedRow {
    pub(super) hash: Vec<u8>,
    pub(super) values: Vec<Value>,
}

fn doris_value(data_type: &DataType, canonical: &[u8]) -> Value {
    // encode_canonical already checked the field's Arrow type, physical array,
    // index and nullability before producing this complete field slice.
    if canonical[0] == 0 {
        return Value::NULL;
    }
    let bytes = &canonical[1..];
    macro_rules! integer {
        ($kind:ty, $value:ident) => {
            Value::$value(
                <$kind>::from_be_bytes(bytes.try_into().expect("validated canonical width")).into(),
            )
        };
    }
    match data_type {
        DataType::Boolean => Value::Int(i64::from(bytes[0])),
        DataType::Int8 => integer!(i8, Int),
        DataType::Int16 => integer!(i16, Int),
        DataType::Int32 | DataType::Date32 => integer!(i32, Int),
        DataType::Int64 | DataType::Timestamp(_, _) => integer!(i64, Int),
        DataType::UInt8 => integer!(u8, UInt),
        DataType::UInt16 => integer!(u16, UInt),
        DataType::UInt32 => integer!(u32, UInt),
        DataType::Utf8 => Value::Bytes(bytes[8..].to_vec()),
        DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::List(_)
        | DataType::Struct(_) => Value::Bytes(STANDARD.encode(canonical).into_bytes()),
        _ => unreachable!("binding and canonical encoding accept only supported DogPaddle types"),
    }
}

fn hex(bytes: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)]);
        output.push(DIGITS[usize::from(byte & 0x0f)]);
    }
    output
}

impl From<RowError> for DorisSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
