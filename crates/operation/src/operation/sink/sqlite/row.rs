use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use rusqlite::types::Value;
#[cfg(test)]
use rusqlite::{Row, types::ValueRef};

pub(super) use crate::operation::sink::relation::RowError;
use crate::operation::sink::relation::{encode_canonical, row_hash};

/// Maps shared canonical row values to `SQLite`'s exact storage representation.
#[derive(Debug)]
pub(super) struct RowCodec {
    schema: SchemaRef,
}

impl RowCodec {
    pub(super) const fn new_validated(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn encode_row(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<EncodedRow, RowError> {
        if batch.schema_ref().as_ref() != self.schema.as_ref() {
            return Err(RowError::SchemaMismatch);
        }
        if row_index >= batch.num_rows() {
            return Err(RowError::RowOutOfBounds {
                row_index,
                rows: batch.num_rows(),
            });
        }
        let mut canonical = Vec::new();
        let mut values = Vec::with_capacity(self.schema.fields().len());
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            let start = canonical.len();
            encode_canonical(
                field,
                array.as_ref(),
                row_index,
                field.name(),
                &mut canonical,
            )?;
            values.push(sqlite_value(field.data_type(), &canonical[start..]));
        }
        Ok(EncodedRow {
            hash: row_hash(&canonical),
            #[cfg(test)]
            canonical,
            values,
        })
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct EncodedRow {
    #[cfg(test)]
    pub(super) canonical: Vec<u8>,
    pub(super) hash: [u8; 16],
    pub(super) values: Vec<Value>,
}

#[cfg(test)]
impl EncodedRow {
    pub(super) fn matches(&self, row: &Row<'_>, logical_offset: usize) -> rusqlite::Result<bool> {
        for (offset, expected) in self.values.iter().enumerate() {
            let actual = row.get_ref(logical_offset + offset)?;
            let matches = match (actual, expected) {
                (ValueRef::Null, Value::Null) => true,
                (ValueRef::Integer(actual), Value::Integer(expected)) => actual == *expected,
                (ValueRef::Text(actual), Value::Text(expected)) => actual == expected.as_bytes(),
                (ValueRef::Blob(actual), Value::Blob(expected)) => actual == expected,
                _ => false,
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn sqlite_value(data_type: &DataType, canonical: &[u8]) -> Value {
    if canonical[0] == 0 {
        return Value::Null;
    }
    let bytes = &canonical[1..];
    macro_rules! integer {
        ($kind:ty) => {
            Value::Integer(i64::from(<$kind>::from_be_bytes(
                bytes.try_into().expect("validated canonical width"),
            )))
        };
    }
    match data_type {
        DataType::Boolean | DataType::UInt8 => integer!(u8),
        DataType::Int8 => integer!(i8),
        DataType::Int16 => integer!(i16),
        DataType::Int32 | DataType::Date32 => integer!(i32),
        DataType::Int64 | DataType::Timestamp(_, _) => integer!(i64),
        DataType::UInt16 => integer!(u16),
        DataType::UInt32 => integer!(u32),
        DataType::Utf8 => {
            Value::Text(String::from_utf8(bytes[8..].to_vec()).expect("canonical Arrow UTF-8"))
        }
        DataType::Binary => Value::Blob(bytes[8..].to_vec()),
        DataType::List(_) | DataType::Struct(_) => Value::Blob(canonical.to_vec()),
        DataType::UInt64 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            Value::Blob(bytes.to_vec())
        }
        _ => unreachable!("binding and canonical encoding accept only supported DogPaddle types"),
    }
}
