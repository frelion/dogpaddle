use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use datafusion_common::ScalarValue;

use super::AggregateError;

pub(super) fn scalar(field: &Field, encoded: &[u8]) -> Result<ScalarValue, AggregateError> {
    let mut value = Cursor::new(encoded);
    let scalar = value.scalar(field)?;
    value.finish()?;
    Ok(scalar)
}

pub(super) fn scalars(
    fields: &[Arc<Field>],
    encoded: &[u8],
) -> Result<Vec<ScalarValue>, AggregateError> {
    let mut value = Cursor::new(encoded);
    let scalars = fields
        .iter()
        .map(|field| value.scalar(field))
        .collect::<Result<Vec<_>, _>>()?;
    value.finish()?;
    Ok(scalars)
}

impl Cursor<'_> {
    fn scalar(&mut self, field: &Field) -> Result<ScalarValue, AggregateError> {
        match self.byte()? {
            0 => return null(field.data_type()),
            1 => {}
            _ => return Err(AggregateError::InvalidState),
        }
        let scalar = match field.data_type() {
            DataType::Boolean => ScalarValue::Boolean(Some(match self.byte()? {
                0 => false,
                1 => true,
                _ => return Err(AggregateError::InvalidState),
            })),
            DataType::Int8 => ScalarValue::Int8(Some(i8::from_be_bytes(self.take()?))),
            DataType::Int16 => ScalarValue::Int16(Some(i16::from_be_bytes(self.take()?))),
            DataType::Int32 => ScalarValue::Int32(Some(i32::from_be_bytes(self.take()?))),
            DataType::Int64 => ScalarValue::Int64(Some(i64::from_be_bytes(self.take()?))),
            DataType::UInt8 => ScalarValue::UInt8(Some(u8::from_be_bytes(self.take()?))),
            DataType::UInt16 => ScalarValue::UInt16(Some(u16::from_be_bytes(self.take()?))),
            DataType::UInt32 => ScalarValue::UInt32(Some(u32::from_be_bytes(self.take()?))),
            DataType::UInt64 => ScalarValue::UInt64(Some(u64::from_be_bytes(self.take()?))),
            DataType::Float32 => {
                ScalarValue::Float32(Some(f32::from_bits(u32::from_be_bytes(self.take()?))))
            }
            DataType::Float64 => {
                ScalarValue::Float64(Some(f64::from_bits(u64::from_be_bytes(self.take()?))))
            }
            DataType::Date32 => ScalarValue::Date32(Some(i32::from_be_bytes(self.take()?))),
            DataType::Timestamp(unit, timezone) => {
                let raw = Some(i64::from_be_bytes(self.take()?));
                let timezone = timezone.as_ref().map(Arc::clone);
                match unit {
                    TimeUnit::Second => ScalarValue::TimestampSecond(raw, timezone),
                    TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(raw, timezone),
                    TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(raw, timezone),
                    TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(raw, timezone),
                }
            }
            DataType::Decimal128(precision, scale) => {
                ScalarValue::Decimal128(Some(i128::from_be_bytes(self.take()?)), *precision, *scale)
            }
            DataType::Utf8 => {
                let bytes = self.bytes()?;
                ScalarValue::Utf8(Some(
                    std::str::from_utf8(bytes)
                        .map_err(|_| AggregateError::InvalidState)?
                        .to_owned(),
                ))
            }
            DataType::Binary => ScalarValue::Binary(Some(self.bytes()?.to_vec())),
            _ => return Err(AggregateError::InvalidState),
        };
        Ok(scalar)
    }
}

pub(super) fn null(data_type: &DataType) -> Result<ScalarValue, AggregateError> {
    ScalarValue::try_from(data_type).map_err(AggregateError::DataFusion)
}

pub(super) fn indexable(data_type: &DataType) -> bool {
    matches!(
        data_type,
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
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

pub(super) fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::List(child) => contains_float(child.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        _ => false,
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn byte(&mut self) -> Result<u8, AggregateError> {
        Ok(self.take::<1>()?[0])
    }

    fn bytes(&mut self) -> Result<&'a [u8], AggregateError> {
        let length = usize::try_from(u64::from_be_bytes(self.take()?))
            .map_err(|_| AggregateError::InvalidState)?;
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(AggregateError::InvalidState)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], AggregateError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(AggregateError::InvalidState)?;
        self.remaining = remaining;
        Ok(*value)
    }

    fn finish(self) -> Result<(), AggregateError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(AggregateError::InvalidState)
        }
    }
}
