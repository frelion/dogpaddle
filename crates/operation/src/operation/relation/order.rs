//! Stable ordered scalar encoding shared by relational operations.

use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use datafusion_common::ScalarValue;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum OrderError {
    #[error("scalar value does not match the ordered field type")]
    InvalidValue,
    #[error("ordered scalar encoding is invalid")]
    InvalidEncoding,
}

/// Encodes one scalar so byte order equals its logical ascending order.
///
/// Variable-width results are unframed. A composite key must escape and terminate
/// each component before concatenation so component boundaries remain distinct.
pub(crate) fn order_key(field: &Field, value: &ScalarValue) -> Result<Option<Vec<u8>>, OrderError> {
    if value.is_null() {
        return Ok(None);
    }

    let mut encoded = Vec::new();
    match (field.data_type(), value) {
        (DataType::Boolean, ScalarValue::Boolean(Some(value))) => {
            encoded.push(u8::from(*value));
        }
        (DataType::Int8, ScalarValue::Int8(Some(value))) => {
            signed(value.to_be_bytes(), &mut encoded);
        }
        (DataType::Int16, ScalarValue::Int16(Some(value))) => {
            signed(value.to_be_bytes(), &mut encoded);
        }
        (DataType::Int32, ScalarValue::Int32(Some(value)))
        | (DataType::Date32, ScalarValue::Date32(Some(value))) => {
            signed(value.to_be_bytes(), &mut encoded);
        }
        (DataType::Int64, ScalarValue::Int64(Some(value))) => {
            signed(value.to_be_bytes(), &mut encoded);
        }
        (DataType::UInt8, ScalarValue::UInt8(Some(value))) => encoded.push(*value),
        (DataType::UInt16, ScalarValue::UInt16(Some(value))) => {
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        (DataType::UInt32, ScalarValue::UInt32(Some(value))) => {
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        (DataType::UInt64, ScalarValue::UInt64(Some(value))) => {
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        (DataType::Utf8, ScalarValue::Utf8(Some(value))) => {
            encoded.extend_from_slice(value.as_bytes());
        }
        (DataType::Binary, ScalarValue::Binary(Some(value))) => {
            encoded.extend_from_slice(value);
        }
        (
            DataType::Timestamp(TimeUnit::Second, _),
            ScalarValue::TimestampSecond(Some(value), _),
        )
        | (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            ScalarValue::TimestampMillisecond(Some(value), _),
        )
        | (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            ScalarValue::TimestampMicrosecond(Some(value), _),
        )
        | (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            ScalarValue::TimestampNanosecond(Some(value), _),
        ) => signed(value.to_be_bytes(), &mut encoded),
        (DataType::Decimal128(precision, scale), ScalarValue::Decimal128(Some(value), p, s))
            if precision == p && scale == s =>
        {
            signed(value.to_be_bytes(), &mut encoded);
        }
        _ => return Err(OrderError::InvalidValue),
    }
    Ok(Some(encoded))
}

/// Decodes one complete scalar component produced by [`order_key`].
pub(crate) fn ordered_value(field: &Field, encoded: &[u8]) -> Result<ScalarValue, OrderError> {
    let value = match field.data_type() {
        DataType::Boolean => match encoded {
            [0] => ScalarValue::Boolean(Some(false)),
            [1] => ScalarValue::Boolean(Some(true)),
            _ => return Err(OrderError::InvalidEncoding),
        },
        DataType::Int8 => ScalarValue::Int8(Some(i8::from_be_bytes(unsigned(encoded)?))),
        DataType::Int16 => ScalarValue::Int16(Some(i16::from_be_bytes(unsigned(encoded)?))),
        DataType::Int32 => ScalarValue::Int32(Some(i32::from_be_bytes(unsigned(encoded)?))),
        DataType::Int64 => ScalarValue::Int64(Some(i64::from_be_bytes(unsigned(encoded)?))),
        DataType::UInt8 => ScalarValue::UInt8(Some(u8::from_be_bytes(fixed(encoded)?))),
        DataType::UInt16 => ScalarValue::UInt16(Some(u16::from_be_bytes(fixed(encoded)?))),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::from_be_bytes(fixed(encoded)?))),
        DataType::UInt64 => ScalarValue::UInt64(Some(u64::from_be_bytes(fixed(encoded)?))),
        DataType::Utf8 => ScalarValue::Utf8(Some(
            std::str::from_utf8(encoded)
                .map_err(|_| OrderError::InvalidEncoding)?
                .to_owned(),
        )),
        DataType::Binary => ScalarValue::Binary(Some(encoded.to_vec())),
        DataType::Date32 => ScalarValue::Date32(Some(i32::from_be_bytes(unsigned(encoded)?))),
        DataType::Timestamp(unit, timezone) => {
            let value = Some(i64::from_be_bytes(unsigned(encoded)?));
            let timezone = timezone.as_ref().map(Arc::clone);
            match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(value, timezone),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, timezone),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, timezone),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, timezone),
            }
        }
        DataType::Decimal128(precision, scale) => ScalarValue::Decimal128(
            Some(i128::from_be_bytes(unsigned(encoded)?)),
            *precision,
            *scale,
        ),
        _ => return Err(OrderError::InvalidValue),
    };
    Ok(value)
}

pub(crate) fn indexable(data_type: &DataType) -> bool {
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

fn signed<const N: usize>(mut bytes: [u8; N], output: &mut Vec<u8>) {
    bytes[0] ^= 0x80;
    output.extend_from_slice(&bytes);
}

fn unsigned<const N: usize>(encoded: &[u8]) -> Result<[u8; N], OrderError> {
    let mut bytes = fixed(encoded)?;
    bytes[0] ^= 0x80;
    Ok(bytes)
}

fn fixed<const N: usize>(encoded: &[u8]) -> Result<[u8; N], OrderError> {
    encoded.try_into().map_err(|_| OrderError::InvalidEncoding)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, TimeUnit};
    use datafusion_common::ScalarValue;

    use super::{order_key, ordered_value};

    fn assert_order_and_round_trip(field: &Field, values: &[ScalarValue]) -> Vec<Vec<u8>> {
        let encoded = values
            .iter()
            .map(|value| order_key(field, value).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (encoded, expected) in encoded.iter().zip(values) {
            assert_eq!(ordered_value(field, encoded).unwrap(), *expected);
        }
        encoded
    }

    #[test]
    fn signed_integer_keys_preserve_negative_order() {
        let field = Field::new("value", DataType::Int64, false);
        let values =
            [i64::MIN, -2, -1, 0, 1, 2, i64::MAX].map(|value| ScalarValue::Int64(Some(value)));

        let encoded = assert_order_and_round_trip(&field, &values);
        assert_eq!(encoded[1], [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
        assert_eq!(encoded[3], [0x80, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn variable_length_keys_preserve_byte_order() {
        let string = Field::new("value", DataType::Utf8, false);
        let strings = ["", "\0", "a", "a\0", "aa", "b"]
            .map(|value| ScalarValue::Utf8(Some(value.to_owned())));
        let encoded = assert_order_and_round_trip(&string, &strings);
        assert_eq!(encoded[3], b"a\0");

        let binary = Field::new("value", DataType::Binary, true);
        let binaries = [vec![], vec![0], vec![0, 0], vec![0, 1], vec![1]]
            .map(|value| ScalarValue::Binary(Some(value)));
        let encoded = assert_order_and_round_trip(&binary, &binaries);
        assert_eq!(encoded[3], [0, 1]);
        assert_eq!(
            order_key(&binary, &ScalarValue::Binary(None)).unwrap(),
            None
        );
    }

    #[test]
    fn timestamp_keys_preserve_signed_order_and_timezone() {
        let timezone: Arc<str> = "UTC".into();
        let field = Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::clone(&timezone))),
            false,
        );
        let values = [-2, -1, 0, 1, 2].map(|value| {
            ScalarValue::TimestampNanosecond(Some(value), Some(Arc::clone(&timezone)))
        });

        let encoded = assert_order_and_round_trip(&field, &values);
        assert_eq!(encoded[0], [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
    }

    #[test]
    fn decimal_keys_preserve_signed_order_and_metadata() {
        let field = Field::new("value", DataType::Decimal128(20, 4), false);
        let values =
            [-10_000, -1, 0, 1, 10_000].map(|value| ScalarValue::Decimal128(Some(value), 20, 4));

        let encoded = assert_order_and_round_trip(&field, &values);
        assert_eq!(
            encoded[1],
            [
                0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff,
            ]
        );
        assert_eq!(
            encoded[2],
            [0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
