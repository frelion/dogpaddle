use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use datafusion_common::ScalarValue;

use super::AggregateError;

pub(super) fn order_key(
    field: &Field,
    value: &ScalarValue,
) -> Result<Option<Vec<u8>>, AggregateError> {
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
        _ => return Err(AggregateError::InvalidState),
    }
    Ok(Some(encoded))
}

pub(super) fn ordered_value(field: &Field, encoded: &[u8]) -> Result<ScalarValue, AggregateError> {
    let value = match field.data_type() {
        DataType::Boolean => match encoded {
            [0] => ScalarValue::Boolean(Some(false)),
            [1] => ScalarValue::Boolean(Some(true)),
            _ => return Err(AggregateError::InvalidState),
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
                .map_err(|_| AggregateError::InvalidState)?
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
        _ => return Err(AggregateError::InvalidState),
    };
    Ok(value)
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

fn signed<const N: usize>(mut bytes: [u8; N], output: &mut Vec<u8>) {
    bytes[0] ^= 0x80;
    output.extend_from_slice(&bytes);
}

fn unsigned<const N: usize>(encoded: &[u8]) -> Result<[u8; N], AggregateError> {
    let mut bytes = fixed(encoded)?;
    bytes[0] ^= 0x80;
    Ok(bytes)
}

fn fixed<const N: usize>(encoded: &[u8]) -> Result<[u8; N], AggregateError> {
    encoded.try_into().map_err(|_| AggregateError::InvalidState)
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field};
    use datafusion_common::ScalarValue;

    use super::{order_key, ordered_value};

    #[test]
    fn order_keys_sort_and_round_trip() {
        let integer = Field::new("value", DataType::Int64, false);
        let integers = [-2, -1, 0, 1, 2]
            .into_iter()
            .map(|value| {
                order_key(&integer, &ScalarValue::Int64(Some(value)))
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(integers.windows(2).all(|pair| pair[0] < pair[1]));
        for (encoded, expected) in integers.iter().zip([-2, -1, 0, 1, 2]) {
            assert_eq!(
                ordered_value(&integer, encoded).unwrap(),
                ScalarValue::Int64(Some(expected))
            );
        }

        let string = Field::new("value", DataType::Utf8, false);
        let values = ["", "\0", "a", "a\0", "aa"];
        let encoded = values
            .iter()
            .map(|value| {
                order_key(&string, &ScalarValue::Utf8(Some((*value).to_owned())))
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (encoded, expected) in encoded.iter().zip(values) {
            assert_eq!(
                ordered_value(&string, encoded).unwrap(),
                ScalarValue::Utf8(Some(expected.to_owned()))
            );
        }

        let binary = Field::new("value", DataType::Binary, true);
        let values = [Vec::new(), vec![0], vec![0, 0], vec![0, 1], vec![1]];
        let encoded = values
            .iter()
            .map(|value| {
                order_key(&binary, &ScalarValue::Binary(Some(value.clone())))
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (encoded, expected) in encoded.iter().zip(values) {
            assert_eq!(
                ordered_value(&binary, encoded).unwrap(),
                ScalarValue::Binary(Some(expected))
            );
        }
        assert_eq!(
            order_key(&binary, &ScalarValue::Binary(None)).unwrap(),
            None
        );
    }
}
