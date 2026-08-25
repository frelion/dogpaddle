use std::borrow::Cow;

use thiserror::Error;

/// A stable codec failure.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("{message}")]
pub struct CodecError {
    message: String,
}

impl CodecError {
    /// Creates a codec error without exposing a concrete serialization library.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// A value that can be persisted by the store.
///
/// Decoding an encoded value must reconstruct the same logical value across
/// process restarts.
pub trait StoreValue: Sized {
    /// Encodes this value into its durable representation.
    ///
    /// # Errors
    ///
    /// Returns an error when this value cannot be encoded canonically.
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError>;

    /// Decodes borrowed or owned bytes produced by [`StoreValue::encode_value`].
    ///
    /// Both [`Cow::Borrowed`] and [`Cow::Owned`] represent the same logical
    /// encoding. Implementations must not require one variant: inspect bytes
    /// through [`AsRef::as_ref`], or call [`Cow::into_owned`] when the decoded
    /// value can reuse an owned buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `bytes` is not a valid value for this codec.
    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError>;
}

/// A key whose encoding preserves its [`Ord`] ordering byte-for-byte.
///
/// Implementations must be canonical, injective, and round-trip exactly. For
/// any `a` and `b`, `a.cmp(b)` must equal
/// `a.encode_key()?.as_ref().cmp(b.encode_key()?.as_ref())`.
pub trait StoreKey: Sized + Ord {
    /// Encodes this key in lexicographically order-preserving form.
    ///
    /// # Errors
    ///
    /// Returns an error when this key cannot be encoded canonically.
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError>;

    /// Decodes borrowed or owned bytes produced by [`StoreKey::encode_key`].
    ///
    /// Both [`Cow::Borrowed`] and [`Cow::Owned`] represent the same logical
    /// encoding. Implementations must not require one variant: inspect bytes
    /// through [`AsRef::as_ref`], or call [`Cow::into_owned`] when the decoded
    /// key can reuse an owned buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `bytes` is not a valid key for this codec.
    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError>;
}

impl StoreValue for Vec<u8> {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.as_slice())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Ok(bytes.into_owned())
    }
}

impl StoreKey for Vec<u8> {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.as_slice())
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Ok(bytes.into_owned())
    }
}

impl StoreValue for String {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.as_bytes())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        String::from_utf8(bytes.into_owned())
            .map_err(|error| CodecError::new(format!("invalid UTF-8 value: {error}")))
    }
}

impl StoreKey for String {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.as_bytes())
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        String::from_utf8(bytes.into_owned())
            .map_err(|error| CodecError::new(format!("invalid UTF-8 key: {error}")))
    }
}

macro_rules! unsigned_codec {
    ($ty:ty, $name:literal) => {
        impl StoreValue for $ty {
            fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
                Ok(self.to_be_bytes())
            }

            fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
                let array: [u8; size_of::<$ty>()] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| CodecError::new(concat!("invalid ", $name, " value length")))?;
                Ok(<$ty>::from_be_bytes(array))
            }
        }

        impl StoreKey for $ty {
            fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
                Ok(self.to_be_bytes())
            }

            fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
                let array: [u8; size_of::<$ty>()] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| CodecError::new(concat!("invalid ", $name, " key length")))?;
                Ok(<$ty>::from_be_bytes(array))
            }
        }
    };
}

unsigned_codec!(u32, "u32");
unsigned_codec!(u64, "u64");

impl StoreValue for i64 {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.to_be_bytes())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let array: [u8; size_of::<i64>()] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid i64 value length"))?;
        Ok(Self::from_be_bytes(array))
    }
}

impl StoreKey for i64 {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut bytes = self.to_be_bytes();
        bytes[0] ^= 0x80;
        Ok(bytes)
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut array: [u8; size_of::<Self>()] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid i64 key length"))?;
        array[0] ^= 0x80;
        Ok(Self::from_be_bytes(array))
    }
}

impl StoreValue for bool {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok([u8::from(*self)])
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        match bytes.as_ref() {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(CodecError::new("invalid bool encoding")),
        }
    }
}

impl StoreValue for () {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok([])
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new("invalid unit encoding"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{Cow, StoreKey, StoreValue};

    fn assert_value_bytes<T>(value: &T, expected: &[u8])
    where
        T: StoreValue + Debug + Eq,
    {
        assert_eq!(value.encode_value().unwrap().as_ref(), expected);
        assert_eq!(&T::decode_value(Cow::Borrowed(expected)).unwrap(), value);
    }

    fn assert_key_bytes<T>(value: &T, expected: &[u8])
    where
        T: StoreKey + Debug + Eq,
    {
        assert_eq!(value.encode_key().unwrap().as_ref(), expected);
        assert_eq!(&T::decode_key(Cow::Borrowed(expected)).unwrap(), value);
        assert_eq!(
            &T::decode_key(Cow::Owned(expected.to_vec())).unwrap(),
            value
        );
    }

    fn assert_key_codec<T>(values: &[T])
    where
        T: StoreKey + Debug + Eq,
    {
        for value in values {
            let encoded = value.encode_key().unwrap();
            assert_eq!(
                T::decode_key(Cow::Borrowed(encoded.as_ref())).unwrap(),
                *value
            );
        }
        for pair in values.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(
                pair[0].encode_key().unwrap().as_ref() < pair[1].encode_key().unwrap().as_ref()
            );
        }
    }

    #[test]
    fn integer_keys_round_trip_and_preserve_order() {
        assert_key_codec(&[0_u32, 1, u32::MAX - 1, u32::MAX]);
        assert_key_codec(&[0_u64, 1, u64::MAX - 1, u64::MAX]);
        assert_key_codec(&[i64::MIN, -2, -1, 0, 1, 2, i64::MAX]);
    }

    #[test]
    fn byte_and_string_keys_round_trip_and_preserve_order() {
        assert_key_codec(&[Vec::new(), vec![0], vec![0, 0], vec![0, 1], vec![0xff]]);
        assert_key_codec(&[
            String::new(),
            "a".to_owned(),
            "aa".to_owned(),
            "b".to_owned(),
            "犬".to_owned(),
        ]);
    }

    #[test]
    fn primitive_values_have_stable_encodings() {
        assert_value_bytes(&0x0102_0304_u32, &[1, 2, 3, 4]);
        assert_value_bytes(&0x0102_0304_0506_0708_u64, &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_value_bytes(&-2_i64, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
        assert_value_bytes(&false, &[0]);
        assert_value_bytes(&true, &[1]);
        assert_value_bytes(&(), &[]);
        assert_value_bytes(&"Shiba 柴犬".to_owned(), "Shiba 柴犬".as_bytes());
    }

    #[test]
    fn owning_codecs_accept_borrowed_and_owned_inputs() {
        let bytes = b"Shiba";
        assert_eq!(
            Vec::<u8>::decode_value(Cow::Borrowed(bytes)).unwrap(),
            bytes
        );
        assert_eq!(
            Vec::<u8>::decode_value(Cow::Owned(bytes.to_vec())).unwrap(),
            bytes
        );
        assert_eq!(String::decode_value(Cow::Borrowed(bytes)).unwrap(), "Shiba");
        assert_eq!(
            String::decode_value(Cow::Owned(bytes.to_vec())).unwrap(),
            "Shiba"
        );
    }

    #[test]
    fn primitive_keys_have_stable_ordered_encodings() {
        assert_key_bytes(&0x0102_0304_u32, &[1, 2, 3, 4]);
        assert_key_bytes(&0x0102_0304_0506_0708_u64, &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_key_bytes(&-2_i64, &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
    }

    #[test]
    fn malformed_values_are_rejected() {
        assert!(bool::decode_value(Cow::Borrowed(&[2])).is_err());
        assert!(<()>::decode_value(Cow::Borrowed(&[0])).is_err());
        assert!(String::decode_value(Cow::Borrowed(&[0xff])).is_err());
        assert!(u64::decode_value(Cow::Borrowed(&[0; 7])).is_err());
    }
}
