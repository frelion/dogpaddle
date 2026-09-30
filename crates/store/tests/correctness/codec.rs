use std::{borrow::Cow, fmt::Debug};

use dogpaddle_store::{StoreKey, StoreValue};

fn assert_value_bytes<T>(value: &T, expected: &[u8])
where
    T: StoreValue + Debug + Eq,
{
    assert_eq!(value.encode_value().unwrap().as_ref(), expected);
    assert_eq!(&T::decode_value(Cow::Borrowed(expected)).unwrap(), value);
    assert_eq!(
        &T::decode_value(Cow::Owned(expected.to_vec())).unwrap(),
        value
    );
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
        assert_eq!(
            T::decode_key(Cow::Owned(encoded.as_ref().to_vec())).unwrap(),
            *value
        );
    }
    for pair in values.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(pair[0].encode_key().unwrap().as_ref() < pair[1].encode_key().unwrap().as_ref());
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
fn malformed_builtin_encodings_are_rejected() {
    assert!(bool::decode_value(Cow::Borrowed(&[2])).is_err());
    assert!(<()>::decode_value(Cow::Borrowed(&[0])).is_err());
    assert!(String::decode_value(Cow::Borrowed(&[0xff])).is_err());
    assert!(String::decode_key(Cow::Owned(vec![0xff])).is_err());
    assert!(u32::decode_value(Cow::Borrowed(&[0; 3])).is_err());
    assert!(u64::decode_value(Cow::Borrowed(&[0; 7])).is_err());
    assert!(i64::decode_value(Cow::Borrowed(&[0; 9])).is_err());
    assert!(u32::decode_key(Cow::Borrowed(&[0; 3])).is_err());
    assert!(u64::decode_key(Cow::Borrowed(&[0; 7])).is_err());
    assert!(i64::decode_key(Cow::Borrowed(&[0; 9])).is_err());
}

#[test]
fn positive_weight_has_one_fixed_width_codec_and_rejects_zero() {
    use std::num::NonZeroU64;
    for weight in [1, 7, u64::MAX] {
        let value = NonZeroU64::new(weight).unwrap();
        let bytes = value.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(bytes, weight.to_be_bytes());
        assert_eq!(
            NonZeroU64::decode_value(Cow::Borrowed(&bytes)).unwrap(),
            value
        );
        assert_eq!(NonZeroU64::decode_value(Cow::Owned(bytes)).unwrap(), value);
    }
    for length in 0..8 {
        assert!(NonZeroU64::decode_value(Cow::Owned(vec![1; length])).is_err());
    }
    assert!(NonZeroU64::decode_value(Cow::Borrowed(&[0; 8])).is_err());
    assert!(NonZeroU64::decode_value(Cow::Borrowed(&[1; 9])).is_err());
}

#[test]
fn partition_key_framing_is_canonical_and_preserves_tuple_order() {
    use dogpaddle_store::PartitionKey;
    let key = PartitionKey(vec![0, 1, 0], vec![9]);
    let bytes = key.encode_key().unwrap().as_ref().to_vec();
    assert_eq!(bytes, [0, 255, 1, 0, 255, 0, 0, 9]);
    assert_eq!(
        PartitionKey::<Vec<u8>, Vec<u8>>::decode_key(Cow::Borrowed(&bytes)).unwrap(),
        key
    );
    let components = [vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![255]];
    let mut keys = Vec::new();
    for partition in &components {
        for local in &components {
            keys.push(PartitionKey(partition.clone(), local.clone()));
        }
    }
    let encoded = keys
        .iter()
        .map(|key| key.encode_key().unwrap().as_ref().to_vec())
        .collect::<Vec<_>>();
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    for bytes in [vec![], vec![0], vec![0, 1], vec![1, 0, 255]] {
        assert!(PartitionKey::<Vec<u8>, Vec<u8>>::decode_key(Cow::Owned(bytes)).is_err());
    }
}
