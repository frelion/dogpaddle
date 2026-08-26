use std::{borrow::Cow, num::NonZeroI64};

use dogpaddle_store::StoreValue;

use super::{CanonicalF64, Change, ChangeError, MAX_NESTING_DEPTH, Record, RecordError, Value};

fn record(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Record {
    Record::try_new(fields).unwrap()
}

fn encode(change: &Change) -> Vec<u8> {
    change.encode_value().unwrap().as_ref().to_vec()
}

fn assert_value_golden(value: Value, encoded_value: &[u8]) {
    let change = Change::insertion(record([("x", value)]));
    let mut expected = vec![
        0x00, 0x01, // version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // diff
        0x00, 0x00, 0x00, 0x01, // field count
        0x00, 0x00, 0x00, 0x01, b'x', // name
    ];
    expected.extend_from_slice(&u32::try_from(encoded_value.len()).unwrap().to_be_bytes());
    expected.extend_from_slice(encoded_value);

    assert_eq!(encode(&change), expected);
    assert_eq!(
        Change::decode_value(Cow::Borrowed(&expected)).unwrap(),
        change
    );
}

fn nested_array_change(depth: usize) -> Vec<u8> {
    let mut value = vec![0x00];
    for _ in 0..depth {
        let mut nested = vec![0x30, 0x00, 0x00, 0x00, 0x01];
        nested.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
        nested.extend_from_slice(&value);
        value = nested;
    }

    let mut change = vec![
        0x00, 0x01, // version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // diff
        0x00, 0x00, 0x00, 0x01, // field count
        0x00, 0x00, 0x00, 0x01, b'x', // name
    ];
    change.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
    change.extend_from_slice(&value);
    change
}

#[test]
fn record_construction_canonicalizes_field_order_and_rejects_duplicates() {
    let first = Record::try_new([
        ("z", Value::Null),
        ("", Value::Bool(false)),
        ("a", Value::U64(7)),
    ])
    .unwrap();
    let second = Record::try_new([
        ("a", Value::U64(7)),
        ("z", Value::Null),
        ("", Value::Bool(false)),
    ])
    .unwrap();

    assert_eq!(first, second);
    assert_eq!(
        first.fields().map(|(name, _)| name).collect::<Vec<_>>(),
        ["", "a", "z"]
    );
    assert_eq!(first.get("a"), Some(&Value::U64(7)));
    assert!(first.contains_field("z"));
    assert!(!first.contains_field("missing"));
    assert_eq!(
        encode(&Change::insertion(first)),
        encode(&Change::insertion(second))
    );

    assert_eq!(
        Record::try_new([("same", Value::Null), ("same", Value::Bool(true))]),
        Err(RecordError::DuplicateField {
            name: "same".to_owned()
        })
    );
}

#[test]
fn canonical_f64_has_one_zero_and_one_nan_representation() {
    assert_eq!(CanonicalF64::new(-0.0), CanonicalF64::new(0.0));
    assert_eq!(CanonicalF64::new(-0.0).to_bits(), 0);
    assert_eq!(
        CanonicalF64::from_bits(0x7ff0_0000_0000_0001),
        CanonicalF64::from_bits(0xffff_ffff_ffff_ffff)
    );
    assert_eq!(CanonicalF64::new(f64::NAN).to_bits(), 0x7ff8_0000_0000_0000);
    assert_eq!(
        CanonicalF64::new(f64::NEG_INFINITY).to_bits(),
        f64::NEG_INFINITY.to_bits()
    );
}

#[test]
fn change_rejects_zero_but_accepts_the_full_non_zero_i64_domain() {
    let empty = Record::default();
    assert_eq!(
        Change::try_new(0, empty.clone()),
        Err(ChangeError::ZeroDiff)
    );
    assert_eq!(Change::insertion(empty.clone()).diff().get(), 1);
    assert_eq!(Change::retraction(empty.clone()).diff().get(), -1);
    assert_eq!(
        Change::try_new(i64::MIN, empty).unwrap().diff().get(),
        i64::MIN
    );
    assert_eq!(
        encode(&Change::retraction(Record::default())),
        [
            0x00, 0x01, // version
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // diff -1
            0x00, 0x00, 0x00, 0x00, // empty record
        ]
    );
}

#[test]
fn nesting_limit_is_enforced_when_values_are_composed_into_a_record() {
    let mut accepted = Value::Null;
    for _ in 0..MAX_NESTING_DEPTH {
        accepted = Value::Array(vec![accepted]);
    }
    assert!(Record::try_new([("nested", accepted.clone())]).is_ok());

    let rejected = Value::Array(vec![accepted]);
    assert_eq!(
        Record::try_new([("nested", rejected)]),
        Err(RecordError::NestingTooDeep {
            max_depth: MAX_NESTING_DEPTH
        })
    );
}

#[test]
fn change_v1_has_stable_golden_bytes_and_accepts_owned_or_borrowed_input() {
    let change = Change::insertion(record([("value", Value::U64(42))]));
    let expected = [
        0x00, 0x01, // version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // diff
        0x00, 0x00, 0x00, 0x01, // field count
        0x00, 0x00, 0x00, 0x05, b'v', b'a', b'l', b'u', b'e', // name
        0x00, 0x00, 0x00, 0x09, // value length
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a,
    ];

    assert_eq!(encode(&change), expected);
    assert_eq!(
        Change::decode_value(Cow::Borrowed(&expected)).unwrap(),
        change
    );
    assert_eq!(
        Change::decode_value(Cow::Owned(expected.to_vec())).unwrap(),
        change
    );
    assert_eq!(
        Change::project_diff(&expected).unwrap(),
        NonZeroI64::new(1).unwrap()
    );
}

#[test]
fn every_value_tag_has_stable_golden_bytes() {
    assert_value_golden(Value::Null, &[0x00]);
    assert_value_golden(Value::Bool(false), &[0x01]);
    assert_value_golden(Value::Bool(true), &[0x02]);
    assert_value_golden(
        Value::I64(-2),
        &[0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe],
    );
    assert_value_golden(
        Value::U64(42),
        &[0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a],
    );
    assert_value_golden(
        Value::F64(CanonicalF64::new(1.5)),
        &[0x12, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    );
    assert_value_golden(Value::String("A".to_owned()), &[0x20, b'A']);
    assert_value_golden(Value::Bytes(vec![0, 255]), &[0x21, 0x00, 0xff]);
    assert_value_golden(
        Value::Array(vec![Value::Null, Value::Bool(true)]),
        &[
            0x30, 0x00, 0x00, 0x00, 0x02, // element count
            0x00, 0x00, 0x00, 0x01, 0x00, // null
            0x00, 0x00, 0x00, 0x01, 0x02, // true
        ],
    );
    assert_value_golden(
        Value::Object(record([("y", Value::Null)])),
        &[
            0x31, 0x00, 0x00, 0x00, 0x01, // field count
            0x00, 0x00, 0x00, 0x01, b'y', // name
            0x00, 0x00, 0x00, 0x01, 0x00, // null
        ],
    );
}

#[test]
fn a_change_containing_every_value_kind_round_trips_canonically() {
    let change = Change::retraction(record([
        ("array", Value::Array(vec![Value::Null, Value::Bool(true)])),
        ("bool", Value::Bool(false)),
        ("bytes", Value::Bytes(vec![0, 1, 255])),
        ("f64", Value::F64(CanonicalF64::new(1.5))),
        ("i64", Value::I64(-7)),
        ("object", Value::Object(record([("inner", Value::Null)]))),
        ("string", Value::String("犬".to_owned())),
        ("u64", Value::U64(u64::MAX)),
    ]));
    let encoded = encode(&change);

    assert_eq!(
        Change::decode_value(Cow::Borrowed(&encoded)).unwrap(),
        change
    );
    assert_eq!(
        encode(&Change::decode_value(Cow::Borrowed(&encoded)).unwrap()),
        encoded
    );
}

#[test]
fn decoder_rejects_all_truncations_and_non_canonical_envelopes() {
    let valid = encode(&Change::insertion(record([("value", Value::U64(42))])));
    for end in 0..valid.len() {
        assert!(
            Change::decode_value(Cow::Borrowed(&valid[..end])).is_err(),
            "accepted truncation at byte {end}"
        );
    }

    let mut unsupported = valid.clone();
    unsupported[..2].copy_from_slice(&2_u16.to_be_bytes());
    assert!(Change::decode_value(Cow::Borrowed(&unsupported)).is_err());
    assert!(Change::project_diff(&unsupported).is_err());

    let mut zero = valid.clone();
    zero[2..10].fill(0);
    assert!(Change::decode_value(Cow::Borrowed(&zero)).is_err());
    assert!(Change::project_diff(&zero).is_err());

    let mut trailing = valid;
    trailing.push(0);
    assert!(Change::decode_value(Cow::Borrowed(&trailing)).is_err());
}

#[test]
fn decoder_rejects_non_canonical_records_values_and_floats() {
    let mut unordered = encode(&Change::insertion(record([
        ("a", Value::Null),
        ("b", Value::Null),
    ])));
    let first_name = 18;
    let second_name = 28;
    unordered.swap(first_name, second_name);
    assert!(Change::decode_value(Cow::Borrowed(&unordered)).is_err());

    let mut duplicate = encode(&Change::insertion(record([
        ("a", Value::Null),
        ("b", Value::Null),
    ])));
    duplicate[second_name] = b'a';
    assert!(Change::decode_value(Cow::Borrowed(&duplicate)).is_err());

    let mut invalid_name = encode(&Change::insertion(record([("x", Value::Null)])));
    invalid_name[18] = 0xff;
    assert!(Change::decode_value(Cow::Borrowed(&invalid_name)).is_err());

    let mut invalid_string = encode(&Change::insertion(record([(
        "x",
        Value::String("a".to_owned()),
    )])));
    *invalid_string.last_mut().unwrap() = 0xff;
    assert!(Change::decode_value(Cow::Borrowed(&invalid_string)).is_err());

    let mut unknown_tag = encode(&Change::insertion(record([("x", Value::Null)])));
    *unknown_tag.last_mut().unwrap() = 0x80;
    assert!(Change::decode_value(Cow::Borrowed(&unknown_tag)).is_err());

    let mut zero_value_len = encode(&Change::insertion(record([("x", Value::Null)])));
    zero_value_len[19..23].fill(0);
    assert!(Change::decode_value(Cow::Borrowed(&zero_value_len)).is_err());

    let mut scalar_trailing = encode(&Change::insertion(record([("x", Value::Null)])));
    scalar_trailing[19..23].copy_from_slice(&2_u32.to_be_bytes());
    scalar_trailing.push(0);
    assert!(Change::decode_value(Cow::Borrowed(&scalar_trailing)).is_err());

    let mut negative_zero = encode(&Change::insertion(record([(
        "x",
        Value::F64(CanonicalF64::new(0.0)),
    )])));
    let bits = negative_zero.len() - 8;
    negative_zero[bits..].copy_from_slice(&(-0.0_f64).to_bits().to_be_bytes());
    assert!(Change::decode_value(Cow::Borrowed(&negative_zero)).is_err());

    let mut non_canonical_nan = encode(&Change::insertion(record([(
        "x",
        Value::F64(CanonicalF64::new(f64::NAN)),
    )])));
    let bits = non_canonical_nan.len() - 8;
    non_canonical_nan[bits..].copy_from_slice(&0x7ff0_0000_0000_0001_u64.to_be_bytes());
    assert!(Change::decode_value(Cow::Borrowed(&non_canonical_nan)).is_err());
}

#[test]
fn decoder_enforces_the_same_64_container_depth_as_record_construction() {
    let accepted = nested_array_change(MAX_NESTING_DEPTH);
    assert!(Change::decode_value(Cow::Borrowed(&accepted)).is_ok());

    let rejected = nested_array_change(MAX_NESTING_DEPTH + 1);
    assert!(Change::decode_value(Cow::Borrowed(&rejected)).is_err());
    assert_eq!(Change::project_diff(&rejected).unwrap().get(), 1);
}
