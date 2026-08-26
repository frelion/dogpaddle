use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray, StructArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{
    Change, ChangeProjection, MAX_NESTING_DEPTH, decode_change, decode_change_projected,
    encode_change,
};

use super::support::{assert_change_eq, fixture_hex, hex, representative_change};

const KIND_KEY: &str = "dogpaddle.kind";
const VERSION_KEY: &str = "dogpaddle.change.version";

#[test]
fn complete_round_trip_preserves_order_and_is_a_standard_marked_arrow_stream() {
    let change = representative_change();
    let encoded = encode_change(&change).unwrap();
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);

    let mut reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "$dogpaddle.diff");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(
        schema.metadata().get(KIND_KEY).map(String::as_str),
        Some("change")
    );
    assert_eq!(
        schema.metadata().get(VERSION_KEY).map(String::as_str),
        Some("1")
    );
    let physical = reader.next().unwrap().unwrap();
    assert_eq!(physical.num_columns(), change.records().num_columns() + 1);
    assert_eq!(
        physical
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        change.diffs()
    );
    assert!(reader.next().is_none());
}

#[test]
fn zero_column_change_stream_has_stable_golden_bytes() {
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let records =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1])).unwrap();
    let expected = fixture_hex(include_str!("../fixtures/v1/zero_columns.hex"));

    let encoded = encode_change(&change).unwrap();
    assert_eq!(hex(&encoded), expected);
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
}

#[test]
fn sliced_representative_change_stream_has_stable_golden_bytes() {
    let source = representative_change();
    let change = source.try_slice(1, 2).unwrap();
    let expected = fixture_hex(include_str!(
        "../fixtures/v1/sliced_representative_change.hex"
    ));

    let encoded = encode_change(&change).unwrap();
    assert_eq!(hex(&encoded), expected);
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
}

#[test]
fn metadata_insertion_order_does_not_change_stream_bytes() {
    let encode = |metadata| {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::UInt64, false)],
            metadata,
        ));
        let records =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap();
        encode_change(&Change::try_new(records, Int64Array::from(vec![1])).unwrap()).unwrap()
    };
    let entries = [
        ("alpha", "1"),
        ("bravo", "2"),
        ("charlie", "3"),
        ("delta", "4"),
        ("echo", "5"),
        ("foxtrot", "6"),
        ("golf", "7"),
        ("hotel", "8"),
    ];
    let first = entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect::<HashMap<_, _>>();
    let first_order = first.keys().cloned().collect::<Vec<_>>();
    let hasher = first.hasher().clone();
    let second = [0, 1, 4, 16, 64, 256]
        .into_iter()
        .find_map(|capacity| {
            let mut candidate = HashMap::with_capacity_and_hasher(capacity, hasher.clone());
            candidate.extend(
                entries
                    .iter()
                    .rev()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
            );
            (candidate.keys().cloned().collect::<Vec<_>>() != first_order).then_some(candidate)
        })
        .expect("test setup could not construct two distinct HashMap iteration orders");
    let second_order = second.keys().cloned().collect::<Vec<_>>();

    assert_eq!(first, second);
    assert_ne!(first_order, second_order);
    assert_eq!(encode(first), encode(second));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the complete scalar/null/edge-value matrix stays together for auditability"
)]
fn every_supported_scalar_round_trips_real_values_and_float_edges() {
    let f32_nan = f32::from_bits(0x7fc0_1234);
    let f64_nan = f64::from_bits(0x7ff8_0000_0000_1234);
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "bool",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ])),
        ),
        (
            "i8",
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(-1),
                Some(0),
                Some(i8::MAX),
                None,
                Some(42),
                Some(-42),
            ])),
        ),
        (
            "i16",
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                Some(-1),
                Some(0),
                Some(i16::MAX),
                None,
                Some(42),
                Some(-42),
            ])),
        ),
        (
            "i32",
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(-1),
                Some(0),
                Some(i32::MAX),
                None,
                Some(42),
                Some(-42),
            ])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(i64::MAX),
                None,
                Some(42),
                Some(-42),
            ])),
        ),
        (
            "u8",
            Arc::new(UInt8Array::from(vec![
                Some(0),
                Some(1),
                Some(u8::MAX - 1),
                Some(u8::MAX),
                None,
                Some(42),
                Some(7),
            ])),
        ),
        (
            "u16",
            Arc::new(UInt16Array::from(vec![
                Some(0),
                Some(1),
                Some(u16::MAX - 1),
                Some(u16::MAX),
                None,
                Some(42),
                Some(7),
            ])),
        ),
        (
            "u32",
            Arc::new(UInt32Array::from(vec![
                Some(0),
                Some(1),
                Some(u32::MAX - 1),
                Some(u32::MAX),
                None,
                Some(42),
                Some(7),
            ])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(1),
                Some(u64::MAX - 1),
                Some(u64::MAX),
                None,
                Some(42),
                Some(7),
            ])),
        ),
        (
            "f32",
            Arc::new(Float32Array::from(vec![
                Some(f32::NEG_INFINITY),
                Some(-0.0),
                Some(0.0),
                Some(f32::INFINITY),
                Some(f32_nan),
                None,
                Some(-123.5),
            ])),
        ),
        (
            "f64",
            Arc::new(Float64Array::from(vec![
                Some(f64::NEG_INFINITY),
                Some(-0.0),
                Some(0.0),
                Some(f64::INFINITY),
                Some(f64_nan),
                None,
                Some(-123.5),
            ])),
        ),
        (
            "utf8",
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("é"),
                Some("犬"),
                Some("last"),
                None,
                Some("\0"),
                Some("🦀"),
            ])),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"\x00"[..]),
                Some(&b"\xff\x00"[..]),
                Some(&b"bytes"[..]),
                None,
                Some(&b"\x00\xff"[..]),
                Some(&b"last"[..]),
            ])),
        ),
        ("null", new_null_array(&DataType::Null, 7)),
    ];
    let fields = columns
        .iter()
        .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
        .collect::<Vec<_>>();
    let records = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, column)| column).collect(),
    )
    .unwrap();
    let change = Change::try_new(
        records,
        Int64Array::from(vec![1, -1, 2, -2, i64::MIN, i64::MAX, 1]),
    )
    .unwrap();

    let decoded = decode_change(&encode_change(&change).unwrap()).unwrap();
    assert_change_eq(&decoded, &change);
    let f32_values = decoded
        .records()
        .column(9)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(f32_values.value(1).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(f32_values.value(2).to_bits(), 0.0_f32.to_bits());
    assert_eq!(f32_values.value(4).to_bits(), f32_nan.to_bits());
    assert!(f32_values.is_null(5));
    let f64_values = decoded
        .records()
        .column(10)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(f64_values.value(1).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(f64_values.value(2).to_bits(), 0.0_f64.to_bits());
    assert_eq!(f64_values.value(4).to_bits(), f64_nan.to_bits());
    assert!(f64_values.is_null(5));
}

#[test]
fn validity_and_boolean_bitmaps_round_trip_across_byte_and_word_boundaries() {
    for rows in [7, 8, 9, 63, 64, 65] {
        let booleans = (0..rows)
            .map(|index| (index % 5 != 0).then_some(index % 2 == 0))
            .collect::<Vec<_>>();
        let integers = (0..rows)
            .map(|index| (index % 7 != 0).then_some(i64::from(index)))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("boolean", DataType::Boolean, true),
            Field::new("integer", DataType::Int64, true),
        ]));
        let records = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(booleans)),
                Arc::new(Int64Array::from(integers)),
            ],
        )
        .unwrap();
        let diffs = Int64Array::from(
            (0..rows)
                .map(|index| if index % 2 == 0 { 1 } else { -1 })
                .collect::<Vec<_>>(),
        );
        let change = Change::try_new(records, diffs).unwrap();
        let encoded = encode_change(&change).unwrap();
        assert_change_eq(&decode_change(&encoded).unwrap(), &change);
        for selection in [vec![0], vec![1]] {
            let projection = ChangeProjection::try_new(change.schema(), selection).unwrap();
            assert_change_eq(
                &decode_change_projected(&encoded, &projection).unwrap(),
                &change.try_project(&projection).unwrap(),
            );
        }
    }
}

#[test]
fn maximum_list_struct_and_mixed_nesting_round_trip() {
    let mut list = DataType::Int64;
    let mut structure = DataType::Int64;
    let mut mixed = DataType::Int64;
    for _ in 0..MAX_NESTING_DEPTH {
        list = DataType::List(Arc::new(Field::new("item", list, true)));
        structure = DataType::Struct(vec![Field::new("member", structure, true)].into());
        mixed = match mixed {
            DataType::List(_) => DataType::Struct(vec![Field::new("member", mixed, true)].into()),
            _ => DataType::List(Arc::new(Field::new("item", mixed, true))),
        };
    }

    for data_type in [list, structure, mixed] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", data_type.clone(), true),
            Field::new("tail", DataType::UInt64, false),
        ]));
        let records = RecordBatch::try_new(
            schema,
            vec![
                new_null_array(&data_type, 1),
                Arc::new(UInt64Array::from(vec![7])),
            ],
        )
        .unwrap();
        let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
        let encoded = encode_change(&change).unwrap();
        assert_change_eq(&decode_change(&encoded).unwrap(), &change);
        let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
        assert_change_eq(
            &decode_change_projected(&encoded, &projection).unwrap(),
            &change.try_project(&projection).unwrap(),
        );
    }
}

#[test]
fn zero_logical_columns_keep_their_non_zero_row_count() {
    let options = RecordBatchOptions::new().with_row_count(Some(2));
    let records =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1, 1])).unwrap();
    let encoded = encode_change(&change).unwrap();
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);

    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &projection).unwrap(),
        &change,
    );
}

#[test]
fn projected_decode_matches_in_memory_projection_for_every_top_level_layout() {
    let change = representative_change();
    let encoded = encode_change(&change).unwrap();
    let field_count = change.schema().fields().len();
    let mut selections = vec![vec![], vec![0, 2, 4, 6]];
    selections.extend((0..field_count).map(|index| vec![index]));
    selections.push((0..field_count).collect());

    for selection in selections {
        let projection = ChangeProjection::try_new(change.schema(), selection).unwrap();
        let expected = change.try_project(&projection).unwrap();
        let actual = decode_change_projected(&encoded, &projection).unwrap();
        assert_change_eq(&actual, &expected);
        assert_eq!(actual.schema(), projection.output_schema());
    }
}

#[test]
fn projected_decode_is_owned_and_outlives_the_encoded_entry() {
    let projection = ChangeProjection::try_new(representative_change().schema(), [0, 2]).unwrap();
    let decoded = {
        let encoded = encode_change(&representative_change()).unwrap();
        decode_change_projected(&encoded, &projection).unwrap()
    };

    assert_eq!(decoded.num_rows(), 3);
    assert_eq!(decoded.diffs().values(), &[1, -1, 2]);
    assert_eq!(
        decoded
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(2),
        "next"
    );
}

#[test]
fn schema_field_and_nested_metadata_survive_full_and_projected_decode() {
    let child = Arc::new(
        Field::new("value", DataType::Int64, true)
            .with_metadata(HashMap::from([("unit".to_owned(), "count".to_owned())])),
    );
    let object = StructArray::from(vec![(
        Arc::clone(&child),
        Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("object", object.data_type().clone(), true).with_metadata(HashMap::from([
                ("semantic".to_owned(), "object".to_owned()),
            ])),
        ],
        HashMap::from([("source".to_owned(), "metadata-test".to_owned())]),
    ));
    let records = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(object)]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let encoded = encode_change(&change).unwrap();

    let decoded = decode_change(&encoded).unwrap();
    assert_eq!(decoded.schema(), schema);
    let projection = ChangeProjection::try_new(schema, [0]).unwrap();
    let decoded = decode_change_projected(&encoded, &projection).unwrap();
    assert_eq!(decoded.schema(), projection.output_schema());
}

#[test]
fn nullable_struct_parent_masks_nulls_in_a_non_nullable_child() {
    let child = Arc::new(Field::new("value", DataType::Int64, false));
    let object = StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(vec![None, Some(2)]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "object",
        object.data_type().clone(),
        true,
    )]));
    let records = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(object)]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, 1])).unwrap();
    let encoded = encode_change(&change).unwrap();

    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
    let identity = ChangeProjection::try_new(Arc::clone(&schema), [0]).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &identity).unwrap(),
        &change,
    );
    let empty = ChangeProjection::try_new(schema, []).unwrap();
    let empty = decode_change_projected(&encoded, &empty).unwrap();
    assert_eq!(empty.num_rows(), 2);
    assert_eq!(empty.diffs().values(), &[1, 1]);
}
