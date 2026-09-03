use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array, types::Date32Type,
};
use arrow_buffer::NullBuffer;
use arrow_ipc::{
    MetadataVersion,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{
    Change, ChangeError, ChangeProjection, CodecError, MAX_NESTING_DEPTH, decode_change,
    decode_change_projected, encode_change,
};

use super::support::{assert_change_eq, fixture_hex, hex, representative_change};

const KIND_KEY: &str = "dogpaddle.kind";
const VERSION_KEY: &str = "dogpaddle.change.version";

fn temporal_decimal_change() -> Change {
    let decimal = Decimal128Array::from(vec![
        Some(-99_999_999_999_999_999_999_999_999_999_999_999_999_i128),
        Some(0),
        Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128),
        None,
    ])
    .with_precision_and_scale(38, 6)
    .unwrap();
    let negative_scale = Decimal128Array::from(vec![Some(-9999), Some(-1), Some(9999), None])
        .with_precision_and_scale(4, -2)
        .unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            Some(0),
            None,
        ])),
        Arc::new(TimestampSecondArray::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            None,
        ])),
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(i64::MAX), Some(1), Some(0), None])
                .with_timezone("+00:00"),
        ),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(i64::MIN), Some(-1), Some(0), None])
                .with_timezone("+08:00"),
        ),
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(i64::MAX), Some(1), Some(0), None])
                .with_timezone("America/Los_Angeles"),
        ),
        Arc::new(decimal),
        Arc::new(negative_scale),
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Field::new(format!("field-{index}"), column.data_type().clone(), true)
        })
        .collect::<Vec<_>>();
    let records = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2])).unwrap()
}

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
}

#[test]
fn temporal_and_decimal_stream_has_stable_bytes_and_standard_arrow_interop() {
    let change = temporal_decimal_change();
    let encoded = encode_change(&change).unwrap();
    let expected = fixture_hex(include_str!(
        "../fixtures/v1/temporal_and_decimal_change.hex"
    ));
    assert_eq!(hex(&encoded), expected);
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);

    for selection in [
        vec![],
        vec![0],
        vec![1, 2, 3, 4],
        vec![5, 6],
        (0..7).collect(),
    ] {
        let projection = ChangeProjection::try_new(change.schema(), selection).unwrap();
        assert_change_eq(
            &decode_change_projected(&encoded, &projection).unwrap(),
            &change.try_project(&projection).unwrap(),
        );
    }

    let mut reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let physical = reader.next().unwrap().unwrap();
    for (index, expected) in change.records().columns().iter().enumerate() {
        assert_eq!(physical.column(index + 1).to_data(), expected.to_data());
    }
    assert!(reader.next().is_none());
}

#[test]
fn decimal128_value_overflow_is_rejected_by_full_and_selected_but_not_unselected_decode() {
    let amount = Field::new("amount", DataType::Decimal128(2, -1), true);
    let tail = Field::new("tail", DataType::UInt64, false);
    let logical_schema = Arc::new(Schema::new(vec![amount.clone(), tail.clone()]));
    let physical_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("$dogpaddle.diff", DataType::Int64, false),
            amount,
            tail,
        ],
        HashMap::from([
            (KIND_KEY.to_owned(), "change".to_owned()),
            (VERSION_KEY.to_owned(), "1".to_owned()),
        ]),
    ));
    let invalid_amount = Decimal128Array::from(vec![Some(99), Some(100), None])
        .with_precision_and_scale(2, -1)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&physical_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, -1, 2])),
            Arc::new(invalid_amount),
            Arc::new(UInt64Array::from(vec![7, 8, 9])),
        ],
    )
    .unwrap();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), &physical_schema, options).unwrap();
    writer.write(&batch).unwrap();
    let encoded = writer.into_inner().unwrap();

    assert_decimal_value_error(&decode_change(&encoded));
    let select_amount = ChangeProjection::try_new(Arc::clone(&logical_schema), [0]).unwrap();
    assert_decimal_value_error(&decode_change_projected(&encoded, &select_amount));

    let select_tail = ChangeProjection::try_new(logical_schema, [1]).unwrap();
    let decoded = decode_change_projected(&encoded, &select_tail).unwrap();
    assert_eq!(decoded.diffs().values(), &[1, -1, 2]);
    assert_eq!(decoded.schema().field(0).name(), "tail");
    assert_eq!(
        decoded
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[7, 8, 9]
    );
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
fn every_supported_scalar_layout_round_trips() {
    macro_rules! values {
        ($array:ty; $($value:expr),+ $(,)?) => {
            Arc::new(<$array>::from(vec![$($value),+])) as ArrayRef
        };
    }

    let float32_nan = f32::from_bits(0x7fc0_1234);
    let float64_nan = f64::from_bits(0x7ff8_0000_0000_1234);
    let columns: Vec<ArrayRef> = vec![
        new_null_array(&DataType::Null, 6),
        values!(BooleanArray; Some(false), Some(true), None, Some(false), Some(true), Some(false)),
        values!(Int8Array; Some(i8::MIN), Some(i8::MAX), Some(-1), Some(0), Some(1), None),
        values!(Int16Array; Some(i16::MIN), Some(i16::MAX), Some(-1), Some(0), Some(1), None),
        values!(Int32Array; Some(i32::MIN), Some(i32::MAX), Some(-1), Some(0), Some(1), None),
        values!(Int64Array; Some(i64::MIN), Some(i64::MAX), Some(-1), Some(0), Some(1), None),
        values!(UInt8Array; Some(u8::MIN), Some(u8::MAX), Some(1), Some(u8::MAX - 1), Some(42), None),
        values!(UInt16Array; Some(u16::MIN), Some(u16::MAX), Some(1), Some(u16::MAX - 1), Some(42), None),
        values!(UInt32Array; Some(u32::MIN), Some(u32::MAX), Some(1), Some(u32::MAX - 1), Some(42), None),
        values!(UInt64Array; Some(u64::MIN), Some(u64::MAX), Some(1), Some(u64::MAX - 1), Some(42), None),
        values!(Float32Array; Some(f32::NEG_INFINITY), Some(-0.0), Some(0.0), Some(f32::INFINITY), Some(float32_nan), None),
        values!(Float64Array; Some(f64::NEG_INFINITY), Some(-0.0), Some(0.0), Some(f64::INFINITY), Some(float64_nan), None),
        values!(StringArray; Some(""), Some("犬"), Some("\0"), Some("🦀"), Some("last"), None),
        values!(BinaryArray; Some(&[][..]), Some(&[0][..]), Some(&[u8::MAX, 0][..]), Some(b"bytes"), Some(&[7][..]), None),
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Field::new(format!("field-{index}"), column.data_type().clone(), true)
        })
        .collect::<Vec<_>>();
    let records = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2, 3, -3])).unwrap();
    let decoded = decode_change(&encode_change(&change).unwrap()).unwrap();

    assert_change_eq(&decoded, &change);
    let float32 = decoded
        .records()
        .column(10)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(
        [
            float32.value(0).to_bits(),
            float32.value(1).to_bits(),
            float32.value(2).to_bits(),
            float32.value(3).to_bits(),
            float32.value(4).to_bits()
        ],
        [f32::NEG_INFINITY, -0.0, 0.0, f32::INFINITY, float32_nan].map(f32::to_bits)
    );
    let float64 = decoded
        .records()
        .column(11)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        [
            float64.value(0).to_bits(),
            float64.value(1).to_bits(),
            float64.value(2).to_bits(),
            float64.value(3).to_bits(),
            float64.value(4).to_bits()
        ],
        [f64::NEG_INFINITY, -0.0, 0.0, f64::INFINITY, float64_nan].map(f64::to_bits)
    );
}

#[test]
fn validity_and_boolean_bitmaps_round_trip_across_a_byte_boundary() {
    for rows in [7, 8, 9] {
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
fn maximum_mixed_nesting_preserves_nested_metadata_in_full_and_projected_decodes() {
    let mut mixed = DataType::Int64;
    for depth in 0..MAX_NESTING_DEPTH {
        let child = |name| {
            let field = Field::new(name, mixed.clone(), true);
            if depth + 1 == MAX_NESTING_DEPTH {
                field.with_metadata(HashMap::from([(
                    "nested-semantic".to_owned(),
                    "sentinel".to_owned(),
                )]))
            } else {
                field
            }
        };
        mixed = match mixed {
            DataType::List(_) => DataType::Struct(vec![child("member")].into()),
            _ => DataType::List(Arc::new(child("item"))),
        };
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", mixed.clone(), true),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            new_null_array(&mixed, 1),
            Arc::new(UInt64Array::from(vec![7])),
        ],
    )
    .unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let encoded = encode_change(&change).unwrap();
    let decoded = decode_change(&encoded).unwrap();
    assert_change_eq(&decoded, &change);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let projected = decode_change_projected(&encoded, &projection).unwrap();
    assert_change_eq(&projected, &change.try_project(&projection).unwrap());

    for decoded in [&decoded, &projected] {
        let schema = decoded.schema();
        let child = match schema.field(0).data_type() {
            DataType::List(child) => child,
            DataType::Struct(children) => &children[0],
            data_type => panic!("expected nested field, got {data_type}"),
        };
        assert_eq!(
            child.metadata().get("nested-semantic").map(String::as_str),
            Some("sentinel")
        );
    }
}

#[test]
fn temporal_and_decimal_types_round_trip_inside_complete_nested_subtrees() {
    let dates = ListArray::from_iter_primitive::<Date32Type, _, _>([
        Some(vec![Some(-1), Some(0)]),
        None,
        Some(vec![Some(1)]),
    ]);
    let occurred_at =
        TimestampNanosecondArray::from(vec![Some(-1), None, Some(1)]).with_timezone("UTC");
    let amount = Decimal128Array::from(vec![Some(-12_345), None, Some(67_890)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let occurred_at_field = Arc::new(Field::new(
        "occurred_at",
        occurred_at.data_type().clone(),
        true,
    ));
    let amount_field = Arc::new(Field::new("amount", amount.data_type().clone(), true));
    let object = StructArray::from(vec![
        (occurred_at_field, Arc::new(occurred_at) as ArrayRef),
        (amount_field, Arc::new(amount) as ArrayRef),
    ]);
    let columns: Vec<ArrayRef> = vec![Arc::new(dates), Arc::new(object)];
    let schema = Arc::new(Schema::new(vec![
        Field::new("dates", columns[0].data_type().clone(), true),
        Field::new("object", columns[1].data_type().clone(), true),
    ]));
    let records = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, -1, 1])).unwrap();
    let encoded = encode_change(&change).unwrap();

    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
    for selection in [vec![0], vec![1]] {
        let projection = ChangeProjection::try_new(Arc::clone(&schema), selection).unwrap();
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

fn assert_decimal_value_error(result: &Result<Change, CodecError>) {
    assert!(matches!(
        result,
        Err(CodecError::Change(ChangeError::InvalidDecimal128Value {
            field,
            index: 1,
            value: 100,
            precision: 2,
            scale: -1,
        })) if field == "amount"
    ));
}
