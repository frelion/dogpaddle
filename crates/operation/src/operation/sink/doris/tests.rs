use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, NullArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, types::Int64Type,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use mysql::Value;

use super::{
    config::{DorisSinkConfig, DorisTargetSpec},
    definition::DorisSinkDefinition,
    row::DorisRowCodec,
    schema::DorisLayout,
};
use crate::{decode_definition, encode_definition, operation::sink::relation::RowError};

#[test]
fn definition_round_trips_canonically() {
    let definition = DorisSinkDefinition::try_new(
        DorisTargetSpec::try_new("sink_1", "analytics", "materialized", 42).unwrap(),
    )
    .unwrap();
    let encoded = encode_definition(&definition);
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(decoded.persistence_tag(), 18);
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one row fixture checks the complete supported type mapping and field order"
)]
fn row_codec_maps_every_supported_type_without_reinterpreting_canonical_values() {
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(7), None]), None]);
    let flag = Arc::new(Field::new("flag", DataType::Boolean, false));
    let text = Arc::new(Field::new("text", DataType::Utf8, true));
    let structure = StructArray::from(vec![
        (
            Arc::clone(&flag),
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ),
        (
            Arc::clone(&text),
            Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(2)),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(i8::MIN), None])),
        Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        Arc::new(UInt8Array::from(vec![Some(u8::MAX), None])),
        Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
        Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        Arc::new(Float32Array::from(vec![Some(-0.0), None])),
        Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            None,
        ])),
        Arc::new(StringArray::from(vec![Some("hé\0"), None])),
        Arc::new(BinaryArray::from(vec![Some(&[0, 255][..]), None])),
        Arc::new(list),
        Arc::new(structure),
        Arc::new(Date32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(TimestampSecondArray::from(vec![Some(i64::MIN), None])),
        Arc::new(TimestampMillisecondArray::from(vec![Some(i64::MAX), None]).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]).with_timezone("+08:00")),
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(i64::MIN + 1), None])
                .with_timezone("America/New_York"),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(-123), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("null", DataType::Null, false),
        Field::new("bool", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
        Field::new("list", columns[14].data_type().clone(), true),
        Field::new("struct", DataType::Struct(vec![flag, text].into()), true),
        Field::new("date32", DataType::Date32, true),
        Field::new("timestamp_s", columns[17].data_type().clone(), true),
        Field::new("timestamp_ms", columns[18].data_type().clone(), true),
        Field::new("timestamp_us", columns[19].data_type().clone(), true),
        Field::new("timestamp_ns", columns[20].data_type().clone(), true),
        Field::new("decimal", columns[21].data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let codec = DorisRowCodec::new(DorisLayout::try_new(schema).unwrap());
    let present = codec.encode_row(&batch, 0).unwrap();
    let absent = codec.encode_row(&batch, 1).unwrap();

    assert_eq!(
        present.values,
        vec![
            Value::NULL,
            Value::Int(1),
            Value::Int(i64::from(i8::MIN)),
            Value::Int(i64::from(i16::MIN)),
            Value::Int(i64::from(i32::MIN)),
            Value::Int(i64::MIN),
            Value::UInt(u64::from(u8::MAX)),
            Value::UInt(u64::from(u16::MAX)),
            Value::UInt(u64::from(u32::MAX)),
            Value::Bytes(b"Af//////////".to_vec()),
            Value::Bytes(b"AYAAAAA=".to_vec()),
            Value::Bytes(b"AX/4AAAAAAAB".to_vec()),
            Value::Bytes("hé\0".as_bytes().to_vec()),
            Value::Bytes(b"AQAAAAAAAAACAP8=".to_vec()),
            Value::Bytes(b"AQAAAAAAAAACAQAAAAAAAAAHAA==".to_vec()),
            Value::Bytes(b"AQEBAQAAAAAAAAABeA==".to_vec()),
            Value::Int(i64::from(i32::MIN)),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Int(-1),
            Value::Int(i64::MIN + 1),
            Value::Bytes(b"Af///////////////////4U=".to_vec()),
        ]
    );
    let mut expected_absent = vec![Value::NULL; present.values.len()];
    expected_absent[15] = Value::Bytes(b"AQEAAA==".to_vec());
    assert_eq!(absent.values, expected_absent);
    assert_ne!(present.hash, absent.hash);
    assert_eq!(present.hash.len(), 32);
    assert!(present.hash.iter().all(u8::is_ascii_hexdigit));
}

#[test]
fn row_codec_rejects_null_in_non_nullable_nested_child_without_partial_row() {
    let child = Arc::new(Field::new("required", DataType::Int64, false));
    let structure = StructArray::new(
        vec![Arc::clone(&child)].into(),
        vec![Arc::new(Int64Array::from(vec![Some(7), None]))],
        Some(NullBuffer::from(vec![true, false])),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "object",
        DataType::Struct(vec![child].into()),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(structure)]).unwrap();
    let codec = DorisRowCodec::new(DorisLayout::try_new(schema).unwrap());
    assert_eq!(
        codec.encode_row(&batch, 0).unwrap().values,
        [Value::Bytes(b"AQEAAAAAAAAABw==".to_vec())]
    );
    assert_eq!(codec.encode_row(&batch, 1).unwrap().values, [Value::NULL]);
}

#[test]
fn row_codec_preserves_canonical_nested_type_validation_with_relaxed_record_batch_names() {
    let actual_child = Arc::new(Field::new("actual", DataType::Int64, false));
    let declared_child = Arc::new(Field::new("declared", DataType::Int64, false));
    let structure = StructArray::from(vec![(
        actual_child,
        Arc::new(Int64Array::from(vec![9])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "object",
        DataType::Struct(vec![declared_child].into()),
        false,
    )]));
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![Arc::new(structure)],
        &RecordBatchOptions::new().with_match_field_names(false),
    )
    .unwrap();
    let codec = DorisRowCodec::new(DorisLayout::try_new(schema).unwrap());

    assert_eq!(
        codec.encode_row(&batch, 0),
        Err(RowError::ArrayTypeMismatch {
            field: "object".to_owned(),
            expected: DataType::Struct(vec![Field::new("declared", DataType::Int64, false)].into()),
            actual: DataType::Struct(vec![Field::new("actual", DataType::Int64, false)].into()),
        })
    );
}

#[test]
fn row_codec_rejects_out_of_bounds_index() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap();
    let codec = DorisRowCodec::new(DorisLayout::try_new(schema).unwrap());
    assert!(codec.encode_row(&batch, 1).is_err());
}

#[test]
fn target_spec_rejects_zero_cluster_identity() {
    assert!(DorisTargetSpec::try_new("sink", "db", "table", 0).is_err());
}

#[test]
fn target_spec_rejects_case_folded_state_collision() {
    assert!(DorisTargetSpec::try_new("sink", "db", "$DOGPADDLE.STATE.SINK", 1).is_err());
}

#[test]
fn layout_rejects_case_folded_column_collisions() {
    let duplicate = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("VALUE", DataType::Int64, false),
    ]));
    assert!(DorisLayout::try_new(duplicate).is_err());

    let technical = Arc::new(Schema::new(vec![Field::new(
        "__DOGPADDLE_ID",
        DataType::Int64,
        false,
    )]));
    assert!(DorisLayout::try_new(technical).is_err());
}

#[test]
fn runtime_config_requires_numeric_ip_and_redacts_password() {
    assert!(DorisSinkConfig::new_unencrypted("localhost", 9030, "db", "user", "secret").is_err());
    let config =
        DorisSinkConfig::new_unencrypted("127.0.0.1", 9030, "db", "user", "secret").unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("secret"));
}
