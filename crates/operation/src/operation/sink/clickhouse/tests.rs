use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Number, Value};

use crate::operation::sink::relation::RowError;

use super::{
    config::{ClickHouseSinkConfig, ClickHouseTargetSpec},
    definition::ClickHouseSinkDefinition,
    row::ClickHouseRowCodec,
    schema::ClickHouseLayout,
};
use crate::{decode_definition, encode_definition};

const DATABASE_UUID: &str = "12345678-1234-1234-1234-123456789abc";

#[test]
fn definition_round_trips_canonically() {
    let definition = ClickHouseSinkDefinition::try_new(
        ClickHouseTargetSpec::try_new("sink_1", "analytics", "materialized", DATABASE_UUID)
            .unwrap(),
    )
    .unwrap();
    let encoded = encode_definition(&definition);
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(decoded.persistence_tag(), 19);
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture checks every native numeric width and timestamp unit against JSON Number semantics"
)]
fn row_codec_preserves_number_widths_and_timestamp_units() {
    let columns: Vec<(Field, ArrayRef, Value)> = vec![
        (
            Field::new("bool", DataType::Boolean, false),
            Arc::new(BooleanArray::from(vec![true])),
            Number::from(1_u8).into(),
        ),
        (
            Field::new("i8", DataType::Int8, false),
            Arc::new(Int8Array::from(vec![i8::MIN])),
            Number::from(i8::MIN).into(),
        ),
        (
            Field::new("i16", DataType::Int16, false),
            Arc::new(Int16Array::from(vec![i16::MIN])),
            Number::from(i16::MIN).into(),
        ),
        (
            Field::new("i32", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![i32::MIN])),
            Number::from(i32::MIN).into(),
        ),
        (
            Field::new("i64", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![i64::MIN])),
            Number::from(i64::MIN).into(),
        ),
        (
            Field::new("u8", DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![u8::MAX])),
            Number::from(u8::MAX).into(),
        ),
        (
            Field::new("u16", DataType::UInt16, false),
            Arc::new(UInt16Array::from(vec![u16::MAX])),
            Number::from(u16::MAX).into(),
        ),
        (
            Field::new("u32", DataType::UInt32, false),
            Arc::new(UInt32Array::from(vec![u32::MAX])),
            Number::from(u32::MAX).into(),
        ),
        (
            Field::new("u64", DataType::UInt64, false),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Number::from(u64::MAX).into(),
        ),
        (
            Field::new("date", DataType::Date32, false),
            Arc::new(Date32Array::from(vec![-42])),
            Number::from(-42).into(),
        ),
    ];
    let timestamps: Vec<(Field, ArrayRef, Value)> = vec![
        (
            Field::new(
                "seconds",
                DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                false,
            ),
            Arc::new(TimestampSecondArray::from(vec![i64::MIN])),
            Number::from(i64::MIN).into(),
        ),
        (
            Field::new(
                "millis",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Arc::new(TimestampMillisecondArray::from(vec![i64::MAX]).with_timezone("UTC")),
            Number::from(i64::MAX).into(),
        ),
        (
            Field::new(
                "micros",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+08:00".into())),
                false,
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("+08:00")),
            Number::from(-1).into(),
        ),
        (
            Field::new(
                "nanos",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
            Arc::new(TimestampNanosecondArray::from(vec![1])),
            Number::from(1).into(),
        ),
    ];
    let all = columns.into_iter().chain(timestamps).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        all.iter()
            .map(|(field, _, _)| field.clone())
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        all.iter().map(|(_, array, _)| Arc::clone(array)).collect(),
    )
    .unwrap();
    let row = ClickHouseRowCodec::new(ClickHouseLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();
    assert_eq!(
        row.values,
        all.iter()
            .map(|(_, _, value)| value.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(row.values[8].as_u64(), Some(u64::MAX));
    assert_eq!(row.values[8].to_string(), u64::MAX.to_string());
}

#[test]
fn row_codec_preserves_encoded_float_bits_and_nested_values() {
    let nested_child = Arc::new(Field::new("child", DataType::Int64, true));
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(7), None])]);
    let structure = StructArray::from(vec![(
        Arc::clone(&nested_child),
        Arc::new(Int64Array::from(vec![Some(-9)])) as ArrayRef,
    )]);
    let columns: Vec<(Field, ArrayRef, Vec<u8>)> = vec![
        (
            Field::new("minus_zero", DataType::Float32, false),
            Arc::new(Float32Array::from(vec![-0.0])),
            [vec![1], (-0.0_f32).to_bits().to_be_bytes().to_vec()].concat(),
        ),
        (
            Field::new("plus_zero", DataType::Float32, false),
            Arc::new(Float32Array::from(vec![0.0])),
            [vec![1], 0.0_f32.to_bits().to_be_bytes().to_vec()].concat(),
        ),
        (
            Field::new("nan", DataType::Float64, false),
            Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0001,
            )])),
            [vec![1], 0x7ff8_0000_0000_0001_u64.to_be_bytes().to_vec()].concat(),
        ),
        (
            Field::new("decimal", DataType::Decimal128(10, 2), false),
            Arc::new(
                Decimal128Array::from(vec![-123_i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            [vec![1], (-123_i128).to_be_bytes().to_vec()].concat(),
        ),
        (
            Field::new("text", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["é"])),
            [vec![1, 0, 0, 0, 0, 0, 0, 0, 2], "é".as_bytes().to_vec()].concat(),
        ),
        (
            Field::new("bytes", DataType::Binary, false),
            Arc::new(BinaryArray::from(vec![&[0, 255][..]])),
            vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 255],
        ),
        (
            Field::new("list", list.data_type().clone(), false),
            Arc::new(list),
            [
                vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 1],
                7_i64.to_be_bytes().to_vec(),
                vec![0],
            ]
            .concat(),
        ),
        (
            Field::new("struct", DataType::Struct(vec![nested_child].into()), false),
            Arc::new(structure),
            [vec![1, 1], (-9_i64).to_be_bytes().to_vec()].concat(),
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(field, _, _)| field.clone())
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        columns
            .iter()
            .map(|(_, array, _)| Arc::clone(array))
            .collect(),
    )
    .unwrap();
    let row = ClickHouseRowCodec::new(ClickHouseLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();
    assert_eq!(
        row.values,
        columns
            .iter()
            .map(|(_, _, bytes)| Value::String(STANDARD.encode(bytes)))
            .collect::<Vec<_>>()
    );
    assert_ne!(row.values[0], row.values[1]);
    assert_eq!(row.hash.len(), 32);
}

#[test]
fn row_codec_uses_canonical_null_marker_and_preserves_errors() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("null", DataType::Null, false),
        Field::new("number", DataType::UInt64, true),
        Field::new("encoded", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(NullArray::new(1)) as ArrayRef,
            Arc::new(UInt64Array::from(vec![None])),
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    let codec = ClickHouseRowCodec::new(ClickHouseLayout::try_new(Arc::clone(&schema)).unwrap());
    assert_eq!(
        codec.encode_row(&batch, 0).unwrap().values,
        vec![Value::Null; 4]
    );
    assert_eq!(
        codec.encode_row(&batch, 1).unwrap_err(),
        RowError::RowOutOfBounds {
            row_index: 1,
            rows: 1
        }
    );
    let other_schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::Int64,
        false,
    )]));
    let other =
        RecordBatch::try_new(other_schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();
    assert_eq!(
        codec.encode_row(&other, 0).unwrap_err(),
        RowError::SchemaMismatch
    );
}

#[test]
fn target_spec_rejects_zero_database_uuid() {
    assert!(
        ClickHouseTargetSpec::try_new(
            "sink",
            "db",
            "table",
            "00000000-0000-0000-0000-000000000000"
        )
        .is_err()
    );
    assert!(
        ClickHouseTargetSpec::try_new("sink", "db", "table", "123456781234-1234-1234-123456789abc")
            .is_err()
    );
    assert!(
        ClickHouseTargetSpec::try_new("sink", "db", "$dogpaddle.state.sink", DATABASE_UUID)
            .is_err()
    );
}

#[test]
fn runtime_config_requires_numeric_ip_and_redacts_password() {
    assert!(
        ClickHouseSinkConfig::new_unencrypted("localhost", 8123, "db", "user", "secret").is_err()
    );
    let config =
        ClickHouseSinkConfig::new_unencrypted("127.0.0.1", 8123, "db", "user", "secret").unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("secret"));
}
