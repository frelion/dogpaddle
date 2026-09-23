use std::{
    net::TcpListener,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, NullArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use super::{
    PostgresSinkConfig, PostgresSinkError, PostgresTargetSpec,
    config::validate_absence_snapshot,
    row::{PostgresRowCodec, PostgresValue},
    schema::PostgresLayout,
    target::{SqlPlan, quote_identifier},
};
use crate::operation::sink::relation::{RowError, canonical_row, encode_canonical, row_hash};

fn spec(table: &str) -> PostgresTargetSpec {
    PostgresTargetSpec::try_new("sink_1", "database", "Target Schema", table, "1", 2).unwrap()
}

#[test]
fn runtime_config_debug_redacts_the_password() {
    let config = PostgresSinkConfig::new_unencrypted(
        "127.0.0.1",
        5432,
        "database",
        "writer",
        "visible-secret",
    )
    .unwrap();
    let debug = format!("{config:?}");

    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("visible-secret"));
}

#[test]
fn runtime_config_rejects_dns_names_to_keep_connect_deadlines_bounded() {
    assert!(matches!(
        PostgresSinkConfig::new_unencrypted(
            "localhost",
            5432,
            "database",
            "writer",
            "secret",
        ),
        Err(PostgresSinkError::InvalidConfig { message })
            if message == "host must be a numeric IPv4 or IPv6 address"
    ));
}

#[test]
fn a_peer_that_accepts_tcp_but_never_handshakes_hits_the_connect_deadline() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (release, released) = mpsc::channel();
    let peer = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        released.recv().unwrap();
    });
    let config =
        PostgresSinkConfig::new_unencrypted("127.0.0.1", port, "database", "writer", "secret")
            .unwrap();

    let result = config.connect_with_timeout(Duration::from_millis(100));
    release.send(()).unwrap();
    peer.join().unwrap();

    assert!(matches!(
        result,
        Err(PostgresSinkError::Timeout { stage: "connect" })
    ));
}

#[test]
fn absence_snapshot_rejects_missing_schema_class_and_table_row_type() {
    let target = spec("target");

    assert!(matches!(
        validate_absence_snapshot(&target, false, None, false),
        Err(PostgresSinkError::TargetMissing { name }) if name == "Target Schema"
    ));
    assert!(matches!(
        validate_absence_snapshot(
            &target,
            true,
            Some("$dogpaddle.hash.sink_1".to_owned()),
            false,
        ),
        Err(PostgresSinkError::TargetExists { name })
            if name == "$dogpaddle.hash.sink_1"
    ));
    assert!(matches!(
        validate_absence_snapshot(&target, true, None, true),
        Err(PostgresSinkError::TargetExists { name }) if name == "target"
    ));
    assert!(validate_absence_snapshot(&target, true, None, false).is_ok());
}

#[test]
fn identifiers_are_quoted_as_independent_postgresql_components() {
    assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "odd\"column",
        DataType::Int64,
        false,
    )]));
    let layout = PostgresLayout::try_new(schema).unwrap();
    let plan = SqlPlan::new(&spec("odd.table"), &layout);

    assert!(plan.initialize.contains("\"Target Schema\".\"odd.table\""));
    assert!(
        plan.initialize
            .contains("CREATE INDEX \"$dogpaddle.hash.sink_1\" ON")
    );
    assert!(
        !plan
            .initialize
            .contains("CREATE INDEX \"Target Schema\".\"$dogpaddle.hash.sink_1\"")
    );
    assert!(plan.insert_statement(1).contains("\"odd\"\"column\""));
    assert!(!plan.initialize.contains("\"Target Schema.odd.table\""));
}

#[test]
fn maximum_sink_identity_keeps_derived_names_below_postgresql_limit() {
    let sink_id = "a".repeat(32);
    let spec = PostgresTargetSpec::try_new(sink_id, "database", "schema", "table", "1", 2).unwrap();

    assert!(spec.object_names().iter().all(|name| name.len() <= 63));
}

#[test]
#[allow(clippy::too_many_lines)]
fn row_codec_maps_fixed_width_values_and_typed_nulls_from_canonical_bytes() {
    let fields = vec![
        Field::new("null", DataType::Null, false),
        Field::new("boolean", DataType::Boolean, true),
        Field::new("int8", DataType::Int8, true),
        Field::new("int16", DataType::Int16, true),
        Field::new("int32", DataType::Int32, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("uint8", DataType::UInt8, true),
        Field::new("uint16", DataType::UInt16, true),
        Field::new("uint32", DataType::UInt32, true),
        Field::new("uint64", DataType::UInt64, true),
        Field::new("float32", DataType::Float32, true),
        Field::new("float64", DataType::Float64, true),
        Field::new("date32", DataType::Date32, true),
        Field::new("seconds", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new(
            "millis",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "micros",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("decimal", DataType::Decimal128(10, 2), true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(2)),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(-128), None])),
        Arc::new(Int16Array::from(vec![Some(-32_768), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        Arc::new(UInt8Array::from(vec![Some(u8::MAX), None])),
        Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
        Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(0x7f80_0123)),
            None,
        ])),
        Arc::new(Float64Array::from(vec![Some(-0.0_f64), None])),
        Arc::new(Date32Array::from(vec![Some(-12), None])),
        Arc::new(TimestampSecondArray::from(vec![Some(-1), None])),
        Arc::new(TimestampMillisecondArray::from(vec![Some(-2), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(-3), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(-4), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-12345), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some("before\0after"), None])),
        Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
    ];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let codec = PostgresRowCodec::new(PostgresLayout::try_new(schema).unwrap());
    let present = codec.encode_row(&batch, 0).unwrap();
    let nulls = codec.encode_row(&batch, 1).unwrap();

    assert_eq!(present.hash, row_hash(&canonical_row(&batch, 0).unwrap()));
    assert_eq!(nulls.hash, row_hash(&canonical_row(&batch, 1).unwrap()));
    assert_eq!(
        present.values,
        vec![
            PostgresValue::Bytes(None),
            PostgresValue::Boolean(Some(true)),
            PostgresValue::Int16(Some(-128)),
            PostgresValue::Int16(Some(-32_768)),
            PostgresValue::Int32(Some(i32::MIN)),
            PostgresValue::Int64(Some(i64::MIN)),
            PostgresValue::Int16(Some(i16::from(u8::MAX))),
            PostgresValue::Int32(Some(i32::from(u16::MAX))),
            PostgresValue::Int64(Some(i64::from(u32::MAX))),
            PostgresValue::Bytes(Some(u64::MAX.to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(0x7f80_0123_u32.to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some((-0.0_f64).to_bits().to_be_bytes().to_vec())),
            PostgresValue::Int32(Some(-12)),
            PostgresValue::Int64(Some(-1)),
            PostgresValue::Int64(Some(-2)),
            PostgresValue::Int64(Some(-3)),
            PostgresValue::Int64(Some(-4)),
            PostgresValue::Bytes(Some((-12345_i128).to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(b"before\0after".to_vec())),
            PostgresValue::Bytes(Some(vec![0, 255])),
        ]
    );
    assert_eq!(
        nulls.values,
        vec![
            PostgresValue::Bytes(None),
            PostgresValue::Boolean(None),
            PostgresValue::Int16(None),
            PostgresValue::Int16(None),
            PostgresValue::Int32(None),
            PostgresValue::Int64(None),
            PostgresValue::Int16(None),
            PostgresValue::Int32(None),
            PostgresValue::Int64(None),
            PostgresValue::Bytes(None),
            PostgresValue::Bytes(None),
            PostgresValue::Bytes(None),
            PostgresValue::Int32(None),
            PostgresValue::Int64(None),
            PostgresValue::Int64(None),
            PostgresValue::Int64(None),
            PostgresValue::Int64(None),
            PostgresValue::Bytes(None),
            PostgresValue::Bytes(None),
            PostgresValue::Bytes(None),
        ]
    );
    assert_eq!(
        codec.encode_row(&batch, 2),
        Err(RowError::RowOutOfBounds {
            row_index: 2,
            rows: 2,
        })
    );
}

#[test]
fn row_codec_uses_shared_canonical_bytes_for_nested_values() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let flag = Arc::new(Field::new("flag", DataType::Boolean, false));
    let label = Arc::new(Field::new("label", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("items", DataType::List(Arc::clone(&item)), false),
        Field::new(
            "object",
            DataType::Struct(vec![Arc::clone(&flag), Arc::clone(&label)].into()),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
                vec![Some(7), None, Some(-2)],
            )])) as ArrayRef,
            Arc::new(StructArray::from(vec![
                (flag, Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
                (
                    label,
                    Arc::new(StringArray::from(vec![Some("nested\0value")])) as ArrayRef,
                ),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let encoded = PostgresRowCodec::new(PostgresLayout::try_new(Arc::clone(&schema)).unwrap())
        .encode_row(&batch, 0)
        .unwrap();
    let expected = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            let mut bytes = Vec::new();
            encode_canonical(field, array.as_ref(), 0, field.name(), &mut bytes).unwrap();
            PostgresValue::Bytes(Some(bytes))
        })
        .collect::<Vec<_>>();

    assert_eq!(encoded.values, expected);
}

#[test]
fn matching_and_batched_writes_bind_exact_typed_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let plan = SqlPlan::new(&spec("target"), &PostgresLayout::try_new(schema).unwrap());
    let lookup = plan.lookup_statement(2);
    let mismatch = plan.mismatch_statement(2);

    assert!(
        lookup
            .contains("target.\"a\" = request.c0 OR (target.\"a\" IS NULL AND request.c0 IS NULL)")
    );
    assert!(
        lookup
            .contains("target.\"b\" = request.c1 OR (target.\"b\" IS NULL AND request.c1 IS NULL)")
    );
    assert!(lookup.contains("(0, $1::bigint, $2::bigint, $3::bytea, $4::bigint, $5::bytea), (1, $6::bigint, $7::bigint, $8::bytea, $9::bigint, $10::bytea)"));
    assert!(lookup.contains("LIMIT request.needed"));
    assert!(lookup.contains("LIMIT request.take"));
    assert!(lookup.contains("request.needed > request.take"));
    assert!(lookup.ends_with("ORDER BY request.n"));
    assert!(!lookup.contains("excluded"));
    assert!(mismatch.contains("$1::bigint[]"));
    assert!(mismatch.contains("$5::bigint[]"));
    assert!(mismatch.contains("target.\"$dogpaddle.hash\" IS DISTINCT FROM expected.hash"));
    assert!(mismatch.contains("target.\"a\" IS DISTINCT FROM expected.c0"));
    assert!(mismatch.contains("target.\"b\" IS DISTINCT FROM expected.c1"));
    assert!(mismatch.contains("target.\"$dogpaddle.id\" = ANY(expected.ids)"));

    assert_eq!(
        plan.delete,
        "DELETE FROM ONLY \"Target Schema\".\"target\" WHERE \"$dogpaddle.id\" = ANY($1::bigint[])"
    );
    let insert = plan.insert_statement(2);
    assert!(insert.contains("($1::bigint, $2::bytea, $3::bigint, $4::bytea), ($5::bigint, $6::bytea, $7::bigint, $8::bytea)"));
    assert!(insert.ends_with("ON CONFLICT (\"$dogpaddle.id\") DO NOTHING"));
    assert!(!insert.contains("RETURNING"));
}

#[test]
fn statements_handle_empty_and_wide_schemas_within_parameter_limits() {
    let empty = SqlPlan::new(
        &spec("empty"),
        &PostgresLayout::try_new(Arc::new(Schema::empty())).unwrap(),
    );
    assert_eq!(empty.insert_batch_size(), 1024);
    assert_eq!(empty.lookup_batch_size(), 1024);
    assert!(
        empty
            .insert_statement(1)
            .contains("($1::bigint, $2::bytea)")
    );
    assert!(
        empty
            .lookup_statement(1)
            .contains("(0, $1::bigint, $2::bigint, $3::bytea)")
    );

    let fields = (0..1_598)
        .map(|index| Field::new(format!("f{index}"), DataType::Int64, true))
        .collect::<Vec<_>>();
    let wide = SqlPlan::new(
        &spec("wide"),
        &PostgresLayout::try_new(Arc::new(Schema::new(fields))).unwrap(),
    );
    assert_eq!(wide.insert_batch_size(), 40);
    assert_eq!(wide.lookup_batch_size(), 40);
    assert_eq!(wide.mismatch_batch_size(), 40);
    let insert = wide.insert_statement(40);
    assert!(insert.contains("$64000::bigint)"));
    assert!(!insert.contains("$64001"));
    let lookup = wide.lookup_statement(40);
    assert!(lookup.contains("$64040::bigint)"));
    assert!(!lookup.contains("$64041"));
    let mismatch = wide.mismatch_statement(40);
    assert!(mismatch.contains("$64000::bigint)"));
    assert!(!mismatch.contains("$64001"));
}

#[test]
fn layout_owns_only_the_target_and_two_indexes() {
    let target = spec("target");
    assert_eq!(
        target.object_names(),
        ["target", "$dogpaddle.hash.sink_1", "$dogpaddle.pk.sink_1",]
    );
    let plan = SqlPlan::new(
        &target,
        &PostgresLayout::try_new(Arc::new(Schema::empty())).unwrap(),
    );
    assert_eq!(plan.initialize.matches("CREATE TABLE").count(), 1);
    assert!(plan.initialize.contains("dogpaddle.postgres-relation.v2:"));
}
