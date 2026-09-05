use std::{
    net::TcpListener,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};

use super::{
    PostgresSinkConfig, PostgresSinkError, PostgresTargetSpec,
    config::validate_absence_snapshot,
    row::{PostgresRowCodec, PostgresValue},
    schema::PostgresLayout,
    target::{SqlPlan, quote_identifier},
};
use crate::operation::sink::relation::encode_canonical;

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
fn row_codec_preserves_unsigned_and_float_bit_patterns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("float32", DataType::Float32, false),
        Field::new("float64", DataType::Float64, false),
    ]));
    let float32 = f32::from_bits(0x7f80_0123);
    let float64 = -0.0_f64;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
            Arc::new(Float32Array::from(vec![float32])) as ArrayRef,
            Arc::new(Float64Array::from(vec![float64])) as ArrayRef,
        ],
    )
    .unwrap();
    let encoded = PostgresRowCodec::new(PostgresLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();

    assert_eq!(
        encoded.values,
        [
            PostgresValue::Bytes(Some(u64::MAX.to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(float32.to_bits().to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(float64.to_bits().to_be_bytes().to_vec())),
        ]
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
    let insert = wide.insert_statement(40);
    assert!(insert.contains("$64000::bigint)"));
    assert!(!insert.contains("$64001"));
    let lookup = wide.lookup_statement(40);
    assert!(lookup.contains("$64040::bigint)"));
    assert!(!lookup.contains("$64041"));
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

#[test]
fn row_codec_preserves_the_complete_utf8_domain_as_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["before\0after"])) as ArrayRef],
    )
    .unwrap();
    let encoded = PostgresRowCodec::new(PostgresLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();

    assert_eq!(
        encoded.values,
        [PostgresValue::Bytes(Some(b"before\0after".to_vec()))]
    );
}
