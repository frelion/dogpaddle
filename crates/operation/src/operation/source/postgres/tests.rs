use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_change::Change;
use serde_json::{Value, json};

use super::{PostgresColumn, PostgresSourceError, PostgresType, convert::convert_values, schema};

fn column(data_type: PostgresType) -> PostgresColumn {
    PostgresColumn::new("value", data_type, true)
}

fn envelope(columns: &[PostgresColumn], op: &str, before: Value, after: Value) -> Value {
    let fields = columns
        .iter()
        .map(|column| {
            let (literal, logical) = column.data_type().connect_type();
            let mut schema = json!({"field":column.name(),"type":literal,"optional":column.is_nullable()});
            if let Some(logical) = logical {
                schema["name"] = json!(logical);
            }
            if let PostgresType::Numeric { precision, scale } = column.data_type() {
                schema["parameters"] = json!({"scale":scale.to_string(),"connect.decimal.precision":precision.to_string()});
            }
            schema
        })
        .collect::<Vec<_>>();
    let mut event = json!({
        "schema":{"type":"struct","fields":[
            {"field":"before","type":"struct","optional":true,"fields":fields},
            {"field":"after","type":"struct","optional":true,"fields":fields}
        ]},
        "payload":{
            "source":{"connector":"postgresql","schema":"public","table":"events","snapshot":"false"},
            "op":op
        }
    });
    event["payload"]["before"] = before;
    event["payload"]["after"] = after;
    event
}

fn convert(
    columns: &[PostgresColumn],
    events: &[Value],
) -> Result<Option<Change>, PostgresSourceError> {
    let bytes = events
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    convert_values(
        columns,
        schema::compile(columns)?,
        "source",
        "public",
        "events",
        bytes
            .iter()
            .map(|bytes| (Some("source.public.events"), Some(bytes.as_slice()))),
    )
}

fn inserted(columns: &[PostgresColumn], row: Value) -> Change {
    convert(columns, &[envelope(columns, "c", Value::Null, row)])
        .unwrap()
        .unwrap()
}

#[test]
fn postgres_conversion_preserves_insert_update_delete_event_order() {
    let columns = [column(PostgresType::Int64)];
    let events = [
        envelope(&columns, "c", Value::Null, json!({"value":1})),
        envelope(&columns, "u", json!({"value":1}), json!({"value":2})),
        envelope(&columns, "d", json!({"value":2}), Value::Null),
    ];
    let change = convert(&columns, &events).unwrap().unwrap();
    assert_eq!(change.diffs().values(), &[1, -1, 1, -1]);
    let values = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[1, 1, 2, 2]);
    let separately = events
        .iter()
        .map(|event| {
            convert(&columns, std::slice::from_ref(event))
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let diffs = separately
        .iter()
        .flat_map(|change| change.diffs().values().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(diffs, change.diffs().values().as_ref());
}

#[test]
fn postgres_conversion_preserves_large_text_binary_and_nulls_across_rebatching() {
    let columns = [
        PostgresColumn::new("text", PostgresType::Text, true),
        PostgresColumn::new("binary", PostgresType::Bytea, true),
    ];
    let large = json!({
        "text": "雪🦀\n\"\\".repeat(4096),
        "binary": BASE64_STANDARD.encode([0, 255, 127, 128].repeat(4096))
    });
    let nulls = json!({"text":null,"binary":null});
    let empty = json!({"text":"","binary":""});
    let events = [
        envelope(&columns, "c", Value::Null, large.clone()),
        envelope(&columns, "u", large, nulls.clone()),
        envelope(&columns, "u", nulls, empty.clone()),
        envelope(&columns, "d", empty, Value::Null),
    ];
    let whole = convert(&columns, &events).unwrap().unwrap();
    assert_eq!(whole.diffs().values(), &[1, -1, 1, -1, 1, -1]);
    for batch_size in 1..events.len() {
        let changes = events
            .chunks(batch_size)
            .map(|batch| convert(&columns, batch).unwrap().unwrap())
            .collect::<Vec<_>>();
        let records = arrow_select::concat::concat_batches(
            &whole.records().schema(),
            changes.iter().map(Change::records),
        )
        .unwrap();
        let diffs = changes
            .iter()
            .flat_map(|change| change.diffs().values().iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(&records, whole.records());
        assert_eq!(diffs, whole.diffs().values().as_ref());
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn postgres_conversion_preserves_every_supported_type_and_null() {
    let columns = [
        PostgresColumn::new("boolean", PostgresType::Boolean, true),
        PostgresColumn::new("small", PostgresType::Int16, true),
        PostgresColumn::new("integer", PostgresType::Int32, true),
        PostgresColumn::new("big", PostgresType::Int64, true),
        PostgresColumn::new("real", PostgresType::Float32, true),
        PostgresColumn::new("double", PostgresType::Float64, true),
        PostgresColumn::new("text", PostgresType::Text, true),
        PostgresColumn::new("binary", PostgresType::Bytea, true),
        PostgresColumn::new("date", PostgresType::Date, true),
        PostgresColumn::new("timestamp", PostgresType::Timestamp, true),
        PostgresColumn::new("zoned", PostgresType::TimestampTz, true),
        PostgresColumn::new(
            "numeric",
            PostgresType::Numeric {
                precision: 38,
                scale: 2,
            },
            true,
        ),
    ];
    let unscaled = -99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    let first = envelope(
        &columns,
        "c",
        Value::Null,
        json!({
            "boolean":true,"small":i16::MIN,"integer":i32::MAX,"big":i64::MAX,
            "real":0.1,"double":-1.25,"text":"雪🦀","binary":"AP9/",
            "date":-1,"timestamp":-1,"zoned":"1970-01-01T01:00:00.000001+01:00",
            "numeric":BASE64_STANDARD.encode(unscaled.to_be_bytes())
        }),
    );
    let nulls = columns
        .iter()
        .map(|column| (column.name().to_owned(), Value::Null))
        .collect::<serde_json::Map<_, _>>();
    let second = envelope(&columns, "c", Value::Null, Value::Object(nulls));
    let change = convert(&columns, &[first, second]).unwrap().unwrap();
    let arrays = change.records().columns();
    assert!(
        arrays[0]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        arrays[1]
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        i16::MIN
    );
    assert_eq!(
        arrays[2]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        i32::MAX
    );
    assert_eq!(
        arrays[3]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MAX
    );
    assert_eq!(
        arrays[4]
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        0.1_f32.to_bits()
    );
    assert_eq!(
        arrays[5]
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-1.25_f64).to_bits()
    );
    assert_eq!(
        arrays[6]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "雪🦀"
    );
    assert_eq!(
        arrays[7]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255, 127]
    );
    assert_eq!(
        arrays[8]
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        -1
    );
    assert_eq!(
        arrays[9]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        -1
    );
    assert_eq!(
        arrays[10]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        arrays[11]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        unscaled
    );
    assert!(arrays.iter().all(|array| array.is_null(1)));
    assert_eq!(
        change.records().schema(),
        schema::compile(&columns).unwrap()
    );
}

#[test]
fn postgres_numeric_decodes_signed_big_endian_bytes_without_rounding() {
    let columns = [column(PostgresType::Numeric {
        precision: 4,
        scale: 2,
    })];
    for (encoded, expected) in [
        ("AA==", 0),
        ("fw==", 127),
        ("AIA=", 128),
        ("/w==", -1),
        ("gA==", -128),
        ("/38=", -129),
    ] {
        let change = inserted(&columns, json!({"value":encoded}));
        let values = change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), expected);
    }
    for encoded in ["", "not base64", "JxA=", "2PA=", "AAAAAAAAAAAAAAAAAAAAAAA="] {
        assert!(
            convert(
                &columns,
                &[envelope(
                    &columns,
                    "c",
                    Value::Null,
                    json!({"value":encoded})
                )]
            )
            .is_err()
        );
    }
}

#[test]
fn postgres_conversion_rejects_row_schema_drift_and_incomplete_images() {
    let columns = [PostgresColumn::new("value", PostgresType::Int64, false)];
    let valid = envelope(&columns, "u", json!({"value":1}), json!({"value":2}));
    let mut cases = Vec::new();
    for row in ["before", "after"] {
        for value in [
            Value::Null,
            json!({}),
            json!({"value":null}),
            json!({"value":1,"extra":2}),
        ] {
            let mut event = valid.clone();
            event["payload"][row] = value;
            cases.push(event);
        }
    }
    for row in [0, 1] {
        for (property, value) in [
            ("type", json!("string")),
            ("field", json!("renamed")),
            ("optional", json!(true)),
            ("name", json!("unknown.logical.type")),
        ] {
            let mut event = valid.clone();
            event["schema"]["fields"][row]["fields"][0][property] = value;
            cases.push(event);
        }
        let mut event = valid.clone();
        event["schema"]["fields"][row]["fields"] = json!([]);
        cases.push(event);
    }
    for event in cases {
        assert!(convert(&columns, &[event]).is_err());
    }
    let decimal = [column(PostgresType::Numeric {
        precision: 4,
        scale: 2,
    })];
    for parameter in ["scale", "connect.decimal.precision"] {
        let mut event = envelope(&decimal, "c", Value::Null, json!({"value":"AA=="}));
        event["schema"]["fields"][0]["fields"][0]["parameters"][parameter] = json!("3");
        assert!(convert(&decimal, &[event]).is_err());
    }
}

#[test]
fn postgres_conversion_rejects_snapshot_truncate_and_wrong_source() {
    let columns = [column(PostgresType::Int64)];
    for operation in ["r", "t", "m", "unknown"] {
        assert!(
            convert(
                &columns,
                &[envelope(
                    &columns,
                    operation,
                    Value::Null,
                    json!({"value":1})
                )]
            )
            .is_err()
        );
    }
    for property in ["schema", "table", "connector", "snapshot"] {
        let mut event = envelope(&columns, "c", Value::Null, json!({"value":1}));
        event["payload"]["source"][property] = json!("wrong");
        assert!(convert(&columns, &[event]).is_err());
    }
}

#[test]
fn postgres_streaming_accepts_the_bridges_null_snapshot_marker() {
    let columns = [column(PostgresType::Int64)];
    for marker in [Value::Null, json!(false), json!("false")] {
        let mut event = envelope(&columns, "c", Value::Null, json!({"value":1}));
        event["payload"]["source"]["snapshot"] = marker;
        assert!(convert(&columns, &[event]).unwrap().is_some());
    }
    for marker in [
        json!(true),
        json!("true"),
        json!("last"),
        json!("incremental"),
    ] {
        let mut event = envelope(&columns, "c", Value::Null, json!({"value":1}));
        event["payload"]["source"]["snapshot"] = marker;
        assert!(convert(&columns, &[event]).is_err());
    }
    let mut snapshot = envelope(&columns, "r", Value::Null, json!({"value":1}));
    snapshot["payload"]["source"]["snapshot"] = Value::Null;
    assert!(convert(&columns, &[snapshot]).is_err());
}

#[test]
fn postgres_conversion_accepts_only_identified_control_records() {
    let columns = [column(PostgresType::Int64)];
    let heartbeat = serde_json::to_vec(&json!({"schema":{"type":"struct","name":"io.debezium.connector.common.Heartbeat","fields":[{"field":"ts_ms","type":"int64","optional":false}]},"payload":{"ts_ms":123}})).unwrap();
    let convert_control = |topic, value| {
        convert_values(
            &columns,
            schema::compile(&columns).unwrap(),
            "source",
            "public",
            "events",
            [(topic, value)],
        )
    };
    assert!(
        convert_control(Some("source.public.events"), None)
            .unwrap()
            .is_none()
    );
    assert!(
        convert_control(
            Some("__debezium-heartbeat.source"),
            Some(heartbeat.as_slice())
        )
        .unwrap()
        .is_none()
    );
    assert!(convert_control(Some("foreign.public.events"), None).is_err());
    assert!(convert_control(Some("__debezium-heartbeat.source"), None).is_err());
    assert!(convert_control(Some("source.public.events"), Some(heartbeat.as_slice())).is_err());
    assert!(convert_control(None, None).is_err());
    assert!(convert_control(Some("source.public.events"), Some(b"{}".as_slice())).is_err());
}

#[test]
fn postgres_conversion_rejects_overflow_special_temporal_and_toast_values() {
    for (data_type, value) in [
        (PostgresType::Int16, json!(32768)),
        (PostgresType::Int32, json!(2_147_483_648_u64)),
        (PostgresType::Int64, json!(u64::MAX)),
        (PostgresType::Float32, json!(1e100)),
        (PostgresType::Boolean, json!("true")),
        (PostgresType::Date, json!(-2_147_483_648_i64)),
        (
            PostgresType::Timestamp,
            json!(9_223_372_036_825_200_000_i64),
        ),
        (
            PostgresType::Timestamp,
            json!(-9_223_372_036_832_400_000_i64),
        ),
        (PostgresType::TimestampTz, json!("infinity")),
        (
            PostgresType::TimestampTz,
            json!("1970-01-01T00:00:00.000000001Z"),
        ),
        (PostgresType::Text, json!("__debezium_unavailable_value")),
        (
            PostgresType::Bytea,
            json!(BASE64_STANDARD.encode(b"__debezium_unavailable_value")),
        ),
        (PostgresType::Bytea, json!("bad base64")),
    ] {
        let columns = [column(data_type)];
        assert!(
            convert(
                &columns,
                &[envelope(&columns, "c", Value::Null, json!({"value":value}))]
            )
            .is_err(),
            "{data_type:?}"
        );
    }
}

#[test]
fn postgres_float_preserves_signed_zero_nan_and_infinities() {
    for data_type in [PostgresType::Float32, PostgresType::Float64] {
        let columns = [column(data_type)];
        for (value, expected) in [
            (json!(-0.0), -0.0_f64),
            (json!("NaN"), f64::NAN),
            (json!("Infinity"), f64::INFINITY),
            (json!("-Infinity"), f64::NEG_INFINITY),
        ] {
            let change = inserted(&columns, json!({"value":value}));
            let array = change.records().column(0);
            let actual = match data_type {
                PostgresType::Float32 => f64::from(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .value(0),
                ),
                PostgresType::Float64 => array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                _ => unreachable!(),
            };
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual.to_bits(), expected.to_bits());
            }
        }
    }
}
