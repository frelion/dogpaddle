use std::sync::Arc;

use arrow_schema::{DataType, Field};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::Schema;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_change::Change;
use serde_json::{Value, json};

use super::{
    PostgresCdcScanError, PostgresCdcScanSpec,
    convert::{convert_capture_values, convert_values},
    schema,
};
use crate::operation::scan::{cdc_convert::SnapshotProgress, cdc_runtime::Captured};

fn column(data_type: DataType) -> Field {
    Field::new("value", data_type, true)
}

fn spec(columns: &[Field]) -> PostgresCdcScanSpec {
    PostgresCdcScanSpec {
        engine_name: "source".to_owned(),
        database: "shop".to_owned(),
        schema: "public".to_owned(),
        table: "events".to_owned(),
        slot: "events_slot".to_owned(),
        publication: "events_pub".to_owned(),
        system_identifier: "123".to_owned(),
        database_oid: 42,
        table_oid: 43,
        columns: columns.to_vec().into(),
    }
}

fn identity_projection(columns: &[Field]) -> Vec<u32> {
    (0..u32::try_from(columns.len()).unwrap()).collect()
}

fn envelope(columns: &[Field], op: &str, before: Value, after: Value) -> Value {
    let fields = columns
        .iter()
        .map(|column| {
            let (literal, logical) = crate::operation::scan::cdc_convert::connect_type(column.data_type()).unwrap();
            let mut schema = json!({"field":column.name(),"type":literal,"optional":column.is_nullable()});
            if let Some(logical) = logical {
                schema["name"] = json!(logical);
            }
            if let DataType::Decimal128(precision, scale) = column.data_type() {
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
            "source":{"connector":"postgresql","schema":"public","table":"events","snapshot":null},
            "op":op
        }
    });
    event["payload"]["before"] = before;
    event["payload"]["after"] = after;
    event
}

fn convert(columns: &[Field], events: &[Value]) -> Result<Option<Change>, PostgresCdcScanError> {
    let projection = identity_projection(columns);
    convert_projected(columns, &projection, events)
}

fn convert_projected(
    columns: &[Field],
    projection: &[u32],
    events: &[Value],
) -> Result<Option<Change>, PostgresCdcScanError> {
    let bytes = events
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    let output_schema = projected_schema(columns, projection)?;
    convert_values(
        &spec(columns),
        projection,
        output_schema,
        bytes
            .iter()
            .map(|bytes| (Some("source.public.events"), Some(bytes.as_slice()))),
    )
}

fn projected_schema(
    columns: &[Field],
    projection: &[u32],
) -> Result<Arc<Schema>, PostgresCdcScanError> {
    let full_schema = schema::compile(&columns.to_vec().into())?;
    let fields = projection
        .iter()
        .map(|index| full_schema.fields()[usize::try_from(*index).unwrap()].clone())
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        full_schema.metadata().clone(),
    )))
}

fn heartbeat() -> Value {
    json!({
        "schema":{
            "type":"struct",
            "name":"io.debezium.connector.common.Heartbeat",
            "fields":[{"field":"ts_ms","type":"int64","optional":false}]
        },
        "payload":{"ts_ms":123}
    })
}

fn snapshot(columns: &[Field], marker: &str, row: Value) -> Value {
    let mut event = envelope(columns, "r", Value::Null, row);
    event["payload"]["source"]["snapshot"] = json!(marker);
    event
}

fn capture(
    columns: &[Field],
    events: &[(&str, Value)],
    progress: SnapshotProgress,
) -> Result<Captured, PostgresCdcScanError> {
    let projection = identity_projection(columns);
    capture_projected(columns, &projection, events, progress)
}

fn capture_projected(
    columns: &[Field],
    projection: &[u32],
    events: &[(&str, Value)],
    progress: SnapshotProgress,
) -> Result<Captured, PostgresCdcScanError> {
    let bytes = events
        .iter()
        .map(|(_, event)| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    convert_capture_values(
        &spec(columns),
        projection,
        projected_schema(columns, projection)?,
        events
            .iter()
            .zip(&bytes)
            .map(|((topic, _), bytes)| (Some(*topic), Some(bytes.as_slice()))),
        progress,
    )
}

fn notification(kind: &str) -> Value {
    json!({
        "schema":{"type":"struct","name":"io.debezium.pipeline.notification.Notification"},
        "payload":{"aggregate_type":"Initial Snapshot","type":kind},
    })
}

fn inserted(columns: &[Field], row: Value) -> Change {
    convert(columns, &[envelope(columns, "c", Value::Null, row)])
        .unwrap()
        .unwrap()
}

#[test]
fn postgres_cdc_conversion_preserves_insert_update_delete_event_order() {
    let columns = [column(DataType::Int64)];
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
fn postgres_cdc_projection_validates_full_rows_and_preserves_zero_column_row_count() {
    let columns = [
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("unused", DataType::Binary, false),
    ];
    let event = envelope(
        &columns,
        "c",
        Value::Null,
        json!({"id":7,"payload":"kept","unused":BASE64_STANDARD.encode([1, 2, 3].repeat(2048))}),
    );

    let projected = convert_projected(&columns, &[1], std::slice::from_ref(&event))
        .unwrap()
        .unwrap();
    assert_eq!(projected.records().num_columns(), 1);
    assert_eq!(projected.records().schema().field(0).name(), "payload");
    assert_eq!(
        projected
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "kept"
    );

    let empty = convert_projected(&columns, &[], std::slice::from_ref(&event))
        .unwrap()
        .unwrap();
    assert_eq!(empty.records().num_columns(), 0);
    assert_eq!(empty.records().num_rows(), 1);
    assert_eq!(empty.diffs().values(), &[1]);

    let malformed_base64 = format!("{}!", event["payload"]["after"]["unused"].as_str().unwrap());
    let mut malformed = event.clone();
    malformed["payload"]["after"]["unused"] = json!(malformed_base64);
    assert!(convert_projected(&columns, &[], &[malformed]).is_err());

    let mut incomplete = event;
    incomplete["payload"]["after"]
        .as_object_mut()
        .unwrap()
        .remove("unused");
    assert!(convert_projected(&columns, &[], &[incomplete]).is_err());
}

#[test]
fn postgres_cdc_projection_rejects_bad_unselected_values_in_streaming_and_capture() {
    let columns = [
        Field::new("id", DataType::Int64, false),
        Field::new("unused", DataType::Int32, false),
    ];
    let projections: [&[u32]; 2] = [&[0], &[]];

    for projection in projections {
        let row = json!({"id":7,"unused":"bad"});
        let streaming = envelope(&columns, "c", Value::Null, row.clone());
        assert!(convert_projected(&columns, projection, &[streaming]).is_err());

        let capture_event = snapshot(&columns, "last", row);
        assert!(
            capture_projected(
                &columns,
                projection,
                &[("source.public.events", capture_event)],
                SnapshotProgress::default(),
            )
            .is_err()
        );
    }
}

#[test]
fn postgres_cdc_conversion_preserves_large_text_binary_and_nulls_across_rebatching() {
    let columns = [
        Field::new("text", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
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
fn postgres_cdc_conversion_preserves_every_supported_type_and_null() {
    let columns = [
        Field::new("boolean", DataType::Boolean, true),
        Field::new("small", DataType::Int16, true),
        Field::new("integer", DataType::Int32, true),
        Field::new("big", DataType::Int64, true),
        Field::new("real", DataType::Float32, true),
        Field::new("double", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
        Field::new("date", DataType::Date32, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "zoned",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("numeric", DataType::Decimal128(38, 2), true),
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
        schema::compile(&columns.to_vec().into()).unwrap()
    );
}

#[test]
fn postgres_cdc_numeric_decodes_signed_big_endian_bytes_without_rounding() {
    let columns = [column(DataType::Decimal128(4, 2))];
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
fn postgres_cdc_conversion_rejects_row_schema_drift_and_incomplete_images() {
    let columns = [Field::new("value", DataType::Int64, false)];
    let valid = envelope(&columns, "u", json!({"value":1}), json!({"value":2}));
    let mut missing_image = valid.clone();
    missing_image["payload"]["before"] = Value::Null;
    assert!(matches!(
        convert(&columns, &[missing_image]),
        Err(PostgresCdcScanError::InvalidRecord(message))
            if message == "missing complete row image; the captured table requires REPLICA IDENTITY FULL"
    ));
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
    let decimal = [column(DataType::Decimal128(4, 2))];
    for parameter in ["scale", "connect.decimal.precision"] {
        let mut event = envelope(&decimal, "c", Value::Null, json!({"value":"AA=="}));
        event["schema"]["fields"][0]["fields"][0]["parameters"][parameter] = json!("3");
        assert!(matches!(
            convert(&decimal, &[event]),
            Err(PostgresCdcScanError::InvalidRecord(message))
                if message == "decimal schema changed at column value"
        ));
    }
}

#[test]
fn postgres_cdc_conversion_rejects_snapshot_truncate_and_wrong_metadata() {
    let columns = [column(DataType::Int64)];
    for operation in ["r", "m", "unknown"] {
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
    assert_eq!(
        convert(
            &columns,
            &[envelope(&columns, "t", Value::Null, Value::Null)]
        )
        .unwrap_err()
        .to_string(),
        "invalid PostgreSQL CDC scan record: record operation or snapshot marker is invalid for the current CDC phase"
    );
    for property in ["schema", "table", "connector", "snapshot"] {
        let mut event = envelope(&columns, "c", Value::Null, json!({"value":1}));
        event["payload"]["source"][property] = json!("wrong");
        assert!(convert(&columns, &[event]).is_err());
    }
}

#[test]
fn postgres_cdc_uses_the_single_table_debezium_snapshot_marker_contract() {
    let columns = [column(DataType::Int64)];
    for marker in ["true", "first"] {
        let captured = capture(
            &columns,
            &[(
                "source.public.events",
                snapshot(&columns, marker, json!({"value":1})),
            )],
            SnapshotProgress::default(),
        )
        .unwrap();
        assert!(!captured.sealed);
        assert!(captured.change.is_some());
    }
    let terminal = capture(
        &columns,
        &[(
            "source.public.events",
            snapshot(&columns, "last", json!({"value":1})),
        )],
        SnapshotProgress::default(),
    )
    .unwrap();
    assert!(
        capture(
            &columns,
            &[("__dogpaddle-notification.source", notification("COMPLETED"))],
            terminal.progress,
        )
        .unwrap()
        .sealed
    );

    let mut streaming = envelope(&columns, "c", Value::Null, json!({"value":1}));
    streaming["payload"]["source"]["snapshot"] = Value::Null;
    assert!(convert(&columns, &[streaming]).unwrap().is_some());

    for marker in [
        json!(true),
        json!(false),
        json!("false"),
        json!("first_in_data_collection"),
        json!("last_in_data_collection"),
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
fn postgres_cdc_capture_keeps_snapshot_and_wal_rows_across_the_completion_boundary() {
    let columns = [column(DataType::Int64)];
    let mut inserted_after_snapshot = envelope(&columns, "c", Value::Null, json!({"value":3}));
    inserted_after_snapshot["payload"]["source"]["snapshot"] = Value::Null;
    let first = capture(
        &columns,
        &[
            (
                "source.public.events",
                snapshot(&columns, "true", json!({"value":1})),
            ),
            (
                "source.public.events",
                snapshot(&columns, "last", json!({"value":2})),
            ),
            ("source.public.events", inserted_after_snapshot),
        ],
        SnapshotProgress::default(),
    )
    .unwrap();
    assert!(!first.sealed);
    let first_change = first.change.unwrap();
    assert_eq!(first_change.diffs().values(), &[1, 1, 1]);
    assert_eq!(
        first_change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );

    let update = envelope(&columns, "u", json!({"value":3}), json!({"value":4}));
    let delete = envelope(&columns, "d", json!({"value":4}), Value::Null);
    let second = capture(
        &columns,
        &[
            ("source.public.events", update),
            ("__debezium-heartbeat.source", heartbeat()),
            ("__dogpaddle-notification.source", notification("COMPLETED")),
            ("source.public.events", delete),
        ],
        first.progress,
    )
    .unwrap();
    assert!(second.sealed);
    let second_change = second.change.unwrap();
    assert_eq!(second_change.diffs().values(), &[-1, 1, -1]);
    assert_eq!(
        second_change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3, 4, 4]
    );
}

#[test]
fn postgres_cdc_capture_uses_explicit_completion_for_an_empty_snapshot() {
    let columns = [column(DataType::Int64)];
    let captured = capture(
        &columns,
        &[
            ("__debezium-heartbeat.source", heartbeat()),
            ("__dogpaddle-notification.source", notification("COMPLETED")),
            (
                "source.public.events",
                envelope(&columns, "c", Value::Null, json!({"value":1})),
            ),
        ],
        SnapshotProgress::default(),
    )
    .unwrap();
    assert!(captured.sealed);
    let change = captured.change.unwrap();
    assert_eq!(change.diffs().values(), &[1]);
}

#[test]
fn postgres_cdc_capture_rejects_incomplete_or_reopened_snapshot_order() {
    let columns = [column(DataType::Int64)];
    let incomplete = capture(
        &columns,
        &[
            (
                "source.public.events",
                snapshot(&columns, "true", json!({"value":1})),
            ),
            ("__debezium-heartbeat.source", heartbeat()),
            ("__dogpaddle-notification.source", notification("COMPLETED")),
        ],
        SnapshotProgress::default(),
    );
    assert!(incomplete.is_err());

    let reopened = capture(
        &columns,
        &[
            ("__dogpaddle-notification.source", notification("COMPLETED")),
            (
                "source.public.events",
                snapshot(&columns, "last", json!({"value":1})),
            ),
        ],
        SnapshotProgress::default(),
    );
    assert!(reopened.is_err());

    let streaming_before_last = capture(
        &columns,
        &[(
            "source.public.events",
            envelope(&columns, "c", Value::Null, json!({"value":1})),
        )],
        SnapshotProgress::default(),
    );
    assert!(streaming_before_last.is_err());

    let progress = capture(
        &columns,
        &[("__dogpaddle-notification.source", notification("STARTED"))],
        SnapshotProgress::default(),
    )
    .unwrap();
    assert!(!progress.sealed);
    for kind in ["ABORTED", "SKIPPED", "UNKNOWN"] {
        assert!(
            capture(
                &columns,
                &[("__dogpaddle-notification.source", notification(kind))],
                SnapshotProgress::default(),
            )
            .is_err()
        );
    }
    assert!(
        capture(
            &columns,
            &[
                ("__dogpaddle-notification.source", notification("COMPLETED")),
                ("__dogpaddle-notification.source", notification("COMPLETED")),
            ],
            SnapshotProgress::default(),
        )
        .is_err()
    );
    let mut malformed = notification("COMPLETED");
    malformed["payload"]["aggregate_type"] = json!("Other");
    assert!(
        capture(
            &columns,
            &[("__dogpaddle-notification.source", malformed)],
            SnapshotProgress::default(),
        )
        .is_err()
    );
}

#[test]
fn postgres_cdc_conversion_accepts_only_identified_control_records() {
    let columns = [column(DataType::Int64)];
    let heartbeat = serde_json::to_vec(&json!({"schema":{"type":"struct","name":"io.debezium.connector.common.Heartbeat","fields":[{"field":"ts_ms","type":"int64","optional":false}]},"payload":{"ts_ms":123}})).unwrap();
    let convert_control = |topic, value| {
        convert_values(
            &spec(&columns),
            &identity_projection(&columns),
            schema::compile(&columns.to_vec().into()).unwrap(),
            [(topic, value)],
        )
    };
    assert!(convert_control(Some("source.public.events"), None).is_err());
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
    let notification = serde_json::to_vec(&notification("COMPLETED")).unwrap();
    assert!(
        convert_control(
            Some("__dogpaddle-notification.source"),
            Some(notification.as_slice())
        )
        .is_err()
    );
}

#[test]
fn postgres_cdc_conversion_rejects_overflow_and_special_temporal_values() {
    for (data_type, value) in [
        (DataType::Int16, json!(32768)),
        (DataType::Int32, json!(2_147_483_648_u64)),
        (DataType::Int64, json!(u64::MAX)),
        (DataType::Float32, json!(1e100)),
        (DataType::Boolean, json!("true")),
        (DataType::Date32, json!(-2_147_483_648_i64)),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            json!(9_223_372_036_825_200_000_i64),
        ),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            json!(-9_223_372_036_832_400_000_i64),
        ),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            json!("infinity"),
        ),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            json!("1970-01-01T00:00:00.000000001Z"),
        ),
        (DataType::Binary, json!("bad base64")),
    ] {
        let columns = [column(data_type.clone())];
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
fn postgres_cdc_preserves_values_that_equal_debeziums_default_toast_placeholder() {
    for (data_type, value) in [
        (DataType::Utf8, json!("__debezium_unavailable_value")),
        (
            DataType::Binary,
            json!(BASE64_STANDARD.encode(b"__debezium_unavailable_value")),
        ),
    ] {
        let columns = [column(data_type.clone())];
        assert!(
            convert(
                &columns,
                &[envelope(&columns, "c", Value::Null, json!({"value":value}))]
            )
            .is_ok(),
            "{data_type:?}"
        );
    }
}

#[test]
fn postgres_cdc_float_preserves_signed_zero_nan_and_infinities() {
    for data_type in [DataType::Float32, DataType::Float64] {
        let columns = [column(data_type.clone())];
        for (value, expected) in [
            (json!(-0.0), -0.0_f64),
            (json!("NaN"), f64::NAN),
            (json!("Infinity"), f64::INFINITY),
            (json!("-Infinity"), f64::NEG_INFINITY),
        ] {
            let change = inserted(&columns, json!({"value":value}));
            let array = change.records().column(0);
            let actual = match data_type {
                DataType::Float32 => f64::from(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .value(0),
                ),
                DataType::Float64 => array
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

#[test]
fn postgres_cdc_empty_completion_keeps_wal_in_same_or_later_conversion() {
    let columns = [column(DataType::Int64)];
    let projection = [0];
    let output_schema = projected_schema(&columns, &projection).unwrap();
    let source = super::runtime::PostgresSource::for_test(&spec(&columns), &projection);
    let completion = serde_json::to_vec(&notification("COMPLETED")).unwrap();
    let wal =
        serde_json::to_vec(&envelope(&columns, "c", Value::Null, json!({"value":19}))).unwrap();
    let completed = crate::operation::scan::cdc_convert::convert_values(
        &source,
        output_schema.clone(),
        [(
            Some("__dogpaddle-notification.source"),
            Some(completion.as_slice()),
        )],
        Some(SnapshotProgress::default()),
    )
    .unwrap();
    assert!(completed.sealed);
    assert!(completed.change.is_none());
    assert_ne!(completed.progress, SnapshotProgress::default());
    let later = crate::operation::scan::cdc_convert::convert_values(
        &source,
        output_schema.clone(),
        [(Some("source.public.events"), Some(wal.as_slice()))],
        Some(completed.progress),
    )
    .unwrap();
    assert!(!later.sealed);
    let same = crate::operation::scan::cdc_convert::convert_values(
        &source,
        output_schema.clone(),
        [
            (
                Some("__dogpaddle-notification.source"),
                Some(completion.as_slice()),
            ),
            (Some("source.public.events"), Some(wal.as_slice())),
        ],
        Some(SnapshotProgress::default()),
    )
    .unwrap();
    assert!(same.sealed);
    let expected = arrow_array::RecordBatch::try_new(
        output_schema,
        vec![Arc::new(Int64Array::from(vec![19]))],
    )
    .unwrap();
    let later = later.change.unwrap();
    let same = same.change.unwrap();
    assert_eq!(later.records(), &expected);
    assert_eq!(same.records(), &expected);
    assert_eq!(later.diffs(), &Int64Array::from(vec![1]));
    assert_eq!(same.diffs(), &Int64Array::from(vec![1]));
}

#[test]
fn postgres_cdc_mixed_capture_preserves_complete_output_across_every_delivery_cut() {
    use arrow_select::concat::concat_batches;

    let columns = [column(DataType::Int64)];
    let projection = [0];
    let output_schema = projected_schema(&columns, &projection).unwrap();
    let source = super::runtime::PostgresSource::for_test(&spec(&columns), &projection);
    let records = [
        (
            "source.public.events",
            snapshot(&columns, "true", json!({"value":1})),
        ),
        (
            "source.public.events",
            snapshot(&columns, "last", json!({"value":2})),
        ),
        (
            "source.public.events",
            envelope(&columns, "c", Value::Null, json!({"value":3})),
        ),
        (
            "source.public.events",
            envelope(&columns, "u", json!({"value":3}), json!({"value":4})),
        ),
        ("__debezium-heartbeat.source", heartbeat()),
        ("__dogpaddle-notification.source", notification("COMPLETED")),
        (
            "source.public.events",
            envelope(&columns, "d", json!({"value":4}), Value::Null),
        ),
    ];
    let bytes = records
        .iter()
        .map(|(_, value)| serde_json::to_vec(value).unwrap())
        .collect::<Vec<_>>();
    let expected = arrow_array::RecordBatch::try_new(
        output_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 3, 4, 4]))],
    )
    .unwrap();
    let expected_diffs = Int64Array::from(vec![1, 1, 1, -1, 1, -1]);
    // The six gaps define every one of the 64 possible ordered Delivery cuts.
    for cuts in 0_u8..64 {
        let mut progress = Some(SnapshotProgress::default());
        let mut start = 0;
        let mut seals = 0;
        let mut batches = Vec::new();
        let mut diffs = Vec::new();
        for end in 1..=records.len() {
            if end != records.len() && cuts & (1 << (end - 1)) == 0 {
                continue;
            }
            let captured = crate::operation::scan::cdc_convert::convert_values(
                &source,
                output_schema.clone(),
                records[start..end]
                    .iter()
                    .zip(&bytes[start..end])
                    .map(|((topic, _), bytes)| (Some(*topic), Some(bytes.as_slice()))),
                progress,
            )
            .unwrap();
            if captured.sealed {
                seals += 1;
                // The ACK of this whole Delivery changes the next poll to streaming.
                progress = None;
            } else if progress.is_some() {
                progress = Some(captured.progress);
            }
            if let Some(change) = captured.change {
                batches.push(change.records().clone());
                diffs.extend_from_slice(change.diffs().values());
            }
            start = end;
        }
        assert_eq!(seals, 1, "cuts={cuts:06b}");
        assert!(progress.is_none(), "cuts={cuts:06b}");
        assert_eq!(start, records.len());
        assert_eq!(
            concat_batches(&output_schema, &batches).unwrap(),
            expected,
            "cuts={cuts:06b}"
        );
        assert_eq!(Int64Array::from(diffs), expected_diffs, "cuts={cuts:06b}");
    }
}
