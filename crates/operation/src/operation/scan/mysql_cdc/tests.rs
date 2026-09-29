use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray,
};
use arrow_schema::Schema;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_change::Change;
use serde_json::{Value, json};

use super::{
    MySqlCdcScanError, MySqlCdcScanSpec, MySqlColumn, MySqlType,
    convert::{SnapshotProgress, convert_snapshot_values, convert_values},
    schema,
};
use crate::operation::scan::cdc_runtime::Captured;

fn column(data_type: MySqlType) -> MySqlColumn {
    MySqlColumn::new("value", data_type, true)
}

fn spec(columns: &[MySqlColumn]) -> MySqlCdcScanSpec {
    MySqlCdcScanSpec {
        engine_name: "source".to_owned(),
        database: "shop".to_owned(),
        table: "events".to_owned(),
        server_uuid: "01234567-89ab-cdef-0123-456789abcdef".to_owned(),
        table_id: 43,
        columns: columns.to_vec(),
    }
}

fn identity_projection(columns: &[MySqlColumn]) -> Vec<u32> {
    (0..u32::try_from(columns.len()).unwrap()).collect()
}

fn envelope(columns: &[MySqlColumn], op: &str, before: Value, after: Value) -> Value {
    let fields = columns
        .iter()
        .map(|column| {
            let (literal, logical) = column.data_type().connect_type();
            let mut field =
                json!({"field":column.name(),"type":literal,"optional":column.is_nullable()});
            if let Some(logical) = logical {
                field["name"] = json!(logical);
            }
            if let MySqlType::Decimal { precision, scale } = column.data_type() {
                field["parameters"] = json!({
                    "scale":scale.to_string(),
                    "connect.decimal.precision":precision.to_string(),
                });
            }
            field
        })
        .collect::<Vec<_>>();
    let mut event = json!({
        "schema":{"type":"struct","fields":[
            {"field":"before","type":"struct","optional":true,"fields":fields},
            {"field":"after","type":"struct","optional":true,"fields":fields}
        ]},
        "payload":{
            "source":{"connector":"mysql","db":"shop","table":"events","snapshot":"false"},
            "op":op
        }
    });
    event["payload"]["before"] = before;
    event["payload"]["after"] = after;
    event
}

fn convert(columns: &[MySqlColumn], events: &[Value]) -> Result<Option<Change>, MySqlCdcScanError> {
    let projection = identity_projection(columns);
    convert_projected(columns, &projection, events)
}

fn convert_projected(
    columns: &[MySqlColumn],
    projection: &[u32],
    events: &[Value],
) -> Result<Option<Change>, MySqlCdcScanError> {
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
            .map(|bytes| (Some("source.shop.events"), Some(bytes.as_slice()))),
    )
}

fn projected_schema(
    columns: &[MySqlColumn],
    projection: &[u32],
) -> Result<Arc<Schema>, MySqlCdcScanError> {
    let full_schema = schema::compile(columns)?;
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
        "schema":{"type":"struct","name":"io.debezium.connector.common.Heartbeat","fields":[{"field":"ts_ms","type":"int64","optional":false}]},
        "payload":{"ts_ms":123},
    })
}

fn notification(kind: &str) -> Value {
    json!({
        "schema":{"type":"struct","name":"io.debezium.pipeline.notification.Notification"},
        "payload":{"aggregate_type":"Initial Snapshot","type":kind},
    })
}

fn snapshot(
    columns: &[MySqlColumn],
    events: &[Value],
) -> Result<Captured<SnapshotProgress>, MySqlCdcScanError> {
    snapshot_after(columns, events, SnapshotProgress::default())
}

fn snapshot_after(
    columns: &[MySqlColumn],
    events: &[Value],
    progress: SnapshotProgress,
) -> Result<Captured<SnapshotProgress>, MySqlCdcScanError> {
    let projection = identity_projection(columns);
    snapshot_after_projected(columns, &projection, events, progress)
}

fn snapshot_after_projected(
    columns: &[MySqlColumn],
    projection: &[u32],
    events: &[Value],
    progress: SnapshotProgress,
) -> Result<Captured<SnapshotProgress>, MySqlCdcScanError> {
    let bytes = events
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    convert_snapshot_values(
        &spec(columns),
        projection,
        projected_schema(columns, projection)?,
        bytes.iter().enumerate().map(|(index, bytes)| {
            let topic = match events[index]["schema"]["name"].as_str() {
                Some("io.debezium.connector.common.Heartbeat") => "__debezium-heartbeat.source",
                Some("io.debezium.pipeline.notification.Notification") => {
                    "__dogpaddle-notification.source"
                }
                _ => "source.shop.events",
            };
            (Some(topic), Some(bytes.as_slice()))
        }),
        progress,
    )
}

#[test]
fn mysql_cdc_conversion_preserves_insert_update_delete_event_order() {
    let columns = [column(MySqlType::Int64)];
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
fn mysql_cdc_projection_validates_full_rows_and_preserves_zero_column_row_count() {
    let columns = [
        MySqlColumn::new("id", MySqlType::Int64, false),
        MySqlColumn::new("payload", MySqlType::Text, false),
        MySqlColumn::new("unused", MySqlType::Binary, false),
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
fn mysql_cdc_projection_rejects_bad_unselected_values_in_streaming_and_capture() {
    let columns = [
        MySqlColumn::new("id", MySqlType::Int64, false),
        MySqlColumn::new("unused", MySqlType::Int32, false),
    ];
    let projections: [&[u32]; 2] = [&[0], &[]];

    for projection in projections {
        let row = json!({"id":7,"unused":"bad"});
        let streaming = envelope(&columns, "c", Value::Null, row.clone());
        assert!(convert_projected(&columns, projection, &[streaming]).is_err());

        let mut capture_event = envelope(&columns, "r", Value::Null, row);
        capture_event["payload"]["source"]["snapshot"] = json!("last");
        assert!(
            snapshot_after_projected(
                &columns,
                projection,
                &[capture_event],
                SnapshotProgress::default(),
            )
            .is_err()
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn mysql_cdc_conversion_preserves_every_supported_type_and_null() {
    let columns = [
        MySqlColumn::new("tiny", MySqlType::Int16, true),
        MySqlColumn::new("small", MySqlType::Int16, true),
        MySqlColumn::new("integer", MySqlType::Int32, true),
        MySqlColumn::new("big", MySqlType::Int64, true),
        MySqlColumn::new("double", MySqlType::Float64, true),
        MySqlColumn::new("text", MySqlType::Text, true),
        MySqlColumn::new("binary", MySqlType::Binary, true),
        MySqlColumn::new(
            "decimal",
            MySqlType::Decimal {
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
            "tiny":-128,"small":i16::MIN,"integer":i32::MAX,"big":i64::MAX,
            "double":-1.25,"text":"雪🦀","binary":"AP9/",
            "decimal":BASE64_STANDARD.encode(unscaled.to_be_bytes()),
        }),
    );
    let nulls = columns
        .iter()
        .map(|column| (column.name().to_owned(), Value::Null))
        .collect::<serde_json::Map<_, _>>();
    let second = envelope(&columns, "c", Value::Null, Value::Object(nulls));
    let change = convert(&columns, &[first, second]).unwrap().unwrap();
    let arrays = change.records().columns();
    assert_eq!(
        arrays[0]
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -128
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
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-1.25_f64).to_bits()
    );
    assert_eq!(
        arrays[5]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "雪🦀"
    );
    assert_eq!(
        arrays[6]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255, 127]
    );
    assert_eq!(
        arrays[7]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        unscaled
    );
    for array in arrays {
        assert!(array.is_null(1));
    }
}

#[test]
fn mysql_cdc_conversion_rejects_snapshot_truncate_schema_change_and_wrong_metadata() {
    let columns = [column(MySqlType::Int64)];
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
    for property in ["db", "table", "connector", "snapshot"] {
        let mut event = envelope(&columns, "c", Value::Null, json!({"value":1}));
        event["payload"]["source"][property] = json!("wrong");
        assert!(convert(&columns, &[event]).is_err());
    }
    let schema_change = serde_json::to_vec(&json!({"schema":{},"payload":{}})).unwrap();
    assert!(
        convert_values(
            &spec(&columns),
            &identity_projection(&columns),
            schema::compile(&columns).unwrap(),
            [(Some("source"), Some(schema_change.as_slice()))],
        )
        .is_err()
    );
}

#[test]
fn mysql_cdc_conversion_validates_exact_schema_and_identified_heartbeat() {
    let columns = [column(MySqlType::Decimal {
        precision: 4,
        scale: 2,
    })];
    let mut event = envelope(&columns, "c", Value::Null, json!({"value":"AA=="}));
    event["schema"]["fields"][0]["fields"][0]["parameters"]["scale"] = json!("3");
    assert!(convert(&columns, &[event]).is_err());

    let heartbeat = serde_json::to_vec(&heartbeat()).unwrap();
    assert!(
        convert_values(
            &spec(&columns),
            &identity_projection(&columns),
            schema::compile(&columns).unwrap(),
            [(
                Some("__debezium-heartbeat.source"),
                Some(heartbeat.as_slice())
            )],
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn mysql_cdc_snapshot_uses_explicit_completion_and_supports_empty_tables() {
    let columns = [column(MySqlType::Int64)];
    let mut row = envelope(&columns, "r", Value::Null, json!({"value":7}));
    row["payload"]["source"]["snapshot"] = json!("last");

    let continuing = snapshot(&columns, std::slice::from_ref(&row)).unwrap();
    assert!(!continuing.sealed);
    assert_eq!(continuing.change.unwrap().diffs().values(), &[1]);

    let complete = snapshot_after(
        &columns,
        &[heartbeat(), notification("COMPLETED")],
        continuing.progress,
    )
    .unwrap();
    assert!(complete.sealed);
    assert!(complete.change.is_none());

    let empty = snapshot(&columns, &[notification("COMPLETED")]).unwrap();
    assert!(empty.sealed);
    assert!(empty.change.is_none());

    assert!(
        !snapshot(&columns, &[heartbeat(), notification("STARTED")])
            .unwrap()
            .sealed
    );
    assert!(snapshot(&columns, &[notification("COMPLETED"), row.clone()]).is_err());
    assert!(
        snapshot(
            &columns,
            &[notification("COMPLETED"), notification("COMPLETED")]
        )
        .is_err()
    );
    for kind in ["ABORTED", "SKIPPED", "UNKNOWN"] {
        assert!(snapshot(&columns, &[notification(kind)]).is_err());
    }
    let mut malformed = notification("COMPLETED");
    malformed["payload"]["aggregate_type"] = json!("Other");
    assert!(snapshot(&columns, &[malformed]).is_err());
    row["payload"]["source"]["snapshot"] = json!("false");
    assert!(snapshot(&columns, &[row]).is_err());
}

#[test]
fn mysql_cdc_snapshot_accepts_debezium_collection_boundary_markers() {
    let columns = [column(MySqlType::Int64)];
    let mut first = envelope(&columns, "r", Value::Null, json!({"value":1}));
    first["payload"]["source"]["snapshot"] = json!("first");
    let progress = snapshot(&columns, &[first]).unwrap().progress;

    let mut last = envelope(&columns, "r", Value::Null, json!({"value":2}));
    last["payload"]["source"]["snapshot"] = json!("last_in_data_collection");
    let progress = snapshot_after(&columns, &[last], progress)
        .unwrap()
        .progress;

    assert!(
        snapshot_after(&columns, &[notification("COMPLETED")], progress)
            .unwrap()
            .sealed
    );
}

#[test]
fn mysql_cdc_snapshot_progress_crosses_delivery_boundaries() {
    let columns = [column(MySqlType::Int64)];
    let mut first_row = envelope(&columns, "r", Value::Null, json!({"value":1}));
    first_row["payload"]["source"]["snapshot"] = json!("true");
    let first = snapshot(&columns, &[first_row]).unwrap();
    assert!(!first.sealed);

    assert!(snapshot_after(&columns, &[notification("COMPLETED")], first.progress,).is_err());

    let mut last = envelope(&columns, "r", Value::Null, json!({"value":2}));
    last["payload"]["source"]["snapshot"] = json!("last");
    let second = snapshot_after(&columns, &[last], first.progress).unwrap();
    let completed =
        snapshot_after(&columns, &[notification("COMPLETED")], second.progress).unwrap();
    assert!(completed.sealed);
    assert!(completed.change.is_none());
}

#[test]
fn mysql_cdc_float_preserves_signed_zero_nan_and_infinities() {
    let columns = [column(MySqlType::Float64)];
    for (value, expected) in [
        (json!(-0.0), -0.0_f64),
        (json!("NaN"), f64::NAN),
        (json!("Infinity"), f64::INFINITY),
        (json!("-Infinity"), f64::NEG_INFINITY),
    ] {
        let change = convert(
            &columns,
            &[envelope(&columns, "c", Value::Null, json!({"value":value}))],
        )
        .unwrap()
        .unwrap();
        let actual = change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        if expected.is_nan() {
            assert!(actual.is_nan());
        } else {
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
    }
}
