use arrow_array::{
    Array, BinaryArray, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray,
};
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_change::Change;
use serde_json::{Value, json};

use super::{
    MySqlCdcScanError, MySqlColumn, MySqlType,
    convert::{convert_snapshot_values, convert_values},
    schema,
};

fn column(data_type: MySqlType) -> MySqlColumn {
    MySqlColumn::new("value", data_type, true)
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
    let bytes = events
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    convert_values(
        columns,
        schema::compile(columns)?,
        "source",
        "shop",
        "events",
        bytes
            .iter()
            .map(|bytes| (Some("source.shop.events"), Some(bytes.as_slice()))),
    )
}

fn heartbeat() -> Value {
    json!({
        "schema":{"type":"struct","name":"io.debezium.connector.common.Heartbeat","fields":[{"field":"ts_ms","type":"int64","optional":false}]},
        "payload":{"ts_ms":123},
    })
}

fn snapshot(
    columns: &[MySqlColumn],
    events: &[Value],
) -> Result<super::convert::SnapshotDelivery, MySqlCdcScanError> {
    let bytes = events
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap())
        .collect::<Vec<_>>();
    convert_snapshot_values(
        columns,
        schema::compile(columns)?,
        "source",
        "shop",
        "events",
        bytes.iter().enumerate().map(|(index, bytes)| {
            let topic =
                if events[index]["schema"]["name"] == "io.debezium.connector.common.Heartbeat" {
                    "__debezium-heartbeat.source"
                } else {
                    "source.shop.events"
                };
            (Some(topic), Some(bytes.as_slice()))
        }),
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
            &columns,
            schema::compile(&columns).unwrap(),
            "source",
            "shop",
            "events",
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
            &columns,
            schema::compile(&columns).unwrap(),
            "source",
            "shop",
            "events",
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
fn mysql_cdc_snapshot_uses_a_terminal_last_heartbeat_and_supports_empty_tables() {
    let columns = [column(MySqlType::Int64)];
    let mut row = envelope(&columns, "r", Value::Null, json!({"value":7}));
    row["payload"]["source"]["snapshot"] = json!("last");

    let continuing = snapshot(&columns, std::slice::from_ref(&row)).unwrap();
    assert!(!continuing.complete);
    assert_eq!(continuing.change.unwrap().diffs().values(), &[1]);

    let complete = snapshot(&columns, &[row.clone(), heartbeat()]).unwrap();
    assert!(complete.complete);
    assert_eq!(complete.change.unwrap().diffs().values(), &[1]);

    let empty = snapshot(&columns, &[heartbeat()]).unwrap();
    assert!(empty.complete);
    assert!(empty.change.is_none());

    assert!(snapshot(&columns, &[]).is_err());
    assert!(snapshot(&columns, &[heartbeat(), row.clone()]).is_err());
    assert!(snapshot(&columns, &[heartbeat(), heartbeat()]).is_err());
    row["payload"]["source"]["snapshot"] = json!("false");
    assert!(snapshot(&columns, &[row]).is_err());
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
