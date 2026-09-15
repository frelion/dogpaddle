use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

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
fn row_codec_preserves_nulls_and_logical_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    let codec = ClickHouseRowCodec::new(ClickHouseLayout::try_new(schema).unwrap());
    let row = codec.encode_row(&batch, 0).unwrap();
    assert_eq!(row.values[0], serde_json::json!(7));
    assert_eq!(row.values[1], serde_json::Value::Null);
    assert_eq!(row.hash.len(), 32);
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
    let codec = ClickHouseRowCodec::new(ClickHouseLayout::try_new(schema).unwrap());
    assert!(codec.encode_row(&batch, 1).is_err());
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
