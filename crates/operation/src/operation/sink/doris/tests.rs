use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::{
    config::{DorisSinkConfig, DorisTargetSpec},
    definition::DorisSinkDefinition,
    row::DorisRowCodec,
    schema::DorisLayout,
};
use crate::{decode_definition, encode_definition};

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
    let codec = DorisRowCodec::new(DorisLayout::try_new(schema).unwrap());
    let row = codec.encode_row(&batch, 0).unwrap();
    assert_eq!(row.values[0], mysql::Value::Int(7));
    assert_eq!(row.values[1], mysql::Value::NULL);
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
