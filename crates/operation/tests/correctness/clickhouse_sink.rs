use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    create_operation, decode_definition, encode_definition, open_operation,
    operation::sink::{
        ClickHouseSinkConfig, ClickHouseSinkDefinition, ClickHouseSinkError,
        ClickHouseSinkSchemaError, ClickHouseTargetSpec,
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store};

use super::support::TestStore;

const DATABASE_UUID: &str = "12345678-1234-1234-1234-123456789abc";
const PASSWORD: &str = "do-not-persist-clickhouse-password";

fn target() -> ClickHouseTargetSpec {
    ClickHouseTargetSpec::try_new("orders_sink", "shop", "orders_materialized", DATABASE_UUID)
        .unwrap()
}

fn definition() -> ClickHouseSinkDefinition {
    ClickHouseSinkDefinition::try_new(target()).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn config(database: &str) -> ClickHouseSinkConfig {
    ClickHouseSinkConfig::new_unencrypted("127.0.0.1", 1, database, "sink_user", PASSWORD).unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x13".to_vec();
    expected.extend_from_slice(br#"{"sink_id":"orders_sink","database":"shop","table":"orders_materialized","database_uuid":"12345678-1234-1234-1234-123456789abc"}"#);
    expected
}

#[test]
fn clickhouse_sink_has_canonical_non_secret_tag_19_bytes() {
    let definition = definition();
    let encoded = encode_definition(&definition);
    assert_eq!(encoded, literal_definition_bytes());
    assert_eq!(definition.persistence_tag(), 19);
    assert_eq!(definition.kind(), OperationKind::Sink(NonZeroU32::MIN));
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
    let printable = String::from_utf8(encoded).unwrap();
    for secret in [PASSWORD, "127.0.0.1", "sink_user"] {
        assert!(!printable.contains(secret));
    }
}

#[test]
fn clickhouse_sink_declares_buffered_state_and_exact_runtime_resource() {
    let definition = definition();
    let binding = (&definition as &dyn OperationDefinition)
        .bind(&[schema()])
        .unwrap();
    assert!(binding.output_schema().is_none());
    assert!(matches!(
        binding.validate_resource(&RuntimeResource::none()),
        Err(OperationSetupError::MissingRuntimeResource)
    ));
    assert!(matches!(
        binding.validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::WrongRuntimeResource)
    ));
    assert!(
        binding
            .validate_resource(&RuntimeResource::new(config("shop")))
            .is_ok()
    );

    let root = TestStore::new();
    let mut setup = Store::setup(root.path()).unwrap();
    let operation = create_operation(
        (&definition as &dyn OperationDefinition)
            .bind(&[schema()])
            .unwrap(),
        &mut setup,
        "operation",
        RuntimeResource::new(config("shop")),
    )
    .unwrap();
    let transactions = setup.commit(|_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    store
        .open_data::<Cell<Vec<u8>>>("operation/sink.control")
        .unwrap();
    store
        .open_data::<OrderedMap<u64, Vec<u8>>>("operation/sink.buffer")
        .unwrap();
}

#[test]
fn clickhouse_sink_validates_schema_target_and_decoded_materialization_offline() {
    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "x".repeat(256),
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        (&definition() as &dyn OperationDefinition).bind(&[invalid_schema])
    else {
        panic!("an oversized ClickHouse field unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<ClickHouseSinkSchemaError>(),
        Some(ClickHouseSinkSchemaError::InvalidFieldName { field: 0, name }) if name.len() == 256
    ));
    assert!(matches!(
        ClickHouseTargetSpec::try_new("Bad-ID", "shop", "output", DATABASE_UUID),
        Err(ClickHouseSinkError::InvalidSpec { .. })
    ));

    let root = TestStore::new();
    let decoded = decode_definition(&literal_definition_bytes()).unwrap();
    let mut setup = Store::setup(root.path()).unwrap();
    let operation = create_operation(
        decoded.bind(&[schema()]).unwrap(),
        &mut setup,
        "operation",
        RuntimeResource::new(config("shop")),
    )
    .unwrap();
    let transactions = setup.commit(|_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let operation = open_operation(
        decoded.bind(&[schema()]).unwrap(),
        &store,
        "operation",
        RuntimeResource::new(config("shop")),
    )
    .unwrap();
    drop(operation);
}

#[test]
fn clickhouse_definition_rejects_every_truncated_prefix() {
    let encoded = literal_definition_bytes();
    for length in 0..encoded.len() {
        assert!(decode_definition(&encoded[..length]).is_err());
    }
}
