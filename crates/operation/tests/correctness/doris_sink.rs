use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationBindError, OperationDefinition, OperationKind,
    RuntimeResource, decode_definition, encode_definition,
    operation::sink::{
        DorisSinkConfig, DorisSinkDefinition, DorisSinkError, DorisSinkSchemaError, DorisTargetSpec,
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store};

use super::support::TestStore;

const PASSWORD: &str = "do-not-persist-doris-password";

fn target() -> DorisTargetSpec {
    DorisTargetSpec::try_new("orders_sink", "shop", "orders_materialized", 42).unwrap()
}

fn definition() -> DorisSinkDefinition {
    DorisSinkDefinition::try_new(target()).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn config(database: &str) -> DorisSinkConfig {
    DorisSinkConfig::new_unencrypted("127.0.0.1", 1, database, "sink_user", PASSWORD).unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x12".to_vec();
    expected.extend_from_slice(br#"{"sink_id":"orders_sink","database":"shop","table":"orders_materialized","cluster_id":42}"#);
    expected
}

#[test]
fn doris_sink_has_canonical_non_secret_tag_18_bytes() {
    let definition = definition();
    let encoded = encode_definition(&definition);
    assert_eq!(encoded, literal_definition_bytes());
    assert_eq!(definition.persistence_tag(), 18);
    assert_eq!(definition.kind(), OperationKind::Sink(NonZeroU32::MIN));
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
    let printable = String::from_utf8(encoded).unwrap();
    for secret in [PASSWORD, "127.0.0.1", "sink_user"] {
        assert!(!printable.contains(secret));
    }
}

#[test]
fn doris_sink_declares_buffered_state_and_exact_runtime_resource() {
    let definition = definition();
    assert_eq!(
        definition
            .data()
            .iter()
            .map(dogpaddle_operation::DataDeclaration::name)
            .collect::<Vec<_>>(),
        ["sink.control", "sink.buffer"]
    );
    let binding = (&definition as &dyn OperationDefinition)
        .bind(&[schema()])
        .unwrap();
    assert!(binding.output_schema().is_none());
    assert!(matches!(
        binding.validate_resource(&RuntimeResource::none()),
        Err(MaterializeError::MissingRuntimeResource)
    ));
    assert!(matches!(
        binding.validate_resource(&RuntimeResource::new(42_u64)),
        Err(MaterializeError::WrongRuntimeResource)
    ));
    assert!(
        binding
            .validate_resource(&RuntimeResource::new(config("shop")))
            .is_ok()
    );

    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    definition.data()[0]
        .create(&mut store, "physical-control")
        .unwrap();
    definition.data()[1]
        .create(&mut store, "physical-buffer")
        .unwrap();
    store
        .open_data::<Cell<Vec<u8>>>("physical-control")
        .unwrap();
    store
        .open_data::<OrderedMap<u64, Vec<u8>>>("physical-buffer")
        .unwrap();
}

#[test]
fn doris_sink_validates_schema_target_and_decoded_materialization_offline() {
    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "x".repeat(65),
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        (&definition() as &dyn OperationDefinition).bind(&[invalid_schema])
    else {
        panic!("an oversized Doris field unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<DorisSinkSchemaError>(),
        Some(DorisSinkSchemaError::InvalidFieldName { field: 0, name }) if name.len() == 65
    ));
    assert!(matches!(
        DorisTargetSpec::try_new("Bad-ID", "shop", "output", 42),
        Err(DorisSinkError::InvalidSpec { .. })
    ));

    let root = TestStore::new();
    let decoded = decode_definition(&literal_definition_bytes()).unwrap();
    let mut store = Store::create(root.path()).unwrap();
    let mut data = DataInstances::new();
    for declaration in decoded.data() {
        data.insert(declaration.create(&mut store, declaration.name()).unwrap())
            .unwrap();
    }
    decoded
        .bind(&[schema()])
        .unwrap()
        .materialize(data, RuntimeResource::new(config("shop")))
        .unwrap();
}

#[test]
fn doris_definition_rejects_every_truncated_prefix() {
    let encoded = literal_definition_bytes();
    for length in 0..encoded.len() {
        assert!(decode_definition(&encoded[..length]).is_err());
    }
}
