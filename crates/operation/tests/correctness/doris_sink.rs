use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    operation::sink::{
        DorisSinkConfig, DorisSinkDefinition, DorisSinkError, DorisSinkSchemaError, DorisTargetSpec,
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store, StoreSetup};

use super::support::{TestStore, construct_checked_with_resource};

fn construct_checked(
    definition: &(impl Clone + Into<OperationDefinition>),
    inputs: &[SchemaRef],
) -> Result<Option<SchemaRef>, OperationBindError> {
    construct_checked_with_resource(definition, inputs, &RuntimeResource::new(config("shop")))
}

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
    let mut expected = Vec::new();
    expected.extend_from_slice(br#"{"doris_sink":{"sink_id":"orders_sink","database":"shop","table":"orders_materialized","cluster_id":42}}"#);
    expected
}

#[test]
fn doris_sink_has_canonical_non_secret_variant_bytes() {
    let definition = definition();
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    assert_eq!(encoded, literal_definition_bytes());
    assert_eq!(
        OperationDefinition::from(definition.clone()).kind(),
        OperationKind::Sink(NonZeroU32::MIN)
    );
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
        encoded
    );
    let printable = String::from_utf8(encoded).unwrap();
    for secret in [PASSWORD, "127.0.0.1", "sink_user"] {
        assert!(!printable.contains(secret));
    }
}

#[test]
fn doris_sink_declares_buffered_state_and_exact_runtime_resource() {
    let definition = definition();
    let binding = construct_checked(&definition, &[schema()]).unwrap();
    assert!(binding.as_ref().is_none());
    assert!(matches!(
        OperationDefinition::from(definition.clone()).validate_resource(&RuntimeResource::none()),
        Err(OperationSetupError::MissingRuntimeResource)
    ));
    assert!(matches!(
        OperationDefinition::from(definition.clone())
            .validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::WrongRuntimeResource)
    ));
    assert!(
        OperationDefinition::from(definition.clone())
            .validate_resource(&RuntimeResource::new(config("shop")))
            .is_ok()
    );

    let root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (operation, output) = OperationDefinition::from(definition.clone())
        .construct(
            &[schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config("shop")),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
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
fn doris_sink_validates_schema_target_and_decoded_materialization_offline() {
    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "x".repeat(65),
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&definition(), &[invalid_schema])
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
    let decoded = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
        &literal_definition_bytes(),
    )
    .unwrap();
    let mut setup = StoreSetup::new();
    let (operation, output) = decoded
        .construct(
            &[schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config("shop")),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let (operation, output) = decoded
        .construct(
            &[schema()],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::new(config("shop")),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    drop(operation);
}

#[test]
fn doris_definition_rejects_every_truncated_prefix() {
    let encoded = literal_definition_bytes();
    for length in 0..encoded.len() {
        assert!(
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded[..length])
                .is_err()
        );
    }
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["sink_id"] = serde_json::json!("");
    let plan: OperationDefinition =
        serde_json::from_value(serde_json::json!({"doris_sink": payload})).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[schema()],
        RuntimeResource::new(config("shop")),
    );
}
