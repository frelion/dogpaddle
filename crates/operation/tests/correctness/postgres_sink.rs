use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    operation::{
        Operation,
        sink::{
            PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkSchemaError,
            PostgresTargetSpec,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store, StoreSetup};

use super::support::{TestStore, construct_checked_with_resource};

fn construct_checked(
    definition: &(impl Clone + Into<OperationDefinition>),
    inputs: &[SchemaRef],
) -> Result<Option<SchemaRef>, OperationBindError> {
    construct_checked_with_resource(definition, inputs, &RuntimeResource::new(config()))
}

const PASSWORD: &str = "do-not-persist-postgres-sink-password";

fn target() -> PostgresTargetSpec {
    PostgresTargetSpec::try_new(
        "orders_sink",
        "shop",
        "public",
        "orders_materialized",
        "123456789",
        42,
    )
    .unwrap()
}

fn definition() -> PostgresSinkDefinition {
    PostgresSinkDefinition::try_new(target()).unwrap()
}

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn config() -> PostgresSinkConfig {
    PostgresSinkConfig::new_unencrypted("127.0.0.1", 1, "shop", "sink_user", PASSWORD).unwrap()
}

fn mismatched_config() -> PostgresSinkConfig {
    PostgresSinkConfig::new_unencrypted("127.0.0.1", 1, "another_database", "sink_user", PASSWORD)
        .unwrap()
}

fn input_change() -> Change {
    let records =
        RecordBatch::try_new(input_schema(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = Vec::new();
    expected.extend_from_slice(br#"{"postgres_sink":{"sink_id":"orders_sink","database":"shop","schema":"public","table":"orders_materialized","system_identifier":"123456789","database_oid":42}}"#);
    expected
}

#[test]
fn postgres_sink_definition_has_canonical_non_secret_variant_bytes() {
    let definition = definition();
    assert_eq!(
        OperationDefinition::from(definition.clone()).kind(),
        OperationKind::Sink(NonZeroU32::MIN)
    );
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    let expected = literal_definition_bytes();
    assert_eq!(encoded, expected);

    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Sink(NonZeroU32::MIN));
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
        encoded
    );
    let printable = String::from_utf8(encoded.clone()).unwrap();
    for secret in [PASSWORD, "127.0.0.1", "sink_user"] {
        assert!(!printable.contains(secret));
    }

    let mut noncanonical = encoded;
    noncanonical.push(0);
    assert!(
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&noncanonical).is_err()
    );
}

#[test]
fn postgres_sink_reopens_an_idle_event_position_without_network_io() {
    // Shared buffered state v1: empty Ready retains absolute event position 7.
    let mut encoded_state = vec![1, 1];
    for _ in 0..3 {
        encoded_state.extend(7_u64.to_be_bytes());
    }
    encoded_state.extend(0_u64.to_be_bytes());
    let store_root = TestStore::new();
    let definition = definition();
    let mut setup = StoreSetup::new();
    let (operation, output) = OperationDefinition::from(definition.clone())
        .construct(
            &[input_schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let transactions = setup.commit(store_root.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(store_root.path()).unwrap();
    let state: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    state
        .access(transaction.access())
        .unwrap()
        .set(&encoded_state)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    for _ in 0..2 {
        let store = Store::open(store_root.path()).unwrap();
        let decoded = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
            &literal_definition_bytes(),
        )
        .unwrap();
        let state: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
        let (operation, output) = decoded
            .construct(
                &[input_schema()],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::new(config()),
            )
            .unwrap()
            .into_parts();
        assert!(output.is_none());
        let (_, reads) = store.into_transactions().split();
        let Operation::Sink(mut sink) = operation else {
            panic!("expected sink");
        };
        let snapshot = reads.begin();
        assert!(sink.load(snapshot.access()).unwrap().is_none());
        assert_eq!(
            state.read(snapshot.access()).unwrap().get().unwrap(),
            Some(encoded_state.clone())
        );
    }
}

#[test]
fn postgres_sink_decoder_rejects_every_truncated_payload_prefix() {
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition().into())
            .unwrap();
    for length in 0..encoded.len() {
        assert!(
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded[..length])
                .is_err(),
            "accepted truncated PostgreSQL sink definition prefix {length}/{}",
            encoded.len()
        );
    }
}

#[test]
fn postgres_sink_declares_exact_buffered_state_and_runtime_resource() {
    let definition = definition();
    assert_eq!(
        OperationDefinition::from(definition.clone()).kind(),
        OperationKind::Sink(NonZeroU32::MIN)
    );
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
            .validate_resource(&RuntimeResource::new(config()))
            .is_ok()
    );

    let store_root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (operation, output) = OperationDefinition::from(definition.clone())
        .construct(
            &[input_schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let transactions = setup.commit(store_root.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(store_root.path()).unwrap();
    let state: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
    store
        .open_data::<OrderedMap<u64, Vec<u8>>>("operation/sink.buffer")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    transaction.commit().unwrap();
}

#[test]
fn postgres_sink_accepts_its_schema_and_rejects_invalid_schema_and_target_specs() {
    let target = target();
    assert_eq!(target.sink_id(), "orders_sink");
    assert_eq!(target.database(), "shop");
    assert_eq!(target.schema(), "public");
    assert_eq!(target.table(), "orders_materialized");
    assert_eq!(target.system_identifier(), "123456789");
    assert_eq!(target.database_oid(), 42);
    assert!(
        construct_checked(&definition(), &[input_schema()])
            .unwrap()
            .is_none()
    );

    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "x".repeat(64),
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&definition(), &[invalid_schema])
    else {
        panic!("a PostgreSQL sink field longer than 63 bytes unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<PostgresSinkSchemaError>(),
        Some(PostgresSinkSchemaError::InvalidFieldName { field: 0, name })
            if name.len() == 64
    ));

    let system_column = Arc::new(Schema::new(vec![Field::new(
        "ctid",
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&definition(), &[system_column])
    else {
        panic!("the exact PostgreSQL system column ctid unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<PostgresSinkSchemaError>(),
        Some(PostgresSinkSchemaError::SystemColumnCollision { field: 0, name })
            if name == "ctid"
    ));

    for invalid in [
        PostgresTargetSpec::try_new("Bad-ID", "shop", "public", "output", "123", 42),
        PostgresTargetSpec::try_new("sink", "shop", "public", "output", "0", 42),
        PostgresTargetSpec::try_new("sink", "shop", "public", "output", "123", 0),
    ] {
        assert!(matches!(
            invalid,
            Err(PostgresSinkError::InvalidSpec { .. })
        ));
    }
}

#[test]
fn postgres_sink_load_is_offline_and_target_check_precedes_initialization_intent() {
    let root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (operation, _) = OperationDefinition::from(definition())
        .construct(
            &[input_schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(mismatched_config()),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
    let Operation::Sink(mut sink) = operation else {
        panic!("expected sink");
    };
    let pending = sink.load(reads.begin().access()).unwrap().unwrap();
    let Err(error) = sink.prepare_initialize(&pending) else {
        panic!("mismatched target accepted");
    };
    assert!(matches!(
        error.downcast_ref::<PostgresSinkError>(),
        Some(PostgresSinkError::DatabaseMismatch)
    ));
    let txn = writes.begin();
    assert!(!sink.try_enqueue(txn.access(), &input_change()).unwrap());
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["sink_id"] = serde_json::json!("");
    let plan: OperationDefinition =
        serde_json::from_value(serde_json::json!({"postgres_sink": payload})).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[input_schema()],
        RuntimeResource::new(config()),
    );
}
