use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    decode_definition, encode_definition,
    operation::{
        Action, OperationInput, Turn,
        sink::{
            PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkSchemaError,
            PostgresTargetSpec,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store, StoreSetup};

use super::support::{TestStore, construct_checked_with_resource, rollback_ready};

fn construct_checked(
    definition: &dyn OperationDefinition,
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
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x0c".to_vec();
    expected.extend_from_slice(br#"{"sink_id":"orders_sink","database":"shop","schema":"public","table":"orders_materialized","system_identifier":"123456789","database_oid":42}"#);
    expected
}

#[test]
fn postgres_sink_definition_has_canonical_non_secret_tag_12_bytes() {
    let definition = definition();
    assert_eq!(definition.persistence_tag(), 12);
    assert_eq!(definition.kind(), OperationKind::Sink(NonZeroU32::MIN));
    let encoded = encode_definition(&definition);
    let expected = literal_definition_bytes();
    assert_eq!(encoded, expected);

    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Sink(NonZeroU32::MIN));
    assert_eq!(decoded.persistence_tag(), 12);
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
    let printable = String::from_utf8(encoded.clone()).unwrap();
    for secret in [PASSWORD, "127.0.0.1", "sink_user"] {
        assert!(!printable.contains(secret));
    }

    let mut noncanonical = encoded;
    noncanonical.push(b' ');
    assert!(decode_definition(&noncanonical).is_err());
}

#[test]
fn postgres_sink_reopens_and_decodes_nonempty_relation_state_without_network_io() {
    // Shared buffered state v1: Ready(empty buffer, relation ID 1).
    let mut ready = vec![1, 1, 0];
    ready.extend([0; size_of::<u64>() * 3]);
    ready.extend(1_u64.to_be_bytes());
    let store_root = TestStore::new();
    let definition = definition();
    let mut setup = StoreSetup::new();
    let (operation, output) = (&definition as &dyn OperationDefinition)
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
        .set(&ready)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    for _ in 0..2 {
        let store = Store::open(store_root.path()).unwrap();
        let decoded = decode_definition(&literal_definition_bytes()).unwrap();
        let state: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
        let (mut operation, output) = decoded
            .construct(
                &[input_schema()],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::new(config()),
            )
            .unwrap()
            .into_parts();
        assert!(output.is_none());
        let mut transactions = store.into_transactions();
        let input = input_change();
        let Turn::Ready(prepared) = operation
            .turn(Some(OperationInput {
                port: 0,
                change: &input,
            }))
            .unwrap()
        else {
            panic!("reopened PostgreSQL sink did not prepare state restoration");
        };
        let transaction = transactions.begin();
        let (Action::Commit(None), completion) = prepared.apply(transaction.access()).unwrap()
        else {
            panic!("reopened PostgreSQL sink did not decode its Ready state");
        };
        drop(transaction);
        drop(completion); // Running it would perform target I/O; rollback must not.

        let transaction = transactions.begin();
        assert_eq!(
            state.access(transaction.access()).unwrap().get().unwrap(),
            Some(ready.clone())
        );
        transaction.commit().unwrap();
    }
}

#[test]
fn postgres_sink_decoder_rejects_every_truncated_payload_prefix() {
    let encoded = encode_definition(&definition());
    for length in 0..encoded.len() {
        assert!(
            decode_definition(&encoded[..length]).is_err(),
            "accepted truncated PostgreSQL sink definition prefix {length}/{}",
            encoded.len()
        );
    }
}

#[test]
fn postgres_sink_declares_exact_buffered_state_and_runtime_resource() {
    let definition = definition();
    assert_eq!(definition.kind(), OperationKind::Sink(NonZeroU32::MIN));
    assert!(matches!(
        (&definition as &dyn OperationDefinition).validate_resource(&RuntimeResource::none()),
        Err(OperationSetupError::MissingRuntimeResource)
    ));
    assert!(matches!(
        (&definition as &dyn OperationDefinition).validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::WrongRuntimeResource)
    ));
    assert!(
        (&definition as &dyn OperationDefinition)
            .validate_resource(&RuntimeResource::new(config()))
            .is_ok()
    );

    let store_root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (operation, output) = (&definition as &dyn OperationDefinition)
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
fn postgres_sink_restores_offline_then_checks_target_before_publishing_initialization() {
    let store_root = TestStore::new();
    let definition = definition();
    let mut setup = StoreSetup::new();
    let (operation, output) = (&definition as &dyn OperationDefinition)
        .construct(
            &[input_schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(mismatched_config()),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let transactions = setup.commit(store_root.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(store_root.path()).unwrap();
    let state: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
    let (mut operation, output) = (&definition as &dyn OperationDefinition)
        .construct(
            &[input_schema()],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::new(mismatched_config()),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let mut transactions = store.into_transactions();

    // The endpoint is deliberately unreachable and names another database.
    // Binding, Store construction, typed setup, turn preparation, and
    // transaction application and first completion only restore local state.
    // The next transaction-free turn checks the target before publishing any
    // initialization intent.
    let change = input_change();
    drop(
        operation
            .turn(Some(OperationInput {
                port: 0,
                change: &change,
            }))
            .unwrap(),
    );
    assert!(matches!(
        rollback_ready(
            &mut operation,
            Some(OperationInput {
                port: 0,
                change: &change,
            }),
            &mut transactions,
        )
        .unwrap(),
        Action::Commit(None)
    ));

    let transaction = transactions.begin();
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    transaction.commit().unwrap();

    let Turn::Ready(prepared) = operation
        .turn(Some(OperationInput {
            port: 0,
            change: &change,
        }))
        .unwrap()
    else {
        panic!("a fresh PostgreSQL sink did not prepare local restoration");
    };
    let transaction = transactions.begin();
    let (Action::Commit(None), completion) = prepared.apply(transaction.access()).unwrap() else {
        panic!("a fresh PostgreSQL sink did not commit local restoration");
    };
    transaction.commit().unwrap();
    completion.run().unwrap();

    let Err(error) = operation.turn(Some(OperationInput {
        port: 0,
        change: &change,
    })) else {
        panic!("a mismatched target was accepted before initialization");
    };
    assert!(matches!(
        error.downcast_ref::<PostgresSinkError>(),
        Some(PostgresSinkError::DatabaseMismatch)
    ));

    let transaction = transactions.begin();
    assert!(
        state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_none()
    );
    transaction.commit().unwrap();
}
