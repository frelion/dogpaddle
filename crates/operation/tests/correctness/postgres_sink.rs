use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationBindError, OperationDefinition, OperationKind,
    RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, OperationInput, Turn,
        sink::{
            PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkSchemaError,
            PostgresTargetSpec,
        },
    },
};
use dogpaddle_store::{Cell, Store};

use super::support::{TestStore, rollback_ready};

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

#[test]
fn postgres_sink_definition_has_canonical_non_secret_tag_12_bytes() {
    let encoded = encode_definition(&definition());
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x0c".to_vec();
    expected.extend_from_slice(br#"{"sink_id":"orders_sink","database":"shop","schema":"public","table":"orders_materialized","system_identifier":"123456789","database_oid":42}"#);
    assert_eq!(encoded, expected);

    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Sink(NonZeroU32::MIN));
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
fn postgres_sink_declares_one_state_cell_and_exact_runtime_resource() {
    let definition = definition();
    assert_eq!(definition.kind(), OperationKind::Sink(NonZeroU32::MIN));
    assert_eq!(
        definition
            .data()
            .iter()
            .map(dogpaddle_operation::DataDeclaration::name)
            .collect::<Vec<_>>(),
        ["postgres_sink.state"]
    );

    let binding = (&definition as &dyn OperationDefinition)
        .bind(&[input_schema()])
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
            .validate_resource(&RuntimeResource::new(config()))
            .is_ok()
    );

    let store_root = TestStore::new();
    let mut store = Store::create(store_root.path()).unwrap();
    definition.data()[0]
        .create(&mut store, "physical-state")
        .unwrap();
    let state: Cell<Vec<u8>> = store.open_data("physical-state").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
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
        (&definition() as &dyn OperationDefinition)
            .bind(&[input_schema()])
            .unwrap()
            .output_schema()
            .is_none()
    );

    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "x".repeat(64),
        DataType::Int64,
        false,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        (&definition() as &dyn OperationDefinition).bind(&[invalid_schema])
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
        (&definition() as &dyn OperationDefinition).bind(&[system_column])
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
fn postgres_sink_build_materialize_and_abandoned_first_turn_do_not_connect() {
    let store_root = TestStore::new();
    let definition = definition();
    let mut store = Store::create(store_root.path()).unwrap();
    let mut data = DataInstances::new();
    data.insert(
        definition.data()[0]
            .create(&mut store, "physical-state")
            .unwrap(),
    )
    .unwrap();
    let state: Cell<Vec<u8>> = store.open_data("physical-state").unwrap();

    // The endpoint is deliberately unreachable and names another database.
    // Binding, Store construction, materialization, turn preparation, and
    // transaction application must not inspect or contact PostgreSQL; only a
    // successfully run completion may validate and connect to the target.
    let mut operation = (&definition as &dyn OperationDefinition)
        .bind(&[input_schema()])
        .unwrap()
        .materialize(data, RuntimeResource::new(mismatched_config()))
        .unwrap();
    let mut transactions = store.into_transactions();
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
            operation.as_mut(),
            Some(OperationInput {
                port: 0,
                change: &change,
            }),
            &mut transactions,
        )
        .unwrap(),
        Action::Commit(None)
    ));

    let transaction = transactions.begin().unwrap();
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
        panic!("a fresh PostgreSQL sink did not prepare initialization");
    };
    let transaction = transactions.begin().unwrap();
    let (Action::Commit(None), completion) = prepared.apply(transaction.access()).unwrap() else {
        panic!("a fresh PostgreSQL sink did not persist initialization");
    };
    transaction.commit().unwrap();
    drop(completion);

    let transaction = transactions.begin().unwrap();
    assert!(
        state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_some()
    );
    transaction.commit().unwrap();
}
