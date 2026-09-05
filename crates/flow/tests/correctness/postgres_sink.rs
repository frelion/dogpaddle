use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::{
    MaterializeError, OperationBindError, col,
    operation::{
        sink::{
            PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkSchemaError, PostgresTargetSpec,
        },
        source::SequenceSourceDefinition,
        transform::SelectDefinition,
    },
};
use dogpaddle_store::{Cell, Store, StoreError};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024).unwrap();
const SINK: &str = "postgres";
const STATE: &str = "station/00000001/operation/postgres_sink.state";

fn config() -> PostgresSinkConfig {
    PostgresSinkConfig::new_unencrypted("127.0.0.1", 1, "database", "writer", "secret-not-durable")
        .unwrap()
}

fn definition() -> PostgresSinkDefinition {
    let target =
        PostgresTargetSpec::try_new("sink_1", "database", "public", "events", "1", 2).unwrap();
    PostgresSinkDefinition::try_new(target).unwrap()
}

fn factory(path: &Path) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let sink = factory.station(SINK, definition());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.connect([source], sink);
    factory
}

#[test]
fn postgres_sink_resource_errors_are_station_scoped_and_precede_store_creation() {
    let root = tempfile::tempdir().unwrap();

    let missing_path = root.path().join("missing");
    let Err(FlowError::RuntimeResource {
        station_id,
        source: MaterializeError::MissingRuntimeResource,
    }) = factory(&missing_path).build()
    else {
        panic!("missing PostgreSQL sink resource was accepted");
    };
    assert_eq!(station_id, SINK);
    assert!(!missing_path.exists());

    let wrong_path = root.path().join("wrong");
    let mut wrong = factory(&wrong_path);
    wrong.resource(SINK, 42_u64).unwrap();
    let Err(FlowError::RuntimeResource {
        station_id,
        source: MaterializeError::WrongRuntimeResource,
    }) = wrong.build()
    else {
        panic!("wrong PostgreSQL sink resource type was accepted");
    };
    assert_eq!(station_id, SINK);
    assert!(!wrong_path.exists());
}

#[test]
fn postgres_sink_schema_rejection_is_pure_and_station_scoped() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let invalid_name = "x".repeat(64);
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let select = factory.station(
        "select",
        SelectDefinition::try_new([(invalid_name.clone(), col("value"))]).unwrap(),
    );
    let sink = factory.station(SINK, definition());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(select, CAPACITY);
    factory.connect([source], select);
    factory.connect([select], sink);
    factory.resource(SINK, config()).unwrap();

    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("PostgreSQL-incompatible field name unexpectedly bound");
    };
    assert_eq!(error.station_id(), SINK);
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("PostgreSQL field-name rejection returned the wrong binding error");
    };
    assert!(matches!(
        source.downcast_ref::<PostgresSinkSchemaError>(),
        Some(PostgresSinkSchemaError::InvalidFieldName { field: 0, name })
            if name == &invalid_name
    ));
    assert!(!path.exists(), "Schema rejection created the Store path");
}

#[test]
fn postgres_sink_build_and_reopen_are_offline_and_use_one_stable_state_cell() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut build = factory(&path);
    build.resource(SINK, config()).unwrap();
    drop(build.build().unwrap());

    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::RuntimeResource {
            station_id,
            source: MaterializeError::MissingRuntimeResource,
        }) if station_id == SINK
    ));

    for _ in 0..2 {
        let mut open = FlowFactory::new(&path);
        open.resource(SINK, config()).unwrap();
        drop(open.open().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let state: Cell<Vec<u8>> = store.open_data(STATE).unwrap();
    assert!(matches!(
        store.open_data::<Cell<Vec<u8>>>(
            "station/00000001/operation/postgres_sink.pending"
        ),
        Err(StoreError::DataNotFound(name))
            if name == "station/00000001/operation/postgres_sink.pending"
    ));
    let transaction = store.read_transaction().unwrap();
    assert!(
        state
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_none()
    );
}
