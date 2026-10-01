use std::path::Path;

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::{
    OperationBindError, OperationSetupError, col,
    operation::{
        scan::SequenceScanDefinition,
        sink::{PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkSchemaError},
        transform::SelectDefinition,
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store};

const SINK: &str = "postgres";
const CONTROL: &str = "operation/00000001/sink.control";
const BUFFER: &str = "operation/00000001/sink.buffer";

fn config() -> PostgresSinkConfig {
    PostgresSinkConfig::new_unencrypted("127.0.0.1", 1, "database", "writer", "secret-not-durable")
        .unwrap()
}

fn definition() -> PostgresSinkDefinition {
    PostgresSinkDefinition::try_new("sink_1", "database", "public", "events", "1", 2).unwrap()
}

fn factory(path: &Path) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    factory.operation(SINK, definition(), [scan]);

    factory
}

#[test]
fn postgres_sink_resource_errors_are_operation_scoped_and_precede_store_creation() {
    let root = tempfile::tempdir().unwrap();

    let missing_path = root.path().join("missing");
    let Err(FlowError::RuntimeResource {
        operation_id,
        source: OperationSetupError::MissingRuntimeResource,
    }) = factory(&missing_path).build()
    else {
        panic!("missing PostgreSQL sink resource was accepted");
    };
    assert_eq!(operation_id, SINK);
    assert!(!missing_path.exists());

    let wrong_path = root.path().join("wrong");
    let mut wrong = factory(&wrong_path);
    wrong.resource(SINK, 42_u64).unwrap();
    let Err(FlowError::RuntimeResource {
        operation_id,
        source: OperationSetupError::WrongRuntimeResource,
    }) = wrong.build()
    else {
        panic!("wrong PostgreSQL sink resource type was accepted");
    };
    assert_eq!(operation_id, SINK);
    assert!(!wrong_path.exists());
}

#[test]
fn postgres_sink_schema_rejection_is_pure_and_operation_scoped() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let invalid_name = "x".repeat(64);
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([(invalid_name.clone(), col("value"))]).unwrap(),
        [scan],
    );
    factory.operation(SINK, definition(), [select]);

    factory.resource(SINK, config()).unwrap();

    let Err(FlowError::Schema {
        operation_id,
        source,
        ..
    }) = factory.build()
    else {
        panic!("PostgreSQL-incompatible field name unexpectedly bound");
    };
    assert_eq!(operation_id, SINK);
    let OperationBindError::Rejected { source } = source else {
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
fn postgres_sink_build_and_reopen_are_offline_and_use_stable_buffered_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut build = factory(&path);
    build.resource(SINK, config()).unwrap();
    drop(build.build().unwrap());

    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::RuntimeResource {
            operation_id,
            source: OperationSetupError::MissingRuntimeResource,
        }) if operation_id == SINK
    ));

    for _ in 0..2 {
        let mut open = FlowFactory::new(&path);
        open.resource(SINK, config()).unwrap();
        drop(open.open().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let state: Cell<Vec<u8>> = store.open_data(CONTROL).unwrap();
    let _: OrderedMap<u64, Vec<u8>> = store.open_data(BUFFER).unwrap();
    let transaction = store.read_transaction();
    assert!(
        state
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_none()
    );
}
