use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::{
    MaterializeError,
    operation::{
        sink::DiscardDefinition,
        source::{
            PostgresColumn, PostgresSourceConfig, PostgresSourceDefinition, PostgresSourceSpec,
            PostgresType, SequenceSourceDefinition,
        },
    },
};

fn config() -> PostgresSourceConfig {
    PostgresSourceConfig::new_unencrypted(
        "/nonexistent/runtime",
        "127.0.0.1",
        1,
        "shop",
        "cdc",
        "secret-not-durable",
    )
    .unwrap()
}

fn factory(path: &Path, field: &str) -> FlowFactory {
    let definition = PostgresSourceDefinition::try_new(PostgresSourceSpec {
        engine_name: "orders".into(),
        database: "shop".into(),
        schema: "public".into(),
        table: "orders".into(),
        slot: "orders_slot".into(),
        publication: "orders_pub".into(),
        system_identifier: "123".into(),
        database_oid: 42,
        table_oid: 43,
        columns: vec![PostgresColumn::new(field, PostgresType::Int64, false)],
    })
    .unwrap();
    let mut factory = FlowFactory::new(path);
    let source = factory.station("pg", definition);
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.connect([source], sink);
    factory.output_capacity_bytes(source, NonZeroU64::new(1024).unwrap());
    factory
}

#[test]
fn postgres_resource_errors_are_station_scoped_and_precede_store_creation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let Err(FlowError::RuntimeResource {
        station_id,
        source: MaterializeError::MissingRuntimeResource,
    }) = factory(&path, "id").build()
    else {
        panic!("missing resource")
    };
    assert_eq!(station_id, "pg");
    assert!(!path.exists());
    let mut wrong = factory(&path, "id");
    wrong.resource("pg", 42_u64).unwrap();
    assert!(matches!(
        wrong.build(),
        Err(FlowError::RuntimeResource {
            source: MaterializeError::WrongRuntimeResource,
            ..
        })
    ));
    assert!(!path.exists());
    let mut extra = factory(&path, "id");
    extra
        .resource("pg", config())
        .unwrap()
        .resource("typo", config())
        .unwrap();
    assert!(
        matches!(extra.build(), Err(FlowError::UnknownRuntimeResource { station_id }) if station_id == "typo")
    );
    assert!(!path.exists());
    let mut duplicate = factory(&path, "id");
    duplicate.resource("pg", config()).unwrap();
    assert!(matches!(
        duplicate.resource("pg", config()),
        Err(FlowError::DuplicateRuntimeResource { .. })
    ));
}

#[test]
fn postgres_schema_failure_is_pure_and_identifies_the_station() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = factory(&path, "$dogpaddle.reserved");
    factory.resource("pg", config()).unwrap();
    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("invalid bound schema")
    };
    assert_eq!(error.station_id(), "pg");
    assert!(!path.exists());
}

#[test]
fn postgres_build_open_and_first_turn_need_neither_postgres_nor_jvm() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = factory(&path, "id");
    factory.resource("pg", config()).unwrap();
    let mut flow = factory.build().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);
    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::RuntimeResource {
            source: MaterializeError::MissingRuntimeResource,
            ..
        })
    ));
    let mut factory = FlowFactory::new(&path);
    factory.resource("pg", config()).unwrap();
    let mut flow = factory.open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);
    let store = dogpaddle_store::Store::open(&path).unwrap();
    let definition: dogpaddle_store::Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let state: dogpaddle_store::Cell<Vec<u8>> = store
        .open_data("station/00000000/operation/postgres_source.checkpoint")
        .unwrap();
    let transaction = store.read_transaction().unwrap();
    let bytes = definition
        .read(transaction.access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap();
    assert!(
        !bytes
            .windows(b"secret-not-durable".len())
            .any(|window| window == b"secret-not-durable")
    );
    assert!(
        state
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_none()
    );
}

#[test]
fn open_rejects_new_topology_and_self_contained_operations_reject_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut open = FlowFactory::new(&path);
    open.station("source", SequenceSourceDefinition::new(0));
    assert!(matches!(open.open(), Err(FlowError::OpenWithDefinition)));
    assert!(!path.exists());
    let mut build = FlowFactory::new(&path);
    let source = build.station("source", SequenceSourceDefinition::new(0));
    let sink = build.station("sink", DiscardDefinition::new());
    build.output_capacity_bytes(source, NonZeroU64::new(1024).unwrap());
    build.connect([source], sink);
    build.resource("source", config()).unwrap();
    assert!(matches!(
        build.build(),
        Err(FlowError::RuntimeResource {
            source: MaterializeError::UnexpectedRuntimeResource,
            ..
        })
    ));
    assert!(!path.exists());
}
