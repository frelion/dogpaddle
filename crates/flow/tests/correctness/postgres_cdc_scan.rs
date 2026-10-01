use std::{num::NonZeroU64, path::Path};

use arrow_schema::{DataType, Field};

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::{
    OperationSetupError,
    operation::{
        scan::{
            PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanSpec,
            SequenceScanDefinition,
        },
        sink::DiscardDefinition,
    },
};

fn config() -> PostgresCdcScanConfig {
    PostgresCdcScanConfig::new_unencrypted(
        "/nonexistent/runtime",
        "127.0.0.1",
        1,
        "shop",
        "cdc",
        "secret-not-durable",
    )
    .unwrap()
}

fn definition() -> PostgresCdcScanDefinition {
    PostgresCdcScanDefinition::try_new(
        PostgresCdcScanSpec {
            engine_name: "orders".into(),
            database: "shop".into(),
            schema: "public".into(),
            table: "orders".into(),
            slot: "orders_slot".into(),
            publication: "orders_pub".into(),
            system_identifier: "123".into(),
            database_oid: 42,
            table_oid: 43,
            columns: vec![Field::new("id", DataType::Int64, false)].into(),
        },
        NonZeroU64::new(1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn factory(path: &Path, projection: &[u32]) -> FlowFactory {
    let mut raw = serde_json::to_value(definition()).unwrap();
    raw["output_projection"] = serde_json::json!(projection);
    let definition: dogpaddle_operation::OperationDefinition =
        serde_json::from_value(serde_json::json!({"postgres_cdc_scan": raw})).unwrap();
    let mut factory = FlowFactory::new(path);
    let scan = factory.operation("pg", definition, []);
    factory.operation("sink", DiscardDefinition::new(), [scan]);

    factory
}

#[test]
fn postgres_cdc_scan_resource_errors_are_operation_scoped_and_precede_store_creation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let Err(FlowError::RuntimeResource {
        operation_id,
        source: OperationSetupError::MissingRuntimeResource,
    }) = factory(&path, &[0]).build()
    else {
        panic!("missing resource")
    };
    assert_eq!(operation_id, "pg");
    assert!(!path.exists());
    let mut wrong = factory(&path, &[0]);
    wrong.resource("pg", 42_u64).unwrap();
    assert!(matches!(
        wrong.build(),
        Err(FlowError::RuntimeResource {
            source: OperationSetupError::WrongRuntimeResource,
            ..
        })
    ));
    assert!(!path.exists());
    let mut extra = factory(&path, &[0]);
    extra
        .resource("pg", config())
        .unwrap()
        .resource("typo", config())
        .unwrap();
    assert!(
        matches!(extra.build(), Err(FlowError::UnknownRuntimeResource { operation_id }) if operation_id == "typo")
    );
    assert!(!path.exists());
    let mut duplicate = factory(&path, &[0]);
    duplicate.resource("pg", config()).unwrap();
    assert!(matches!(
        duplicate.resource("pg", config()),
        Err(FlowError::DuplicateRuntimeResource { .. })
    ));
}

#[test]
fn postgres_cdc_scan_schema_failure_is_pure_and_identifies_the_operation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = factory(&path, &[1]);
    factory.resource("pg", config()).unwrap();
    let Err(FlowError::Schema { operation_id, .. }) = factory.build() else {
        panic!("invalid bound schema")
    };
    assert_eq!(operation_id, "pg");
    assert!(!path.exists());
}

#[test]
fn postgres_cdc_scan_build_and_open_need_neither_postgres_nor_jvm() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = factory(&path, &[0]);
    factory.resource("pg", config()).unwrap();
    let flow = factory.build().unwrap();
    drop(flow);
    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::RuntimeResource {
            source: OperationSetupError::MissingRuntimeResource,
            ..
        })
    ));
    let mut factory = FlowFactory::new(&path);
    factory.resource("pg", config()).unwrap();
    let flow = factory.open().unwrap();
    drop(flow);
    let store = dogpaddle_store::Store::open(&path).unwrap();
    let definition: dogpaddle_store::Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let phase: dogpaddle_store::Cell<u32> = store
        .open_data("operation/00000000/postgres_cdc_scan.phase")
        .unwrap();
    let checkpoint: dogpaddle_store::Cell<Vec<u8>> = store
        .open_data("operation/00000000/postgres_cdc_scan.checkpoint")
        .unwrap();
    let input: dogpaddle_store::Queue<Vec<u8>> = store
        .open_data("operation/00000000/postgres_cdc_scan.input")
        .unwrap();
    {
        let transaction = store.read_transaction();
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
            phase
                .read(transaction.access())
                .unwrap()
                .get()
                .unwrap()
                .is_none()
        );
        assert!(
            checkpoint
                .read(transaction.access())
                .unwrap()
                .get()
                .unwrap()
                .is_none()
        );
    }
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert!(
        input
            .access(transaction.access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
}

#[test]
fn open_rejects_new_topology_and_self_contained_operations_reject_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut open = FlowFactory::new(&path);
    open.operation("scan", SequenceScanDefinition::new(0), []);
    assert!(matches!(open.open(), Err(FlowError::OpenWithDefinition)));
    assert!(!path.exists());
    let mut build = FlowFactory::new(&path);
    let scan = build.operation("scan", SequenceScanDefinition::new(0), []);
    build.operation("sink", DiscardDefinition::new(), [scan]);

    build.resource("scan", config()).unwrap();
    assert!(matches!(
        build.build(),
        Err(FlowError::RuntimeResource {
            source: OperationSetupError::UnexpectedRuntimeResource,
            ..
        })
    ));
    assert!(!path.exists());
}

#[test]
fn retired_cdc_column_definition_is_rejected_without_rewriting_state() {
    use super::support::{read_published_definition, rewrite_checksum};
    use dogpaddle_store::{Cell, Store};

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = factory(&path, &[0]);
    factory.resource("pg", config()).unwrap();
    drop(factory.build().unwrap());
    let mut bytes = read_published_definition(&path);
    let marker = b"dogpaddle.operation\0";
    let start = bytes
        .windows(marker.len())
        .position(|part| part == marker)
        .unwrap();
    let length = usize::try_from(u32::from_be_bytes(
        bytes[start - 4..start].try_into().unwrap(),
    ))
    .unwrap();
    let mut legacy = b"dogpaddle.operation\0\0\x01".to_vec();
    legacy.extend_from_slice(br#"{"postgres_cdc_scan":{"spec":{"engine_name":"orders","database":"shop","schema":"public","table":"orders","slot":"orders_slot","publication":"orders_pub","system_identifier":"123","database_oid":42,"table_oid":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]},"output_projection":[0],"bootstrap_spool_bytes":1048576}}"#);
    bytes[start - 4..start].copy_from_slice(&u32::try_from(legacy.len()).unwrap().to_be_bytes());
    bytes.splice(start..start + length, legacy);
    rewrite_checksum(&mut bytes);
    {
        let store = Store::open(&path).unwrap();
        let definition = store.open_data::<Cell<Vec<u8>>>("flow/definition").unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        definition
            .access(transaction.access())
            .unwrap()
            .set(&bytes)
            .unwrap();
        transaction.commit().unwrap();
    }
    let mut reopen = FlowFactory::new(&path);
    reopen.resource("pg", config()).unwrap();
    assert!(
        matches!(reopen.open(), Err(FlowError::Definition(dogpaddle_flow::FlowDefinitionError::Operation { operation_id, .. })) if operation_id == "pg")
    );
    assert_eq!(read_published_definition(&path), bytes);
}

#[test]
fn deep_raw_source_fields_are_rejected_before_flow_publication() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut field = Field::new("item", DataType::Int64, true);
    for _ in 0..192 {
        field = Field::new("item", DataType::List(std::sync::Arc::new(field)), true);
    }
    let mut raw = serde_json::to_value(definition()).unwrap();
    raw["spec"]["columns"] = serde_json::to_value(vec![field]).unwrap();
    let plan = serde_json::from_value::<dogpaddle_operation::OperationDefinition>(
        serde_json::json!({"postgres_cdc_scan": raw}),
    );
    assert!(plan.is_err());
    assert!(!path.exists());
}
