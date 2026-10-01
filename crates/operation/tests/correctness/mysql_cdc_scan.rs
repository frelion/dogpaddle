use std::{num::NonZeroU32, time::Duration};

use dogpaddle_operation::{
    OperationDefinition, OperationKind, OperationSetupError, RuntimeResource, decode_definition,
    encode_definition,
    operation::{
        Operation,
        scan::{CdcOptions, MySqlCdcScanConfig},
    },
};
use dogpaddle_store::{Cell, Queue, Store, StoreSetup};

use super::support::construct_checked_with_resource;

fn construct_checked(
    definition: &(impl Clone + Into<OperationDefinition>),
    inputs: &[arrow_schema::SchemaRef],
) -> Result<Option<arrow_schema::SchemaRef>, dogpaddle_operation::OperationBindError> {
    construct_checked_with_resource(definition, inputs, &RuntimeResource::new(config()))
}

fn definition() -> OperationDefinition {
    decode_definition(&literal_definition_bytes()).unwrap()
}

fn config() -> MySqlCdcScanConfig {
    MySqlCdcScanConfig::new_unencrypted(
        "/nonexistent/dogpaddle-runtime",
        "127.0.0.1",
        1,
        "shop",
        "cdc",
        "do-not-persist-this-password",
    )
    .unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = b"dogpaddle.operation\0\0\x01".to_vec();
    expected.extend_from_slice(br#"{"mysql_cdc_scan":{"spec":{"engine_name":"orders","database":"shop","table":"orders","server_uuid":"01234567-89ab-cdef-0123-456789abcdef","table_id":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]},"output_projection":[0],"bootstrap_spool_bytes":1048576}}"#);
    expected
}

// The connector-neutral D2 golden is stored verbatim, without an extra
// MySQL envelope. Its binding is exactly this Scan's engine and connector;
// payload bytes remain opaque to dogpaddle-operation.

#[test]
fn mysql_cdc_definition_has_a_canonical_non_secret_variant_and_exact_schema() {
    let definition = definition();
    assert_eq!(definition.kind(), OperationKind::Scan);
    let bytes = encode_definition(&definition);
    let expected = literal_definition_bytes();
    assert_eq!(bytes, expected);
    let decoded = decode_definition(&bytes).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Scan);
    assert_eq!(encode_definition(&decoded), bytes);
    let binding = construct_checked(&decoded, &[]).unwrap();
    let output = binding.as_ref().unwrap();
    assert_eq!(output.fields().len(), 1);
    assert_eq!(output.field(0).name(), "id");
    assert_eq!(output.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert!(!output.field(0).is_nullable());
    assert!(!String::from_utf8(bytes).unwrap().contains("password"));
    let mut trailing = expected.clone();
    trailing.push(b' ');
    assert!(decode_definition(&trailing).is_err());
    for length in 0..expected.len() {
        assert!(decode_definition(&expected[..length]).is_err());
    }
}

#[test]
fn mysql_cdc_uses_one_input_queue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let definition = definition();
    let mut setup = StoreSetup::new();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(path).unwrap();
    store
        .open_data::<Queue<Vec<u8>>>("operation/mysql_cdc_scan.input")
        .unwrap();
    for suffix in ["bootstrap_spool", "published"] {
        let name = format!("operation/mysql_cdc_scan.{suffix}");
        assert!(matches!(
            store.open_data::<Queue<Vec<u8>>>(&name),
            Err(dogpaddle_store::StoreError::DataNotFound(missing)) if missing == name
        ));
    }
}

#[test]
fn mysql_cdc_materialization_requires_one_exact_runtime_resource() {
    let definition = definition();
    assert!(matches!(
        definition.validate_resource(&RuntimeResource::none()),
        Err(OperationSetupError::MissingRuntimeResource)
    ));
    assert!(matches!(
        definition.validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::WrongRuntimeResource)
    ));
    assert!(
        definition
            .validate_resource(&RuntimeResource::new(config()))
            .is_ok()
    );
}

#[test]
fn mysql_cdc_restore_is_read_only_and_does_not_start_external_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut setup = StoreSetup::new();
    let definition: OperationDefinition = definition();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    for _ in 0..2 {
        let store = Store::open(&path).unwrap();
        let phase = store
            .open_data::<Cell<u32>>("operation/mysql_cdc_scan.phase")
            .unwrap();
        let (operation, _) = definition
            .construct(
                &[],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::new(config()),
            )
            .unwrap()
            .into_parts();
        let Operation::Source(mut source) = operation else {
            panic!("expected source");
        };
        let snapshot = store.read_transaction();
        source.restore(snapshot.access()).unwrap();
        assert_eq!(phase.read(snapshot.access()).unwrap().get().unwrap(), None);
        drop(source.poll().unwrap()); // Concrete BeginCapture performs no I/O or writes.
    }
}
#[test]
fn mysql_cdc_corrupt_sealed_checkpoint_is_rejected_without_rewriting_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut setup = StoreSetup::new();
    let definition: OperationDefinition = definition();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(&path).unwrap();
    let phase = store
        .open_data::<Cell<u32>>("operation/mysql_cdc_scan.phase")
        .unwrap();
    let checkpoint_handle = store
        .open_data::<Cell<Vec<u8>>>("operation/mysql_cdc_scan.checkpoint")
        .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    let txn = writes.begin();
    phase.access(txn.access()).unwrap().set(&2).unwrap();
    checkpoint_handle
        .access(txn.access())
        .unwrap()
        .set(&vec![0])
        .unwrap();
    txn.commit().unwrap();
    drop((writes, reads));
    let store = Store::open(&path).unwrap();
    let (operation, _) = definition
        .construct(
            &[],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    let snapshot = store.read_transaction();
    assert!(source.restore(snapshot.access()).is_err());
    let checkpoint_handle = store
        .open_data::<Cell<Vec<u8>>>("operation/mysql_cdc_scan.checkpoint")
        .unwrap();
    assert_eq!(
        checkpoint_handle
            .read(snapshot.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(vec![0])
    );
}
#[test]
fn mysql_cdc_runtime_config_is_secret_safe_and_requires_explicit_unencrypted_setup() {
    let options = CdcOptions::new()
        .connect_timeout(Duration::from_millis(7))
        .unwrap()
        .query_timeout(Duration::from_millis(8))
        .unwrap()
        .retry_limit(9)
        .unwrap()
        .retry_max_delay(Duration::from_millis(301))
        .unwrap()
        .heartbeat_interval(Duration::from_millis(11))
        .unwrap()
        .snapshot_fetch_size(NonZeroU32::new(12).unwrap())
        .unwrap();
    let debug = format!("{:?}", config().options(options));
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("do-not-persist-this-password"));
    assert!(debug.contains("snapshot_fetch_size: Some(12)"));
    assert!(
        MySqlCdcScanConfig::new_unencrypted("relative", "host", 3306, "db", "user", "password")
            .is_err()
    );
    assert!(
        MySqlCdcScanConfig::new_unencrypted("/bundle", "host", 0, "db", "user", "password")
            .is_err()
    );
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["mysql_cdc_scan"]["output_projection"] = serde_json::json!([1, 0]);
    let plan = serde_json::from_value(payload).unwrap();
    crate::support::assert_rejected_plan_before_data(&plan, &[], RuntimeResource::new(config()));
}
