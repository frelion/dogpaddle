use std::{num::NonZeroU32, path::Path, time::Duration};

use dogpaddle_operation::{
    OperationDefinition, OperationKind, OperationSetupError, RuntimeResource, decode_definition,
    encode_definition,
    operation::{
        Action, Operation, OperationError, Turn,
        scan::{MySqlCdcScanConfig, MySqlCdcScanOptions},
    },
};
use dogpaddle_store::{Cell, Queue, Store, StoreSetup, Transactions};

use super::support::{construct_checked_with_resource, decode_hex};

fn construct_checked(
    definition: &dyn OperationDefinition,
    inputs: &[arrow_schema::SchemaRef],
) -> Result<Option<arrow_schema::SchemaRef>, dogpaddle_operation::OperationBindError> {
    construct_checked_with_resource(definition, inputs, &RuntimeResource::new(config()))
}

fn definition() -> Box<dyn OperationDefinition> {
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
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x0f".to_vec();
    expected.extend_from_slice(br#"{"spec":{"engine_name":"orders","database":"shop","table":"orders","server_uuid":"01234567-89ab-cdef-0123-456789abcdef","table_id":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]},"bootstrap_spool_bytes":1048576}"#);
    expected
}

// The connector-neutral D2 golden is stored verbatim, without an extra
// MySQL envelope. Its binding is exactly this Scan's engine and connector;
// payload bytes remain opaque to dogpaddle-operation.
fn checkpoint() -> Vec<u8> {
    decode_hex(concat!(
        "44504442435030310001000000066f72646572730000002a",
        "696f2e646562657a69756d2e636f6e6e6563746f722e6d7973716c2e4d7953716c",
        "436f6e6e6563746f7200000001000000056d7973716c00000003000102bc51316d"
    ))
}

#[test]
fn mysql_cdc_definition_has_a_canonical_non_secret_tag_and_exact_schema() {
    let definition = definition();
    assert_eq!(definition.kind(), OperationKind::Scan);
    assert_eq!(definition.persistence_tag(), 15);
    let bytes = encode_definition(definition.as_ref());
    let expected = literal_definition_bytes();
    assert_eq!(bytes, expected);
    let decoded = decode_definition(&bytes).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Scan);
    assert_eq!(decoded.persistence_tag(), 15);
    assert_eq!(encode_definition(decoded.as_ref()), bytes);
    let binding = construct_checked(decoded.as_ref(), &[]).unwrap();
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
fn mysql_cdc_bootstrap_spool_is_a_queue() {
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
    Store::open(path)
        .unwrap()
        .open_data::<Queue<Vec<u8>>>("operation/mysql_cdc_scan.bootstrap_spool")
        .unwrap();
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

struct Fixture {
    scan: Operation,
    phase: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    transactions: Transactions,
}

impl Fixture {
    fn create(path: &Path) -> Self {
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
        let transactions = setup.commit(path, |_| Ok(())).unwrap();
        drop((operation, transactions));
        Self::open(Store::open(path).unwrap())
    }

    fn open(store: Store) -> Self {
        let definition = decode_definition(&literal_definition_bytes()).unwrap();
        let (scan, _) = definition
            .construct(
                &[],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::new(config()),
            )
            .unwrap()
            .into_parts();
        Self {
            scan,
            phase: store.open_data("operation/mysql_cdc_scan.phase").unwrap(),
            checkpoint: store
                .open_data("operation/mysql_cdc_scan.checkpoint")
                .unwrap(),
            transactions: store.into_transactions(),
        }
    }

    fn set_checkpoint(&mut self, bytes: &[u8]) {
        let transaction = self.transactions.begin();
        self.phase
            .access(transaction.access())
            .unwrap()
            .set(&2)
            .unwrap();
        self.checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&bytes.to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    fn restore(&mut self, commit: bool) -> Result<(), OperationError> {
        let Turn::Ready(prepared) = self.scan.turn(None)? else {
            panic!("expected prepared work");
        };
        let transaction = self.transactions.begin();
        let (action, completion) = prepared.apply(transaction.access())?;
        assert!(matches!(action, Action::Commit(None)));
        if commit {
            transaction.commit()?;
            completion.run()?;
        } else {
            drop(transaction);
            drop(completion);
        }
        Ok(())
    }

    fn durable_checkpoint(&mut self) -> Option<Vec<u8>> {
        let transaction = self.transactions.begin();
        let checkpoint = self
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap();
        transaction.commit().unwrap();
        checkpoint
    }
}

#[test]
fn mysql_cdc_initialization_and_reopen_do_not_start_external_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    drop(Fixture::create(&path));
    for _ in 0..2 {
        let mut fixture = Fixture::open(Store::open(&path).unwrap());
        drop(fixture.scan.turn(None).unwrap());
        // Rollback cannot publish initialized memory state or start the JVM.
        for _ in 0..2 {
            fixture.restore(false).unwrap();
        }
        fixture.restore(true).unwrap();
        assert_eq!(fixture.durable_checkpoint(), None);
    }
}

#[test]
fn mysql_cdc_reopen_commits_capture_reset_without_external_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut fixture = Fixture::create(&path);
    let captured = checkpoint();
    fixture.set_checkpoint(&captured);
    let transaction = fixture.transactions.begin();
    fixture
        .phase
        .access(transaction.access())
        .unwrap()
        .set(&1)
        .unwrap();
    transaction.commit().unwrap();
    drop(fixture);

    for (commit, expected_phase) in [(false, 1), (true, 4)] {
        let mut fixture = Fixture::open(Store::open(&path).unwrap());
        fixture.restore(commit).unwrap();
        // MySQL has no source-owned snapshot slot to remove. The restore
        // transaction may enter Resetting, but rollback must retain Capturing.
        let transaction = fixture.transactions.begin();
        assert_eq!(
            fixture
                .phase
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(expected_phase)
        );
        assert_eq!(
            fixture
                .checkpoint
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(captured.clone())
        );
        transaction.commit().unwrap();
    }
}

#[test]
fn mysql_cdc_restore_rejects_corrupt_checkpoint_without_initializing() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::create(&root.path().join("state"));
    let invalid = b"not a Debezium checkpoint";
    fixture.set_checkpoint(invalid);
    for _ in 0..2 {
        assert!(fixture.restore(true).is_err());
    }
    assert_eq!(fixture.durable_checkpoint(), Some(invalid.to_vec()));
}

#[test]
fn mysql_cdc_restores_opaque_checkpoint_across_rollback_and_reopen_without_external_io() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut fixture = Fixture::create(&path);
    let initial = checkpoint();
    fixture.set_checkpoint(&initial);
    drop(fixture);
    for _ in 0..2 {
        let mut fixture = Fixture::open(Store::open(&path).unwrap());
        drop(fixture.scan.turn(None).unwrap());
        for _ in 0..2 {
            fixture.restore(false).unwrap();
            assert_eq!(fixture.durable_checkpoint(), Some(initial.clone()));
        }
        fixture.restore(true).unwrap();
        assert_eq!(fixture.durable_checkpoint(), Some(initial.clone()));
    }
}

#[test]
fn mysql_cdc_runtime_config_is_secret_safe_and_requires_explicit_unencrypted_setup() {
    let options = MySqlCdcScanOptions::new()
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
fn mysql_cdc_runtime_options_reject_values_debezium_cannot_represent() {
    let excessive = Duration::from_millis(u64::try_from(i32::MAX).unwrap() + 1);
    for result in [
        MySqlCdcScanOptions::new().connect_timeout(Duration::ZERO),
        MySqlCdcScanOptions::new().connect_timeout(Duration::from_nanos(1)),
        MySqlCdcScanOptions::new().connect_timeout(excessive),
        MySqlCdcScanOptions::new().query_timeout(Duration::ZERO),
        MySqlCdcScanOptions::new().query_timeout(Duration::from_millis(2_147_483_001)),
        MySqlCdcScanOptions::new().retry_max_delay(Duration::from_millis(300)),
        MySqlCdcScanOptions::new().heartbeat_interval(Duration::ZERO),
    ] {
        assert!(result.is_err());
    }
    assert!(
        MySqlCdcScanOptions::new()
            .retry_limit(u32::try_from(i32::MAX).unwrap() + 1)
            .is_err()
    );
    assert!(
        MySqlCdcScanOptions::new()
            .snapshot_fetch_size(NonZeroU32::new(u32::try_from(i32::MAX).unwrap() + 1).unwrap())
            .is_err()
    );
}
