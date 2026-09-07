use std::path::Path;

use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition, encode_definition,
    operation::{Action, Operation, OperationError, Turn, scan::MySqlCdcScanConfig},
};
use dogpaddle_store::{Cell, Store, Transactions};

use super::support::decode_hex;

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
        54_001,
    )
    .unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x0f".to_vec();
    expected.extend_from_slice(br#"{"spec":{"engine_name":"orders","database":"shop","table":"orders","server_uuid":"01234567-89ab-cdef-0123-456789abcdef","table_id":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]},"bootstrap_checkpoint":"RFBEQkNQMDEAAQAAAAZvcmRlcnMAAAAqaW8uZGViZXppdW0uY29ubmVjdG9yLm15c3FsLk15U3FsQ29ubmVjdG9yAAAAAQAAAAVteXNxbAAAAAMAAQK8UTFt"}"#);
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
    assert_eq!(
        decoded
            .data()
            .iter()
            .map(dogpaddle_operation::DataDeclaration::name)
            .collect::<Vec<_>>(),
        ["mysql_cdc_scan.checkpoint"]
    );
    let binding = decoded.bind(&[]).unwrap();
    let output = binding.output_schema().unwrap();
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
fn mysql_cdc_materialization_requires_one_exact_runtime_resource() {
    let definition = definition();
    let binding = definition.bind(&[]).unwrap();
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
}

struct Fixture {
    scan: Box<dyn Operation>,
    checkpoint: Cell<Vec<u8>>,
    transactions: Transactions,
}

impl Fixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).unwrap();
        let definition = definition();
        for declaration in definition.data() {
            declaration.create(&mut store, declaration.name()).unwrap();
        }
        Self::open(store)
    }

    fn open(store: Store) -> Self {
        let definition = decode_definition(&literal_definition_bytes()).unwrap();
        let mut data = DataInstances::new();
        for declaration in definition.data() {
            data.insert(declaration.open(&store, declaration.name()).unwrap())
                .unwrap();
        }
        Self {
            scan: definition
                .bind(&[])
                .unwrap()
                .materialize(data, RuntimeResource::new(config()))
                .unwrap(),
            checkpoint: store.open_data("mysql_cdc_scan.checkpoint").unwrap(),
            transactions: store.into_transactions(),
        }
    }

    fn set_checkpoint(&mut self, bytes: &[u8]) {
        let transaction = self.transactions.begin().unwrap();
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
        let transaction = self.transactions.begin()?;
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
        let transaction = self.transactions.begin().unwrap();
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
    let debug = format!("{:?}", config());
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("do-not-persist-this-password"));
    assert!(
        MySqlCdcScanConfig::new_unencrypted("relative", "host", 3306, "db", "user", "password", 1)
            .is_err()
    );
    assert!(
        MySqlCdcScanConfig::new_unencrypted("/bundle", "host", 0, "db", "user", "password", 1)
            .is_err()
    );
    assert!(
        MySqlCdcScanConfig::new_unencrypted("/bundle", "host", 3306, "db", "user", "password", 0)
            .is_err()
    );
}
