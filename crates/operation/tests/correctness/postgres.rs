use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, Turn,
        source::{
            PostgresColumn, PostgresSourceConfig, PostgresSourceDefinition, PostgresSourceSpec,
            PostgresType,
        },
    },
};
use dogpaddle_store::{Cell, Store, Transactions};
use std::path::Path;

use super::support::decode_hex;

fn definition() -> PostgresSourceDefinition {
    PostgresSourceDefinition::try_new(PostgresSourceSpec {
        engine_name: "orders".into(),
        database: "shop".into(),
        schema: "public".into(),
        table: "orders".into(),
        slot: "orders_slot".into(),
        publication: "orders_pub".into(),
        system_identifier: "123".into(),
        database_oid: 42,
        table_oid: 43,
        columns: vec![PostgresColumn::new("id", PostgresType::Int64, false)],
    })
    .unwrap()
}

fn config() -> PostgresSourceConfig {
    PostgresSourceConfig::new_unencrypted(
        "/nonexistent/dogpaddle-runtime",
        "127.0.0.1",
        1,
        "shop",
        "cdc",
        "do-not-persist-this-password",
    )
    .unwrap()
}

#[test]
fn postgres_definition_has_a_canonical_non_secret_tag_and_exact_schema() {
    let definition = definition();
    assert_eq!(definition.kind(), OperationKind::Source);
    let bytes = encode_definition(&definition);
    let mut expected = b"dogpaddle.operation\0\0\x01\0\x0b".to_vec();
    expected.extend_from_slice(br#"{"engine_name":"orders","database":"shop","schema":"public","table":"orders","slot":"orders_slot","publication":"orders_pub","system_identifier":"123","database_oid":42,"table_oid":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]}"#);
    assert_eq!(bytes, expected);
    let decoded = decode_definition(&bytes).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), bytes);
    assert_eq!(
        decoded
            .data()
            .iter()
            .map(dogpaddle_operation::DataDeclaration::name)
            .collect::<Vec<_>>(),
        ["postgres_source.checkpoint"]
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
fn postgres_materialization_requires_one_exact_runtime_resource() {
    let definition = definition();
    let binding = (&definition as &dyn OperationDefinition).bind(&[]).unwrap();
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
    source: Box<dyn Operation>,
    checkpoint: Cell<Vec<u8>>,
    transactions: Transactions,
}

impl Fixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).unwrap();
        for declaration in definition().data() {
            declaration.create(&mut store, declaration.name()).unwrap();
        }
        Self::open(store)
    }

    fn open(store: Store) -> Self {
        let definition = decode_definition(&encode_definition(&definition())).unwrap();
        let mut data = DataInstances::new();
        for declaration in definition.data() {
            data.insert(declaration.open(&store, declaration.name()).unwrap())
                .unwrap();
        }
        Self {
            source: definition
                .bind(&[])
                .unwrap()
                .materialize(data, RuntimeResource::new(config()))
                .unwrap(),
            checkpoint: store.open_data("postgres_source.checkpoint").unwrap(),
            transactions: store.into_transactions(),
        }
    }

    fn set_checkpoint(&mut self, bytes: &Vec<u8>) {
        let transaction = self.transactions.begin().unwrap();
        self.checkpoint
            .access(transaction.access())
            .unwrap()
            .set(bytes)
            .unwrap();
        transaction.commit().unwrap();
    }

    fn restore(&mut self, commit: bool) -> Result<(), OperationError> {
        let Turn::Ready(prepared) = self.source.turn(None)? else {
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

// The connector-neutral D2 golden is stored verbatim, without a Source envelope.
// These tests only restore its framing; connector binding is checked at start.
fn checkpoint() -> Vec<u8> {
    decode_hex(concat!(
        "4450444243503031000100000008656e67696e652d61",
        "0000000b636f6e6e6563746f722e4100000002",
        "0000000200ff000000020102000000017a00000001031ef7d5c2"
    ))
}

#[test]
fn postgres_initialization_and_reopen_do_not_start_external_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    drop(Fixture::create(&path));
    for _ in 0..2 {
        let mut fixture = Fixture::open(Store::open(&path).unwrap());
        drop(fixture.source.turn(None).unwrap());
        // Rollback cannot publish initialized memory state or start the JVM.
        for _ in 0..2 {
            fixture.restore(false).unwrap();
        }
        fixture.restore(true).unwrap();
        assert_eq!(fixture.durable_checkpoint(), None);
    }
}

#[test]
fn postgres_restores_opaque_checkpoint_across_rollback_and_reopen_without_external_io() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut fixture = Fixture::create(&path);
    let initial = checkpoint();
    fixture.set_checkpoint(&initial);
    drop(fixture);
    for _ in 0..2 {
        let mut fixture = Fixture::open(Store::open(&path).unwrap());
        drop(fixture.source.turn(None).unwrap());
        for _ in 0..2 {
            fixture.restore(false).unwrap();
            assert_eq!(fixture.durable_checkpoint(), Some(initial.clone()));
        }
        fixture.restore(true).unwrap();
        assert_eq!(fixture.durable_checkpoint(), Some(initial.clone()));
    }
}

#[test]
fn postgres_restore_rejects_corrupt_checkpoint_without_initializing() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::create(&root.path().join("state"));
    let valid = checkpoint();
    let mut corrupt = valid.clone();
    corrupt[0] ^= 1;
    let mut trailing = valid.clone();
    trailing.push(0);
    for invalid in [
        Vec::new(),
        valid[..valid.len() - 1].to_vec(),
        corrupt,
        trailing,
    ] {
        fixture.set_checkpoint(&invalid);
        for _ in 0..2 {
            assert!(fixture.restore(true).is_err());
        }
        assert_eq!(fixture.durable_checkpoint(), Some(invalid));
    }
    fixture.set_checkpoint(&valid);
    fixture.restore(true).unwrap();
    assert_eq!(fixture.durable_checkpoint(), Some(valid));
}

#[test]
fn postgres_schema_rejects_unsupported_precision_and_invalid_columns() {
    let column = |data_type| PostgresColumn::new("id", data_type, false);
    for columns in [
        vec![],
        vec![PostgresColumn::new("", PostgresType::Int64, false)],
        vec![PostgresColumn::new(
            "$dogpaddle.value",
            PostgresType::Int64,
            false,
        )],
        vec![column(PostgresType::Int64), column(PostgresType::Text)],
        vec![column(PostgresType::Numeric {
            precision: 0,
            scale: 0,
        })],
        vec![column(PostgresType::Numeric {
            precision: 39,
            scale: 0,
        })],
        vec![column(PostgresType::Numeric {
            precision: 2,
            scale: 3,
        })],
        vec![column(PostgresType::Numeric {
            precision: 2,
            scale: -1,
        })],
    ] {
        let mut spec = definition().spec().clone();
        spec.columns = columns;
        if let Ok(definition) = PostgresSourceDefinition::try_new(spec) {
            assert!((&definition as &dyn OperationDefinition).bind(&[]).is_err());
        }
    }
}

#[test]
fn postgres_runtime_config_is_secret_safe_and_requires_explicit_unencrypted_setup() {
    let debug = format!("{:?}", config());
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("do-not-persist-this-password"));
    assert!(
        PostgresSourceConfig::new_unencrypted("relative", "host", 5432, "db", "user", "password")
            .is_err()
    );
    assert!(
        PostgresSourceConfig::new_unencrypted("/bundle", "host", 0, "db", "user", "password")
            .is_err()
    );
}
