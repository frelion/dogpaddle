use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, DefinitionCodecError, OperationBindError, OperationDefinition, OperationKind,
    RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn,
        sink::{SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkSchemaError},
    },
};
use dogpaddle_store::{Cell, Store, Transactions};
use rusqlite::{Connection, OpenFlags};

use super::support::{
    TestStore, assert_literal_definition, bind, commit_ready, data_names, decode_hex, materialize,
    rollback_ready, value_schema,
};

const SQLITE_SINK_V1: &str = include_str!("../fixtures/v1/sqlite_sink_output_events.hex");
const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

#[test]
fn sqlite_sink_definition_has_stable_v1_literal_and_public_contract() {
    let sqlite =
        SqliteSinkDefinition::try_new("/var/lib/dogpaddle/output.sqlite", "events").unwrap();
    let decoded = assert_literal_definition(
        &sqlite,
        SQLITE_SINK_V1,
        10,
        OperationKind::Sink(std::num::NonZeroU32::MIN),
    );
    assert_eq!(data_names(&sqlite), ["relation_sink.state"]);
    assert_eq!(
        sqlite.database_path(),
        Path::new("/var/lib/dogpaddle/output.sqlite")
    );
    assert_eq!(sqlite.table_name(), "events");
    assert!(
        decoded
            .bind(&[value_schema()])
            .unwrap()
            .output_schema()
            .is_none()
    );

    let encoded = encode_definition(&sqlite);
    let path = b"/var/lib/dogpaddle/output.sqlite";
    let table = b"events";
    let path_length_offset = DEFINITION_HEADER_LEN;
    let path_offset = path_length_offset + size_of::<u32>();
    let table_length_offset = path_offset + path.len();
    let table_offset = table_length_offset + size_of::<u32>();
    assert_eq!(
        &encoded[path_length_offset..path_offset],
        &u32::try_from(path.len()).unwrap().to_be_bytes()
    );
    assert_eq!(&encoded[path_offset..table_length_offset], path);
    assert_eq!(
        &encoded[table_length_offset..table_offset],
        &u32::try_from(table.len()).unwrap().to_be_bytes()
    );
    assert_eq!(&encoded[table_offset..], table);
}

#[test]
fn sqlite_sink_decoder_rejects_invalid_lengths_strings_and_paths() {
    let canonical = decode_hex(SQLITE_SINK_V1);
    let path_length_offset = DEFINITION_HEADER_LEN;
    let path_length = usize::try_from(u32::from_be_bytes(
        canonical[path_length_offset..path_length_offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let path_offset = path_length_offset + size_of::<u32>();
    let table_length_offset = path_offset + path_length;
    let table_offset = table_length_offset + size_of::<u32>();

    let mut forged_path_length = canonical.clone();
    forged_path_length[path_length_offset..path_offset].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_path_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut forged_table_length = canonical.clone();
    forged_table_length[table_length_offset..table_offset].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_table_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    for invalid_utf8_offset in [path_offset, table_offset] {
        let mut invalid_utf8 = canonical.clone();
        invalid_utf8[invalid_utf8_offset] = u8::MAX;
        assert!(matches!(
            decode_definition(&invalid_utf8),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
    }

    let wrap = |path: &[u8], table: &[u8]| {
        let mut encoded = canonical[..DEFINITION_HEADER_LEN].to_vec();
        encoded.extend_from_slice(&u32::try_from(path.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(path);
        encoded.extend_from_slice(&u32::try_from(table.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(table);
        encoded
    };
    for invalid in [
        wrap(b"relative.sqlite", b"events"),
        wrap(b":memory:", b"events"),
        wrap(b"/tmp/invalid\0.sqlite", b"events"),
        wrap(b"/tmp/output.sqlite", b""),
        wrap(b"/tmp/output.sqlite", b"bad\0table"),
        wrap(b"/tmp/output.sqlite", b"SQLITE_reserved"),
    ] {
        assert!(matches!(
            decode_definition(&invalid),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
    }
}

#[test]
fn sqlite_sink_decoder_never_panics_for_valid_header_arbitrary_payloads() {
    let mut header = decode_hex(SQLITE_SINK_V1);
    header.truncate(DEFINITION_HEADER_LEN);
    let mut state = 0x3c6e_f372_fe94_f82b_u64;
    for length in 0..=256 {
        let mut input = header.clone();
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            input.push(state.to_le_bytes()[0]);
        }
        let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&input)));
        assert!(
            result.is_ok(),
            "SQLiteSink decoder panicked for payload length {length}"
        );
    }
}

#[test]
fn sqlite_sink_definition_rejects_non_persistent_paths_and_invalid_table_names() {
    for (path, expected) in [
        (
            PathBuf::from(":memory:"),
            SqliteSinkDefinitionError::InMemoryDatabase,
        ),
        (
            PathBuf::from("relative.sqlite"),
            SqliteSinkDefinitionError::DatabasePathNotAbsolute,
        ),
        (
            PathBuf::from("/tmp/invalid\0.sqlite"),
            SqliteSinkDefinitionError::DatabasePathContainsNul,
        ),
    ] {
        assert_eq!(
            SqliteSinkDefinition::try_new(path, "events").unwrap_err(),
            expected
        );
    }

    for (table, expected) in [
        ("", SqliteSinkDefinitionError::EmptyTableName),
        (
            "invalid\0table",
            SqliteSinkDefinitionError::TableNameContainsNul,
        ),
        (
            "sqlite_events",
            SqliteSinkDefinitionError::ReservedTableName,
        ),
        (
            "SQLITE_EVENTS",
            SqliteSinkDefinitionError::ReservedTableName,
        ),
    ] {
        assert_eq!(
            SqliteSinkDefinition::try_new("/tmp/output.sqlite", table).unwrap_err(),
            expected
        );
    }
}

#[cfg(unix)]
#[test]
fn sqlite_sink_definition_rejects_a_non_utf8_database_path() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt as _};

    let path = PathBuf::from(OsString::from_vec(b"/tmp/invalid-\xff.sqlite".to_vec()));
    assert_eq!(
        SqliteSinkDefinition::try_new(path, "events").unwrap_err(),
        SqliteSinkDefinitionError::DatabasePathNotUtf8
    );
}

#[test]
fn sqlite_sink_binding_accepts_zero_and_1998_logical_columns() {
    let definition = SqliteSinkDefinition::try_new("/tmp/output.sqlite", "events").unwrap();
    let empty = Arc::new(Schema::empty());
    assert!(
        bind(&definition, std::slice::from_ref(&empty))
            .unwrap()
            .output_schema()
            .is_none()
    );

    let empty_name = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, true)]));
    assert!(
        bind(&definition, std::slice::from_ref(&empty_name))
            .unwrap()
            .output_schema()
            .is_none()
    );

    let maximum = Arc::new(Schema::new(
        (0..1_998)
            .map(|index| Field::new(format!("field_{index}"), DataType::Null, true))
            .collect::<Vec<_>>(),
    ));
    assert!(
        bind(&definition, std::slice::from_ref(&maximum))
            .unwrap()
            .output_schema()
            .is_none()
    );

    let temporal_and_decimal = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
            true,
        ),
        Field::new("amount", DataType::Decimal128(38, -4), false),
    ]));
    assert!(
        bind(&definition, std::slice::from_ref(&temporal_and_decimal))
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn sqlite_sink_binding_rejects_sqlite_identifier_collisions_and_1999_columns() {
    let definition = SqliteSinkDefinition::try_new("/tmp/output.sqlite", "events").unwrap();

    let too_many = Arc::new(Schema::new(
        (0..1_999)
            .map(|index| Field::new(format!("field_{index}"), DataType::Null, true))
            .collect::<Vec<_>>(),
    ));
    let Err(OperationBindError::Rejected { source }) =
        bind(&definition, std::slice::from_ref(&too_many))
    else {
        panic!("a SQLite sink with 1999 logical columns unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SqliteSinkSchemaError>(),
        Some(SqliteSinkSchemaError::TooManyColumns {
            actual: 1_999,
            maximum: 1_998,
        })
    ));

    let nul = Arc::new(Schema::new(vec![Field::new(
        "invalid\0name",
        DataType::Utf8,
        true,
    )]));
    let Err(OperationBindError::Rejected { source }) =
        bind(&definition, std::slice::from_ref(&nul))
    else {
        panic!("a SQLite sink field containing NUL unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SqliteSinkSchemaError>(),
        Some(SqliteSinkSchemaError::FieldNameContainsNul { field: 0 })
    ));

    let duplicate = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let Err(OperationBindError::Rejected { source }) =
        bind(&definition, std::slice::from_ref(&duplicate))
    else {
        panic!("ASCII case-insensitive SQLite field collision unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SqliteSinkSchemaError>(),
        Some(SqliteSinkSchemaError::CaseInsensitiveFieldCollision {
            first: 0,
            second: 1,
        })
    ));

    for (name, expected_name) in [
        ("$DOGPADDLE.ID", "$DOGPADDLE.ID"),
        ("$DOGPADDLE.HASH", "$DOGPADDLE.HASH"),
    ] {
        let collision = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
        let Err(OperationBindError::Rejected { source }) =
            bind(&definition, std::slice::from_ref(&collision))
        else {
            panic!("SQLite technical field collision unexpectedly bound");
        };
        assert!(matches!(
            source.downcast_ref::<SqliteSinkSchemaError>(),
            Some(SqliteSinkSchemaError::TechnicalColumnCollision {
                field: 0,
                name,
            }) if name == expected_name
        ));
    }
}

#[test]
fn sqlite_sink_declarations_have_exact_cell_types_and_materialization_is_lazy() {
    let fixture = TestStore::new();
    let sqlite_path = fixture.path().with_extension("sqlite");
    let definition = SqliteSinkDefinition::try_new(&sqlite_path, "events").unwrap();

    let mut store = Store::create(fixture.path()).unwrap();
    assert_eq!(definition.data().len(), 1);
    definition.data()[0]
        .create(&mut store, "sqlite-state")
        .unwrap();
    assert!(!sqlite_path.exists());
    drop(store);

    let store = Store::open(fixture.path()).unwrap();
    store.open_data::<Cell<Vec<u8>>>("sqlite-state").unwrap();
    let operation = materialize(&definition, &[value_schema()], &store, &["sqlite-state"]);
    assert!(!sqlite_path.exists());
    drop(operation);
}

struct Fixture {
    root: TestStore,
    definition: Vec<u8>,
    operation: Box<dyn Operation>,
    state: Cell<Vec<u8>>,
    transactions: Transactions,
}

impl Fixture {
    fn new() -> Self {
        let root = TestStore::new();
        let definition =
            SqliteSinkDefinition::try_new(root.path().with_extension("sqlite"), "events").unwrap();
        let mut store = Store::create(root.path()).unwrap();
        definition.data()[0].create(&mut store, "state").unwrap();
        Self::open(root, encode_definition(&definition), store)
    }

    fn open(root: TestStore, definition: Vec<u8>, store: Store) -> Self {
        let decoded = decode_definition(&definition).unwrap();
        let mut data = DataInstances::new();
        data.insert(decoded.data()[0].open(&store, "state").unwrap())
            .unwrap();
        let operation = decoded
            .bind(&[schema()])
            .unwrap()
            .materialize(data, RuntimeResource::none())
            .unwrap();
        let state = store.open_data("state").unwrap();
        Self {
            root,
            definition,
            operation,
            state,
            transactions: store.into_transactions(),
        }
    }

    fn reopen(self) -> Self {
        let Self {
            root,
            definition,
            operation,
            state,
            transactions,
        } = self;
        drop((operation, state, transactions));
        let store = Store::open(root.path()).unwrap();
        Self::open(root, definition, store)
    }

    fn sqlite_path(&self) -> PathBuf {
        self.root.path().with_extension("sqlite")
    }

    fn connection(&self) -> Connection {
        Connection::open_with_flags(self.sqlite_path(), OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap()
    }

    fn has_table(&self) -> bool {
        self.sqlite_path().exists()
            && self
                .connection()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='events')",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
    }

    fn rows(&self) -> Vec<(i64, i64)> {
        if !self.has_table() {
            return Vec::new();
        }
        self.connection()
            .prepare("SELECT \"$dogpaddle.id\", value FROM events ORDER BY \"$dogpaddle.id\"")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn state(&mut self) -> Option<Vec<u8>> {
        let transaction = self.transactions.begin();
        let state = self
            .state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap();
        transaction.commit().unwrap();
        state
    }

    fn try_commit(&mut self, change: &Change) -> Result<Action, OperationError> {
        commit_ready(
            self.operation.as_mut(),
            Some(input(change)),
            &mut self.transactions,
        )
    }

    fn commit(&mut self, change: &Change) -> Action {
        self.try_commit(change).unwrap()
    }

    fn rollback(&mut self, change: &Change) -> Action {
        rollback_ready(
            self.operation.as_mut(),
            Some(input(change)),
            &mut self.transactions,
        )
        .unwrap()
    }

    fn commit_without_completion(&mut self, change: &Change) -> Action {
        let Turn::Ready(turn) = self.operation.turn(Some(input(change))).unwrap() else {
            panic!("relation sink unexpectedly returned Idle");
        };
        let transaction = self.transactions.begin();
        let (action, completion) = turn.apply(transaction.access()).unwrap();
        transaction.commit().unwrap();
        drop(completion);
        action
    }

    fn initialize(&mut self, change: &Change) {
        for _ in 0..3 {
            assert!(matches!(self.commit(change), Action::Commit(None)));
        }
        assert!(self.has_table());
        assert!(self.rows().is_empty());
    }

    fn finish(&mut self, change: &Change) {
        for _ in 0..32 {
            match self.commit(change) {
                Action::Complete(None) => return,
                Action::Commit(None) => {}
                action => panic!("unexpected sink action {action:?}"),
            }
        }
        panic!("bounded fixture failed to finish");
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn change(values: &[i64], diffs: &[i64]) -> Change {
    let records =
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn input(change: &Change) -> OperationInput<'_> {
    OperationInput { port: 0, change }
}

#[test]
fn initialization_survives_rollback_lost_completion_and_repeated_reopen() {
    let input = change(&[7], &[1]);
    let mut fixture = Fixture::new();
    fixture.rollback(&input); // Discard the initial durable-state read.
    assert_eq!(fixture.state(), None);
    assert!(!fixture.sqlite_path().exists());
    fixture.commit(&input);
    fixture.rollback(&input); // Discard the initialization intent.
    assert_eq!(fixture.state(), None);
    assert!(!fixture.has_table());

    fixture.commit_without_completion(&input);
    let initialization = fixture.state();
    assert!(initialization.is_some());
    assert!(!fixture.has_table());
    for _ in 0..2 {
        fixture = fixture.reopen();
        fixture.commit(&input);
        assert!(fixture.has_table());
        assert!(fixture.rows().is_empty());
        assert_eq!(fixture.state(), initialization);
    }
    fixture.rollback(&input); // Discard initialization settlement.
    assert_eq!(fixture.state(), initialization);
    fixture.finish(&input);
    assert_eq!(fixture.rows(), [(1, 7)]);
}

#[test]
fn prepared_batches_recover_before_and_after_sqlite_commit_without_reallocating_ids() {
    let input = change(&[7], &[2]);
    let mut fixture = Fixture::new();
    fixture.initialize(&input);
    let ready = fixture.state();

    fixture.rollback(&input);
    assert_eq!(fixture.state(), ready);
    assert!(fixture.rows().is_empty());
    fixture.commit_without_completion(&input);
    let prepared = fixture.state();
    assert_ne!(prepared, ready);
    assert!(fixture.rows().is_empty());

    for _ in 0..3 {
        fixture = fixture.reopen();
        fixture.commit(&input);
        assert_eq!(fixture.rows(), [(1, 7), (2, 7)]);
        assert_eq!(fixture.state(), prepared);
    }
    assert!(matches!(fixture.rollback(&input), Action::Complete(None)));
    assert_eq!(fixture.state(), prepared);
    fixture = fixture.reopen();
    fixture.commit(&input);
    assert!(matches!(fixture.commit(&input), Action::Complete(None)));
    assert_ne!(fixture.state(), prepared);
    fixture.finish(&change(&[8], &[1]));
    assert_eq!(fixture.rows(), [(1, 7), (2, 7), (3, 8)]);
}

#[test]
fn a_batch_that_inserts_then_deletes_its_own_ids_is_idempotent_on_reopen() {
    let input = change(&[7, 7, 8, 7], &[2, -1, 1, -1]);
    let mut fixture = Fixture::new();
    fixture.initialize(&input);
    fixture.commit(&input);
    let prepared = fixture.state();
    assert_eq!(fixture.rows(), [(3, 8)]);
    for _ in 0..3 {
        fixture = fixture.reopen();
        fixture.commit(&input);
        assert_eq!(fixture.rows(), [(3, 8)]);
        assert_eq!(fixture.state(), prepared);
    }
    assert!(matches!(fixture.rollback(&input), Action::Complete(None)));
    assert_eq!(fixture.state(), prepared);
    assert!(matches!(fixture.commit(&input), Action::Complete(None)));
    fixture.finish(&change(&[7], &[1]));
    assert_eq!(fixture.rows(), [(3, 8), (4, 7)]);
}

#[test]
fn missing_negative_prefixes_do_not_write_or_consume_ids_and_same_instance_can_retry() {
    let mut fixture = Fixture::new();
    let invalid = change(&[7, 7], &[-1, 1]);
    fixture.initialize(&invalid);
    let ready = fixture.state();
    for invalid in [
        invalid,
        change(&[7, 8, 8], &[1, -1, 1]),
        change(&[7], &[i64::MIN]),
    ] {
        assert!(fixture.try_commit(&invalid).is_err());
        assert_eq!(fixture.state(), ready);
        assert!(fixture.rows().is_empty());
    }
    fixture.finish(&change(&[7], &[1]));
    assert_eq!(fixture.rows(), [(1, 7)]);
    fixture.finish(&change(&[7], &[-1]));
    assert!(fixture.rows().is_empty());
    fixture.finish(&change(&[8], &[1]));
    assert_eq!(fixture.rows(), [(2, 8)]);
}

#[test]
fn large_retractions_are_fully_admitted_and_continue_correctly_across_reopen() {
    let mut fixture = Fixture::new();
    fixture.finish(&change(&[7], &[2_050]));
    let ready = fixture.state();
    assert!(fixture.try_commit(&change(&[7], &[-2_051])).is_err());
    assert_eq!(fixture.state(), ready);
    assert_eq!(fixture.rows().len(), 2_050);

    let remove = change(&[7], &[-2_050]);
    fixture.commit(&remove);
    let prepared = fixture.state();
    assert_eq!(fixture.rows().len(), 1_026);
    fixture = fixture.reopen();
    fixture.commit(&remove);
    assert_eq!(fixture.rows().len(), 1_026);
    assert_eq!(fixture.state(), prepared);
    fixture.finish(&remove);
    assert!(fixture.rows().is_empty());
    fixture.finish(&change(&[8], &[1]));
    assert_eq!(fixture.rows(), [(2_051, 8)]);
}

#[test]
fn invalid_input_port_schema_and_missing_input_fail_before_external_io() {
    let mut fixture = Fixture::new();
    let input = change(&[7], &[1]);
    assert!(fixture.operation.turn(None).is_err());
    assert!(
        fixture
            .operation
            .turn(Some(OperationInput {
                port: 1,
                change: &input
            }))
            .is_err()
    );
    let wrong = Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "different",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    assert!(fixture.try_commit(&wrong).is_err());
    assert_eq!(fixture.state(), None);
    assert!(!fixture.sqlite_path().exists());
    fixture.finish(&input);
    assert_eq!(fixture.rows(), [(1, 7)]);
}

#[test]
fn after_commit_target_error_keeps_the_fixed_batch_for_reopen() {
    let mut fixture = Fixture::new();
    let input = change(&[7], &[1]);
    fixture.initialize(&input);
    let ready = fixture.state();
    fixture
        .connection()
        .execute_batch(
            "CREATE TRIGGER reject_insert BEFORE INSERT ON events \
         BEGIN SELECT RAISE(ABORT, 'injected target failure'); END;",
        )
        .unwrap();
    assert!(fixture.try_commit(&input).is_err());
    assert_ne!(fixture.state(), ready);
    assert!(fixture.rows().is_empty());
    fixture
        .connection()
        .execute_batch("DROP TRIGGER reject_insert")
        .unwrap();
    fixture = fixture.reopen();
    fixture.finish(&input);
    assert_eq!(fixture.rows(), [(1, 7)]);
}

#[test]
fn stable_rebatching_preserves_technical_ids_and_final_relation() {
    let mut whole = Fixture::new();
    whole.finish(&change(&[7, 8, 7, 7, 8], &[2, 1, -1, 1, -1]));
    let mut split = Fixture::new();
    for (value, diff) in [(7, 2), (8, 1), (7, -1), (7, 1), (8, -1)] {
        split.finish(&change(&[value], &[diff]));
    }
    assert_eq!(whole.rows(), [(2, 7), (4, 7)]);
    assert_eq!(whole.rows(), split.rows());
    assert_eq!(whole.state(), split.state());
}
