use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    operation::{
        Operation, OperationError, SinkPrepared,
        sink::{SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkSchemaError},
    },
};
use dogpaddle_store::{Cell, OrderedMap, ReadTransactions, Store, StoreSetup, Transactions};
use rusqlite::{Connection, OpenFlags};

use super::support::{TestStore, assert_literal_definition, construct_checked, value_schema};

const SQLITE_SINK_V1: &str = include_str!("../fixtures/v1/sqlite_sink_output_events.hex");

#[test]
fn sqlite_sink_definition_has_stable_v1_literal_and_public_contract() {
    let sqlite =
        SqliteSinkDefinition::try_new("/var/lib/dogpaddle/output.sqlite", "events").unwrap();
    let decoded = assert_literal_definition(
        &sqlite,
        SQLITE_SINK_V1,
        OperationKind::Sink(std::num::NonZeroU32::MIN),
    );
    assert_eq!(
        sqlite.database_path(),
        Path::new("/var/lib/dogpaddle/output.sqlite")
    );
    assert_eq!(sqlite.table_name(), "events");
    assert!(
        construct_checked(&decoded, &[value_schema()])
            .unwrap()
            .is_none()
    );

    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&sqlite.clone().into())
            .unwrap();
    assert_eq!(
        &encoded[..],
        br#"{"sqlite_sink":{"database_path":"/var/lib/dogpaddle/output.sqlite","table_name":"events"}}"#
    );
}

#[test]
fn raw_sqlite_plan_rejects_a_relative_path_during_binding() {
    let forged = r#"{"database_path":"relative.sqlite","table_name":"events"}"#;
    let plan = serde_json::from_str::<SqliteSinkDefinition>(forged).unwrap();
    assert!(
        OperationDefinition::from(plan)
            .output_schema(&[value_schema()])
            .is_err()
    );
}

#[test]
fn sqlite_sink_codec_checks_structure_and_binding_checks_paths() {
    let canonical = serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(
        &SqliteSinkDefinition::try_new("/var/lib/dogpaddle/output.sqlite", "events")
            .unwrap()
            .into(),
    )
    .unwrap();
    let payload = std::str::from_utf8(&canonical[..]).unwrap();
    for field in ["database_path", "table_name"] {
        let mut missing = Vec::new();
        missing.extend_from_slice(
            payload
                .replacen(
                    &format!("\"{field}\":"),
                    &format!("\"unknown_{field}\":"),
                    1,
                )
                .as_bytes(),
        );
        assert!(
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&missing)
                .unwrap_err()
                .is_data()
        );
    }

    for value in [b"/var/lib/dogpaddle/output.sqlite".as_slice(), b"events"] {
        let mut invalid_utf8 = canonical.clone();
        let offset = invalid_utf8
            .windows(value.len())
            .position(|window| window == value)
            .unwrap();
        invalid_utf8[offset] = u8::MAX;
        assert!(
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&invalid_utf8)
                .is_err()
        );
    }

    let wrap = |path: &str, table: &str| {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(
            format!(
                r#"{{"sqlite_sink":{{"database_path":{},"table_name":{}}}}}"#,
                serde_json::to_string(path).unwrap(),
                serde_json::to_string(table).unwrap()
            )
            .as_bytes(),
        );
        encoded
    };
    for invalid in [
        wrap("relative.sqlite", "events"),
        wrap(":memory:", "events"),
        wrap("/tmp/invalid\0.sqlite", "events"),
        wrap("/tmp/output.sqlite", ""),
        wrap("/tmp/output.sqlite", "bad\0table"),
        wrap("/tmp/output.sqlite", "SQLITE_reserved"),
    ] {
        let plan =
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&invalid).unwrap();
        assert!(plan.output_schema(&[value_schema()]).is_err());
    }
}

#[test]
fn sqlite_sink_decoder_never_panics_for_arbitrary_json_payloads() {
    let mut state = 0x3c6e_f372_fe94_f82b_u64;
    for length in 0..=256 {
        let mut input = Vec::new();
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            input.push(state.to_le_bytes()[0]);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&input)
        }));
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
        construct_checked(&definition, std::slice::from_ref(&empty))
            .unwrap()
            .is_none()
    );

    let empty_name = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, true)]));
    assert!(
        construct_checked(&definition, std::slice::from_ref(&empty_name))
            .unwrap()
            .is_none()
    );

    let maximum = Arc::new(Schema::new(
        (0..1_998)
            .map(|index| Field::new(format!("field_{index}"), DataType::Null, true))
            .collect::<Vec<_>>(),
    ));
    assert!(
        construct_checked(&definition, std::slice::from_ref(&maximum))
            .unwrap()
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
        construct_checked(&definition, std::slice::from_ref(&temporal_and_decimal))
            .unwrap()
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
        construct_checked(&definition, std::slice::from_ref(&too_many))
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
        construct_checked(&definition, std::slice::from_ref(&nul))
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
        construct_checked(&definition, std::slice::from_ref(&duplicate))
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
            construct_checked(&definition, std::slice::from_ref(&collision))
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
    assert!(matches!(
        OperationDefinition::from(definition.clone())
            .validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::UnexpectedRuntimeResource)
    ));
    let mut setup = StoreSetup::new();
    let (operation, output) = OperationDefinition::from(definition.clone())
        .construct(
            &[value_schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    assert!(!sqlite_path.exists());
    let transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));

    let store = Store::open(fixture.path()).unwrap();
    store
        .open_data::<Cell<Vec<u8>>>("operation/sink.control")
        .unwrap();
    store
        .open_data::<OrderedMap<u64, Vec<u8>>>("operation/sink.buffer")
        .unwrap();
    let (operation, output) = OperationDefinition::from(definition.clone())
        .construct(
            &[value_schema()],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    assert!(!sqlite_path.exists());
    drop(operation);
}

struct Fixture {
    input_schema: SchemaRef,
    root: TestStore,
    definition: OperationDefinition,
    sink: Box<dyn dogpaddle_operation::operation::SinkOperation>,
    writes: Transactions,
    reads: ReadTransactions,
}
impl Fixture {
    fn new() -> Self {
        Self::with_schema(schema())
    }
    fn with_schema(input_schema: SchemaRef) -> Self {
        let root = TestStore::new();
        let definition: OperationDefinition =
            SqliteSinkDefinition::try_new(root.path().with_extension("sqlite"), "events")
                .unwrap()
                .into();
        let mut setup = StoreSetup::new();
        let (operation, _) = definition
            .construct(
                &[Arc::clone(&input_schema)],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts();
        let (writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
        let Operation::Sink(sink) = operation else {
            panic!("expected sink");
        };
        Self {
            input_schema,
            root,
            definition,
            sink,
            writes,
            reads,
        }
    }
    fn reopen(self) -> Self {
        let Self {
            input_schema,
            root,
            definition,
            sink,
            writes,
            reads,
        } = self;
        drop((sink, writes, reads));
        let store = Store::open(root.path()).unwrap();
        let (operation, _) = definition
            .construct(
                &[Arc::clone(&input_schema)],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts();
        let (writes, reads) = store.into_transactions().split();
        let Operation::Sink(sink) = operation else {
            panic!("expected sink");
        };
        Self {
            input_schema,
            root,
            definition,
            sink,
            writes,
            reads,
        }
    }
    fn enqueue(&mut self, change: &Change) -> Result<bool, OperationError> {
        let txn = self.writes.begin();
        let admitted = self.sink.try_enqueue(txn.access(), change)?;
        if admitted {
            txn.commit()?;
        }
        Ok(admitted)
    }
    fn plan(&mut self) -> Result<Option<SinkPrepared>, OperationError> {
        let pending = {
            let snapshot = self.reads.begin();
            self.sink.load(snapshot.access())?
        };
        pending
            .map(|pending| self.sink.prepare(pending))
            .transpose()
    }
    fn persist(&mut self, prepared: &SinkPrepared) {
        let txn = self.writes.begin();
        self.sink.persist_prepared(txn.access(), prepared).unwrap();
        txn.commit().unwrap();
    }
    fn settle(&mut self, prepared: &SinkPrepared) {
        let txn = self.writes.begin();
        self.sink.settle(txn.access(), prepared).unwrap();
        txn.commit().unwrap();
    }
    fn drain(&mut self) -> usize {
        let mut batches = 0;
        while let Some(prepared) = self.plan().unwrap() {
            self.persist(&prepared);
            self.sink.deliver(&prepared).unwrap();
            self.settle(&prepared);
            batches += 1;
        }
        batches
    }
    fn rows(&self) -> Vec<(i64, i64)> {
        let connection = Connection::open_with_flags(
            self.root.path().with_extension("sqlite"),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .unwrap();
        connection
            .prepare("SELECT \"$dogpaddle.id\", value FROM events ORDER BY \"$dogpaddle.id\"")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
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
    Change::try_new(
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap(),
        Int64Array::from(diffs.to_vec()),
    )
    .unwrap()
}
#[test]
fn initialization_intent_survives_reopen_and_is_idempotent() {
    let mut fixture = Fixture::new();
    assert!(!fixture.enqueue(&change(&[7], &[1])).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    {
        let txn = fixture.writes.begin();
        fixture
            .sink
            .persist_prepared(txn.access(), &prepared)
            .unwrap();
    }
    assert!(
        !fixture.root.path().with_extension("sqlite").exists()
            || fixture.rows_if_initialized().is_none()
    );
    fixture.persist(&prepared);
    fixture = fixture.reopen();
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture = fixture.reopen();
    assert_eq!(fixture.drain(), 1);
    assert!(fixture.rows().is_empty());
}
impl Fixture {
    fn rows_if_initialized(&self) -> Option<i64> {
        Connection::open(self.root.path().with_extension("sqlite"))
            .unwrap()
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .ok()
    }
}
#[test]
fn capture_parent_and_enqueue_roll_back_together() {
    let mut fixture = Fixture::new();
    fixture.drain();
    {
        let txn = fixture.writes.begin();
        assert!(
            fixture
                .sink
                .try_enqueue(txn.access(), &change(&[7], &[3]))
                .unwrap()
        );
    }
    assert!(fixture.plan().unwrap().is_none());
    assert!(fixture.rows().is_empty());
}
#[test]
fn prepared_replay_before_and_after_target_commit_keeps_fixed_ids() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7], &[3])).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture = fixture.reopen();
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    assert_eq!(
        fixture.rows(),
        [(i64::MIN + 1, 7), (i64::MIN + 2, 7), (i64::MIN + 3, 7)]
    );
    fixture = fixture.reopen();
    assert_eq!(fixture.drain(), 1);
    assert_eq!(
        fixture.rows(),
        [(i64::MIN + 1, 7), (i64::MIN + 2, 7), (i64::MIN + 3, 7)]
    );
}

#[test]
fn prepared_batch_backpressure_writes_nothing_when_the_parent_transaction_commits() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7], &[1024])).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture = fixture.reopen();
    // Restore validates the complete durable plan before input is serviced.
    assert!(fixture.plan().unwrap().is_some());
    {
        let txn = fixture.writes.begin();
        assert!(
            !fixture
                .sink
                .try_enqueue(txn.access(), &change(&[99], &[1]))
                .unwrap()
        );
        // A caller may commit its own pending-page progress after a false result.
        txn.commit().unwrap();
    }
    fixture = fixture.reopen();
    assert_eq!(fixture.drain(), 1);
    assert_eq!(
        fixture.rows(),
        (1..=1024).map(|id| (i64::MIN + id, 7)).collect::<Vec<_>>()
    );
    assert!(fixture.enqueue(&change(&[99], &[1])).unwrap());
    assert_eq!(fixture.drain(), 1);
    assert_eq!(fixture.rows().last(), Some(&(i64::MIN + 1025, 99)));
}

#[test]
fn restoring_oversized_ready_control_fails_without_rewriting_it() {
    let mut fixture = Fixture::new();
    fixture.drain();
    let Fixture {
        input_schema,
        root,
        definition,
        sink,
        writes,
        reads,
    } = fixture;
    drop((sink, writes, reads));
    let store = Store::open(root.path()).unwrap();
    let control: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
    let mut invalid = control
        .read(store.read_transaction().access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap();
    invalid.resize(256, 0);
    let mut writes = store.into_transactions();
    {
        let txn = writes.begin();
        control.access(txn.access()).unwrap().set(&invalid).unwrap();
        txn.commit().unwrap();
    }
    drop((control, writes));
    let store = Store::open(root.path()).unwrap();
    let (operation, _) = definition
        .construct(
            &[input_schema],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let Operation::Sink(mut sink) = operation else {
        panic!("expected sink");
    };
    let control: Cell<Vec<u8>> = store.open_data("operation/sink.control").unwrap();
    let read = store.read_transaction();
    assert!(sink.load(read.access()).is_err());
    assert_eq!(
        control.read(read.access()).unwrap().get().unwrap(),
        Some(invalid)
    );
}
#[test]
fn small_entries_merge_into_one_target_batch_and_mixed_prefixes_use_new_ids() {
    let mut fixture = Fixture::new();
    fixture.drain();
    for _ in 0..3 {
        assert!(fixture.enqueue(&change(&[7], &[1])).unwrap());
    }
    assert!(fixture.enqueue(&change(&[7], &[-3])).unwrap());
    assert_eq!(fixture.drain(), 1);
    assert!(fixture.rows().is_empty());
    assert!(fixture.enqueue(&change(&[7, 7], &[3, -4])).unwrap());
    assert!(fixture.plan().is_err());
    assert!(fixture.rows().is_empty());
}
#[test]
fn invalid_later_negative_slice_keeps_previously_settled_slices() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7], &[1025])).unwrap());
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7], &[-1026])).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture.settle(&prepared);
    assert_eq!(fixture.rows().len(), 1);
    fixture = fixture.reopen();
    assert!(fixture.plan().is_err());
    assert_eq!(fixture.rows().len(), 1);
}
#[test]
fn schema_and_oversized_multiplicity_fail_before_enqueue() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7], &[i64::MAX])).is_err());
    let wrong = super::support::change(&[1]);
    assert!(fixture.enqueue(&wrong).is_err());
    assert!(fixture.plan().unwrap().is_none());
}

#[test]
fn event_positions_keep_negative_gaps_and_survive_an_empty_reopen() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7, 7, 9], &[2, -1, 1])).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture = fixture.reopen();
    assert_eq!(fixture.drain(), 1);
    assert_eq!(fixture.rows(), [(i64::MIN + 2, 7), (i64::MIN + 4, 9)]);

    assert!(fixture.enqueue(&change(&[7, 9], &[-1, -1])).unwrap());
    fixture.drain();
    assert!(fixture.rows().is_empty());
    fixture = fixture.reopen();
    assert!(fixture.plan().unwrap().is_none());
    assert!(fixture.enqueue(&change(&[42], &[1])).unwrap());
    fixture.drain();
    assert_eq!(fixture.rows(), [(i64::MIN + 7, 42)]);
}

#[test]
fn repeated_load_and_rolled_back_settlement_follow_the_store_head() {
    let mut fixture = Fixture::new();
    fixture.drain();
    assert!(fixture.enqueue(&change(&[7, 8, 9], &[1023, 2, 2])).unwrap());
    drop(fixture.plan().unwrap().unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    {
        let txn = fixture.writes.begin();
        fixture.sink.settle(txn.access(), &prepared).unwrap();
    }
    assert_eq!(fixture.rows().len(), 1024);
    drop(fixture.plan().unwrap().unwrap());
    fixture = fixture.reopen();
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture.settle(&prepared);
    assert_eq!(fixture.drain(), 1);
    let rows = fixture.rows();
    assert_eq!(rows.len(), 1027);
    assert_eq!(
        &rows[1022..],
        &[
            (i64::MIN + 1023, 7),
            (i64::MIN + 1024, 8),
            (i64::MIN + 1025, 8),
            (i64::MIN + 1026, 9),
            (i64::MIN + 1027, 9),
        ]
    );
}

#[test]
fn retained_birth_comparison_replays_wide_rows_without_duplicate_payload_budget() {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let payload = "x".repeat(3 * 1024 * 1024);
    let change = Change::try_new(
        RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(StringArray::from(vec![payload.as_str(); 2]))],
        )
        .unwrap(),
        Int64Array::from(vec![3, -1]),
    )
    .unwrap();
    let mut fixture = Fixture::with_schema(input_schema);
    fixture.drain();
    assert!(fixture.enqueue(&change).unwrap());
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture.settle(&prepared);
    let prepared = fixture.plan().unwrap().unwrap();
    fixture.persist(&prepared);
    fixture.sink.deliver(&prepared).unwrap();
    fixture = fixture.reopen();
    assert_eq!(fixture.drain(), 1);
    let connection = Connection::open(fixture.root.path().with_extension("sqlite")).unwrap();
    let rows = connection
        .prepare("SELECT \"$dogpaddle.id\", length(value) FROM events ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (i64::MIN + 2, 3 * 1024 * 1024),
            (i64::MIN + 3, 3 * 1024 * 1024)
        ]
    );
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(
        SqliteSinkDefinition::try_new("/tmp/events.sqlite", "events").unwrap(),
    )
    .unwrap();
    payload["database_path"] = serde_json::json!("relative.sqlite");
    let plan: OperationDefinition =
        serde_json::from_value(serde_json::json!({"sqlite_sink": payload})).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[value_schema()],
        RuntimeResource::none(),
    );
}
