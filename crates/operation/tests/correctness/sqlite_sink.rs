use std::{path::PathBuf, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, OperationDefinition, RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn, sink::SqliteSinkDefinition,
    },
};
use dogpaddle_store::{Cell, Store, Transactions};
use rusqlite::{Connection, OpenFlags};

use super::support::{TestStore, commit_ready, rollback_ready};

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
        let transaction = self.transactions.begin().unwrap();
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
        let transaction = self.transactions.begin().unwrap();
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
