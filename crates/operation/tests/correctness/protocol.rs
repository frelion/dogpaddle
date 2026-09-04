use arrow_array::UInt64Array;
use dogpaddle_change::{Change, decode_change, encode_change};
use dogpaddle_operation::operation::{Action, Operation, Turn};
use dogpaddle_store::{AppendLog, Cell, ScanLimit, Store, StoreError, Transactions};

#[path = "../../examples/support/queue_source.rs"]
mod queue_source;

use queue_source::QueueSource;

use super::support::{TestStore, rollback_ready};

struct QueueFixture {
    source: QueueSource,
    checkpoint: Cell<u64>,
    output: AppendLog<Vec<u8>>,
    transactions: Transactions,
}

impl QueueFixture {
    fn create(path: &std::path::Path) -> Self {
        let mut store = Store::create(path).unwrap();
        store.create_data::<Cell<u64>>("checkpoint").unwrap();
        store.create_data::<AppendLog<Vec<u8>>>("output").unwrap();
        Self::from_store(store)
    }

    fn from_store(store: Store) -> Self {
        let checkpoint: Cell<u64> = store.open_data("checkpoint").unwrap();
        Self {
            source: QueueSource::new(checkpoint.clone()),
            checkpoint,
            output: store.open_data("output").unwrap(),
            transactions: store.into_transactions(),
        }
    }

    fn commit(&mut self) -> Action {
        let Turn::Ready(prepared) = self.source.turn(None).unwrap() else {
            return Action::Idle;
        };
        let transaction = self.transactions.begin().unwrap();
        let (action, after_commit) = prepared.apply(transaction.access()).unwrap();
        match &action {
            Action::Idle => return action,
            Action::Commit(Some(change)) => {
                self.output
                    .access(transaction.access())
                    .unwrap()
                    .append(&encode_change(change).unwrap())
                    .unwrap();
            }
            Action::Commit(None) => {}
            Action::Complete(_) => panic!("a source cannot complete an input"),
        }
        transaction.commit().unwrap();
        after_commit.run().unwrap();
        action
    }

    fn durable_state(&mut self) -> (Option<u64>, Vec<u64>) {
        let transaction = self.transactions.begin().unwrap();
        let checkpoint = self
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap();
        let mut values = Vec::new();
        let scan = self
            .output
            .access(transaction.access())
            .unwrap()
            .scan(
                0,
                ScanLimit::new(16, usize::MAX).unwrap(),
                |entry| -> Result<(), StoreError> {
                    let change = decode_change(&entry.decode_owned()?).unwrap();
                    values.push(value(&change));
                    Ok(())
                },
            )
            .unwrap();
        assert!(scan.caught_up);
        transaction.commit().unwrap();
        (checkpoint, values)
    }
}

fn value(change: &Change) -> u64 {
    assert_eq!(change.num_rows(), 1);
    assert_eq!(change.diffs().value(0), 1);
    change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

fn emitted(action: Action) -> u64 {
    let Action::Commit(Some(change)) = action else {
        panic!("expected one queue record");
    };
    value(&change)
}

#[test]
fn queue_initialization_is_published_only_after_commit() {
    let root = TestStore::new();
    let mut fixture = QueueFixture::create(root.path());

    // Neither abandoning preparation nor rolling back application initializes it.
    drop(fixture.source.turn(None).unwrap());
    assert!(matches!(
        rollback_ready(&mut fixture.source, None, &mut fixture.transactions).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(fixture.commit(), Action::Commit(None)));
    assert_eq!(fixture.durable_state(), (None, vec![]));
    assert_eq!(emitted(fixture.commit()), 10);
    assert_eq!(fixture.durable_state(), (Some(1), vec![10]));
}

#[test]
fn queue_replays_unacknowledged_work_then_advances_in_order() {
    let root = TestStore::new();
    let mut fixture = QueueFixture::create(root.path());
    assert!(matches!(fixture.commit(), Action::Commit(None)));

    for _ in 0..2 {
        let action = rollback_ready(&mut fixture.source, None, &mut fixture.transactions).unwrap();
        assert_eq!(emitted(action), 10);
        assert_eq!(fixture.durable_state(), (None, vec![]));
    }

    for expected in [10, 20, 30] {
        assert_eq!(emitted(fixture.commit()), expected);
    }
    assert!(matches!(fixture.source.turn(None).unwrap(), Turn::Idle));
    assert_eq!(fixture.durable_state(), (Some(3), vec![10, 20, 30]));
}

#[test]
fn queue_reopen_recovers_on_both_sides_of_commit_before_ack() {
    for committed in [false, true] {
        let root = TestStore::new();
        let mut fixture = QueueFixture::create(root.path());
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(emitted(fixture.commit()), 10);

        {
            let Turn::Ready(prepared) = fixture.source.turn(None).unwrap() else {
                panic!("second record was not available");
            };
            let transaction = fixture.transactions.begin().unwrap();
            let (Action::Commit(Some(change)), after_commit) =
                prepared.apply(transaction.access()).unwrap()
            else {
                panic!("second record was not staged");
            };
            assert_eq!(value(&change), 20);
            fixture
                .output
                .access(transaction.access())
                .unwrap()
                .append(&encode_change(&change).unwrap())
                .unwrap();
            if committed {
                transaction.commit().unwrap();
            } else {
                drop(transaction);
            }
            // Simulate losing the runtime before ACK, including after a local commit.
            drop(after_commit);
        }
        drop(fixture);

        let mut reopened = QueueFixture::from_store(Store::open(root.path()).unwrap());
        let expected_before = if committed {
            (Some(2), vec![10, 20])
        } else {
            (Some(1), vec![10])
        };
        assert_eq!(reopened.durable_state(), expected_before);
        assert!(matches!(reopened.commit(), Action::Commit(None)));
        if !committed {
            assert_eq!(emitted(reopened.commit()), 20);
        }
        assert_eq!(emitted(reopened.commit()), 30);
        assert!(matches!(reopened.source.turn(None).unwrap(), Turn::Idle));
        assert_eq!(reopened.durable_state(), (Some(3), vec![10, 20, 30]));
    }
}
