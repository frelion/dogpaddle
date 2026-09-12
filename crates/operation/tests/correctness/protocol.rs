use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow_array::UInt64Array;
use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
    TurnOperation,
};
use dogpaddle_store::{
    Cell, ReadTransactions, Store, SubscribedLog, SubscribedLogWriter, Transactions,
};

#[path = "../../examples/support/queue_scan.rs"]
mod queue_scan;

use queue_scan::QueueScan;

use super::support::{TestStore, rollback_ready};

#[test]
fn post_commit_error_accepts_an_already_erased_operation_error() {
    let source: OperationError = Box::new(std::io::Error::other("erased failure"));
    assert_eq!(PostCommitError::from(source).to_string(), "erased failure");
}

struct BorrowedDeliveryConnector {
    acknowledgements: Arc<AtomicUsize>,
}

impl BorrowedDeliveryConnector {
    fn poll(&mut self) -> BorrowedDelivery<'_> {
        BorrowedDelivery { connector: self }
    }
}

struct BorrowedDelivery<'connector> {
    connector: &'connector mut BorrowedDeliveryConnector,
}

impl BorrowedDelivery<'_> {
    fn ack(self) {
        self.connector
            .acknowledgements
            .fetch_add(1, Ordering::Relaxed);
    }
}

struct BorrowedDeliveryScan {
    accepted: Cell<u64>,
    connector: BorrowedDeliveryConnector,
}

impl TurnOperation for BorrowedDeliveryScan {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        assert!(input.is_none());
        let accepted = self.accepted.clone();
        let delivery = self.connector.poll();
        Ok(Turn::ready(move |access| {
            accepted.access(access)?.set(&7)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    delivery.ack();
                    Ok(())
                }),
            ))
        }))
    }
}

#[test]
fn a_borrowed_delivery_crosses_the_transaction_and_is_only_acked_after_commit() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let accepted = store.create_data::<Cell<u64>>("accepted").unwrap();
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let mut operation = BorrowedDeliveryScan {
        accepted: accepted.clone(),
        connector: BorrowedDeliveryConnector {
            acknowledgements: Arc::clone(&acknowledgements),
        },
    };
    let mut transactions = store.into_transactions();

    {
        let Turn::Ready(prepared) = operation.turn(None).unwrap() else {
            panic!("delivery Scan did not prepare its polled delivery");
        };
        let transaction = transactions.begin();
        let (Action::Commit(None), after_commit) = prepared.apply(transaction.access()).unwrap()
        else {
            panic!("delivery Scan did not stage its checkpoint");
        };
        drop(transaction);
        drop(after_commit);
    }
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    {
        let transaction = transactions.begin();
        assert_eq!(
            accepted
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        transaction.commit().unwrap();
    }

    let Turn::Ready(prepared) = operation.turn(None).unwrap() else {
        panic!("delivery Scan did not prepare the replayed delivery");
    };
    let transaction = transactions.begin();
    let (Action::Commit(None), after_commit) = prepared.apply(transaction.access()).unwrap() else {
        panic!("delivery Scan did not stage its replayed checkpoint");
    };
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    transaction.commit().unwrap();
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    after_commit.run().unwrap();
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 1);

    let transaction = transactions.begin();
    assert_eq!(
        accepted
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(7)
    );
    transaction.commit().unwrap();
}

struct QueueFixture {
    scan: Operation,
    checkpoint: Cell<u64>,
    output: SubscribedLogWriter<Vec<u8>>,
    transactions: Transactions,
    reads: ReadTransactions,
}

impl QueueFixture {
    fn create(path: &std::path::Path) -> Self {
        let mut store = Store::create(path).unwrap();
        store.create_data::<Cell<u64>>("checkpoint").unwrap();
        let output = store
            .create_data::<SubscribedLog<Vec<u8>>>("output")
            .unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        output
            .initialize(std::num::NonZeroU64::MIN, transaction.access())
            .unwrap();
        transaction.commit().unwrap();
        drop(transactions);
        Self::from_store(Store::open(path).unwrap())
    }

    fn from_store(store: Store) -> Self {
        let checkpoint: Cell<u64> = store.open_data("checkpoint").unwrap();
        let output: SubscribedLog<Vec<u8>> = store.open_data("output").unwrap();
        let snapshot = store.read_transaction();
        output
            .validate(std::num::NonZeroU64::MIN, snapshot.access())
            .unwrap();
        drop(snapshot);
        let output = output.writer();
        let (transactions, reads) = store.into_transactions().split();
        Self {
            scan: Operation::Turn(Box::new(QueueScan::new(checkpoint.clone()))),
            checkpoint,
            output,
            transactions,
            reads,
        }
    }

    fn commit(&mut self) -> Action {
        let Turn::Ready(prepared) = self.scan.turn(None).unwrap() else {
            return Action::Idle;
        };
        let transaction = self.transactions.begin();
        let (action, after_commit) = prepared.apply(transaction.access()).unwrap();
        match &action {
            Action::Idle => return action,
            Action::Commit(Some(change)) => {
                assert!(
                    self.output
                        .try_append(
                            &encode_change(change).unwrap(),
                            std::num::NonZeroU64::MAX,
                            transaction.access(),
                        )
                        .unwrap()
                );
            }
            Action::Commit(None) => {}
            Action::Complete(_) => panic!("a Scan cannot complete an input"),
        }
        transaction.commit().unwrap();
        after_commit.run().unwrap();
        action
    }

    fn durable_state(&self) -> (Option<u64>, u64) {
        let transaction = self.reads.begin();
        let checkpoint = self
            .checkpoint
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap();
        let tail = self.output.status(transaction.access()).unwrap().tail;
        (checkpoint, tail)
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
    drop(fixture.scan.turn(None).unwrap());
    assert!(matches!(
        rollback_ready(&mut fixture.scan, None, &mut fixture.transactions).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(fixture.commit(), Action::Commit(None)));
    assert_eq!(fixture.durable_state(), (None, 0));
    assert_eq!(emitted(fixture.commit()), 10);
    assert_eq!(fixture.durable_state(), (Some(1), 1));
}

#[test]
fn queue_replays_unacknowledged_work_then_advances_in_order() {
    let root = TestStore::new();
    let mut fixture = QueueFixture::create(root.path());
    assert!(matches!(fixture.commit(), Action::Commit(None)));

    for _ in 0..2 {
        let action = rollback_ready(&mut fixture.scan, None, &mut fixture.transactions).unwrap();
        assert_eq!(emitted(action), 10);
        assert_eq!(fixture.durable_state(), (None, 0));
    }

    for expected in [10, 20, 30] {
        assert_eq!(emitted(fixture.commit()), expected);
    }
    assert!(matches!(fixture.scan.turn(None).unwrap(), Turn::Idle));
    assert_eq!(fixture.durable_state(), (Some(3), 3));
}

#[test]
fn queue_reopen_recovers_on_both_sides_of_commit_before_ack() {
    for committed in [false, true] {
        let root = TestStore::new();
        let mut fixture = QueueFixture::create(root.path());
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(emitted(fixture.commit()), 10);

        {
            let Turn::Ready(prepared) = fixture.scan.turn(None).unwrap() else {
                panic!("second record was not available");
            };
            let transaction = fixture.transactions.begin();
            let (Action::Commit(Some(change)), after_commit) =
                prepared.apply(transaction.access()).unwrap()
            else {
                panic!("second record was not staged");
            };
            assert_eq!(value(&change), 20);
            assert!(
                fixture
                    .output
                    .try_append(
                        &encode_change(&change).unwrap(),
                        std::num::NonZeroU64::MAX,
                        transaction.access(),
                    )
                    .unwrap()
            );
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
            (Some(2), 2)
        } else {
            (Some(1), 1)
        };
        assert_eq!(reopened.durable_state(), expected_before);
        assert!(matches!(reopened.commit(), Action::Commit(None)));
        if !committed {
            assert_eq!(emitted(reopened.commit()), 20);
        }
        assert_eq!(emitted(reopened.commit()), 30);
        assert!(matches!(reopened.scan.turn(None).unwrap(), Turn::Idle));
        assert_eq!(reopened.durable_state(), (Some(3), 3));
    }
}
