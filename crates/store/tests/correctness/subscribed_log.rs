use std::num::NonZeroU64;

use dogpaddle_store::{
    Cell, ReadTransactions, Store, StoreError, SubscribedLog, SubscribedLogStatus,
    SubscribedLogWriter, Subscription, SubscriptionStatus, Transactions,
};

use crate::support::store_path;

fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn initialize(
    log: &SubscribedLog<Vec<u8>>,
    transactions: &mut Transactions,
    subscriber_count: u64,
) {
    let transaction = transactions.begin();
    log.initialize(non_zero(subscriber_count), transaction.access())
        .unwrap();
    transaction.commit().unwrap();
}

fn commit_append(
    producer: &SubscribedLogWriter<Vec<u8>>,
    transactions: &mut Transactions,
    value: &[u8],
    capacity: u64,
) -> bool {
    let transaction = transactions.begin();
    let accepted = producer
        .try_append(&value.to_vec(), non_zero(capacity), transaction.access())
        .unwrap();
    transaction.commit().unwrap();
    accepted
}

fn commit_acknowledgement(
    subscription: &Subscription<Vec<u8>>,
    transactions: &mut Transactions,
    offset: u64,
) {
    let transaction = transactions.begin();
    subscription
        .acknowledge(offset, transaction.access())
        .unwrap();
    transaction.commit().unwrap();
}

fn peek(
    subscription: &Subscription<Vec<u8>>,
    transactions: &ReadTransactions,
) -> Option<(u64, Vec<u8>)> {
    let transaction = transactions.begin();
    subscription.peek(transaction.access()).unwrap()
}

fn status(
    producer: &SubscribedLogWriter<Vec<u8>>,
    transactions: &ReadTransactions,
) -> SubscribedLogStatus {
    let transaction = transactions.begin();
    producer.status(transaction.access()).unwrap()
}

#[test]
fn subscribers_advance_independently_and_the_slowest_one_controls_retention() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let producer = log.writer();
    let first = log.subscription(0);
    let second = log.subscription(1);
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 2);

    {
        let transaction = snapshots.begin();
        log.validate(non_zero(2), transaction.access()).unwrap();
    }
    {
        let transaction = transactions.begin();
        assert!(
            producer
                .try_append(&b"a".to_vec(), non_zero(100), transaction.access())
                .unwrap()
        );
        assert!(
            producer
                .try_append(&b"bb".to_vec(), non_zero(100), transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();
    }
    assert_eq!(peek(&first, &snapshots), Some((0, b"a".to_vec())));
    assert_eq!(peek(&second, &snapshots), Some((0, b"a".to_vec())));
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 0,
            tail: 2,
            retained_bytes: 19,
        }
    );

    commit_acknowledgement(&first, &mut transactions, 0);
    assert_eq!(peek(&first, &snapshots), Some((1, b"bb".to_vec())));
    assert_eq!(peek(&second, &snapshots), Some((0, b"a".to_vec())));
    assert_eq!(status(&producer, &snapshots).head, 0);
    assert_eq!(status(&producer, &snapshots).retained_bytes, 19);

    commit_acknowledgement(&second, &mut transactions, 0);
    assert_eq!(peek(&first, &snapshots), Some((1, b"bb".to_vec())));
    assert_eq!(peek(&second, &snapshots), Some((1, b"bb".to_vec())));
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 1,
            tail: 2,
            retained_bytes: 10,
        }
    );
}

#[test]
fn soft_capacity_admits_one_oversized_entry_only_when_the_backlog_is_empty() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let marker = store.create_data::<Cell<u64>>("marker").unwrap();
    let producer = log.writer();
    let subscription = log.subscription(0);
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 1);
    let oversized = vec![7; 20];

    assert!(commit_append(&producer, &mut transactions, &oversized, 1));
    {
        let transaction = transactions.begin();
        assert!(
            !producer
                .try_append(&Vec::new(), non_zero(28), transaction.access())
                .unwrap()
        );
        marker
            .access(transaction.access())
            .unwrap()
            .set(&7)
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = snapshots.begin();
        assert_eq!(
            marker.read(transaction.access()).unwrap().get().unwrap(),
            Some(7)
        );
    }
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 0,
            tail: 1,
            retained_bytes: 28,
        }
    );

    commit_acknowledgement(&subscription, &mut transactions, 0);
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 1,
            tail: 1,
            retained_bytes: 0,
        }
    );
    assert!(commit_append(&producer, &mut transactions, &oversized, 1));
    assert_eq!(peek(&subscription, &snapshots), Some((1, oversized)));
}

#[test]
fn offsets_and_subscriber_positions_survive_reopen_without_resetting() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let producer = log.writer();
    let first = log.subscription(0);
    let second = log.subscription(1);
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 2);

    {
        let transaction = transactions.begin();
        for value in [10_u8, 20, 30] {
            assert!(
                producer
                    .try_append(&vec![value], non_zero(100), transaction.access())
                    .unwrap()
            );
        }
        transaction.commit().unwrap();
    }

    // This second acknowledgement must read the first position update from
    // the same RocksDB transaction.
    {
        let transaction = transactions.begin();
        first.acknowledge(0, transaction.access()).unwrap();
        first.acknowledge(1, transaction.access()).unwrap();
        transaction.commit().unwrap();
    }
    commit_acknowledgement(&second, &mut transactions, 0);
    drop(snapshots);
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log = store
        .open_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let producer = log.writer();
    let first = log.subscription(0);
    let second = log.subscription(1);
    {
        let transaction = store.read_transaction();
        let access = transaction.access();
        log.validate(non_zero(2), access).unwrap();
        assert_eq!(
            first.status(access).unwrap(),
            SubscriptionStatus {
                position: 2,
                tail: 3,
            }
        );
        assert_eq!(
            second.status(access).unwrap(),
            SubscriptionStatus {
                position: 1,
                tail: 3,
            }
        );
        assert_eq!(
            producer.status(access).unwrap(),
            SubscribedLogStatus {
                head: 1,
                tail: 3,
                retained_bytes: 18,
            }
        );
    }
    let (mut transactions, snapshots) = store.into_transactions().split();

    commit_acknowledgement(&second, &mut transactions, 1);
    {
        let transaction = transactions.begin();
        first.acknowledge(2, transaction.access()).unwrap();
        second.acknowledge(2, transaction.access()).unwrap();
        transaction.commit().unwrap();
    }
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 3,
            tail: 3,
            retained_bytes: 0,
        }
    );

    assert!(commit_append(&producer, &mut transactions, &[40], 1));
    assert_eq!(peek(&first, &snapshots), Some((3, vec![40])));
    assert_eq!(peek(&second, &snapshots), Some((3, vec![40])));
}

#[test]
fn append_and_acknowledgement_roll_back_with_their_transactions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let producer = log.writer();
    let subscription = log.subscription(0);
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 1);
    assert!(commit_append(
        &producer,
        &mut transactions,
        b"committed",
        100
    ));

    {
        let transaction = transactions.begin();
        assert!(
            producer
                .try_append(&b"dropped".to_vec(), non_zero(100), transaction.access())
                .unwrap()
        );
    }
    assert_eq!(status(&producer, &snapshots).tail, 1);
    assert_eq!(
        peek(&subscription, &snapshots),
        Some((0, b"committed".to_vec()))
    );

    {
        let transaction = transactions.begin();
        subscription.acknowledge(0, transaction.access()).unwrap();
    }
    assert_eq!(
        status(&producer, &snapshots),
        SubscribedLogStatus {
            head: 0,
            tail: 1,
            retained_bytes: 17,
        }
    );
    assert_eq!(
        peek(&subscription, &snapshots),
        Some((0, b"committed".to_vec()))
    );
}

#[test]
fn acknowledgement_mismatch_poisons_and_rolls_back_other_state() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let marker = store.create_data::<Cell<u64>>("marker").unwrap();
    let producer = log.writer();
    let subscription = log.subscription(0);
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 1);
    assert!(commit_append(&producer, &mut transactions, b"entry", 100));

    {
        let transaction = transactions.begin();
        marker
            .access(transaction.access())
            .unwrap()
            .set(&42)
            .unwrap();
        assert!(matches!(
            subscription.acknowledge(1, transaction.access()),
            Err(StoreError::SubscriptionPositionMismatch {
                subscriber: 0,
                expected: 1,
                actual: 0,
            })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }

    let transaction = snapshots.begin();
    let access = transaction.access();
    assert_eq!(marker.read(access).unwrap().get().unwrap(), None);
    assert_eq!(
        subscription.peek(access).unwrap(),
        Some((0, b"entry".to_vec()))
    );
}

#[test]
fn fixed_subscriber_boundaries_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let (mut transactions, snapshots) = store.into_transactions().split();
    initialize(&log, &mut transactions, 2);

    {
        let transaction = snapshots.begin();
        log.validate(non_zero(2), transaction.access()).unwrap();
    }
    {
        let transaction = snapshots.begin();
        assert!(matches!(
            log.validate(non_zero(1), transaction.access()),
            Err(StoreError::SubscriberCountMismatch {
                expected: 1,
                actual: 2,
            })
        ));
    }
    {
        let transaction = snapshots.begin();
        assert!(matches!(
            log.subscription(2).peek(transaction.access()),
            Err(StoreError::SubscriberOutOfRange {
                subscriber: 2,
                subscriber_count: 2,
            })
        ));
    }
    {
        let transaction = transactions.begin();
        assert!(matches!(
            log.subscription(0).acknowledge(0, transaction.access()),
            Err(StoreError::SubscriptionAtTail {
                subscriber: 0,
                tail: 0,
            })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn subscribed_log_has_its_own_persistent_collection_kind() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();

    assert!(matches!(
        store.open_data::<Cell<Vec<u8>>>("changes"),
        Err(StoreError::DataKindMismatch {
            expected: "cell",
            actual: "subscribed log",
            ..
        })
    ));
}
