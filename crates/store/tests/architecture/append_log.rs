use std::{cell::Cell, num::NonZeroUsize};

use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};
use libmdbx::WriteFlags;

use crate::support::{raw_database, store_path};

const LOG_TABLE: &str = "d/00000000";

fn metadata(head: u64, tail: u64) -> [u8; 16] {
    let mut encoded = [0; 16];
    encoded[..8].copy_from_slice(&head.to_be_bytes());
    encoded[8..].copy_from_slice(&tail.to_be_bytes());
    encoded
}

#[test]
fn append_log_has_a_stable_dedicated_layout() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(
            access
                .append_batch(&[b"a".to_vec(), b"b".to_vec()])
                .unwrap(),
            0..2
        );
        access
            .truncate_before(2, NonZeroUsize::new(1).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    {
        let database = raw_database(&path);
        let transaction = database.begin_ro_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        assert_eq!(
            transaction.get::<Vec<u8>>(&table, &[]).unwrap(),
            Some(metadata(1, 2).to_vec())
        );
        assert_eq!(
            transaction
                .get::<Vec<u8>>(&table, &0_u64.to_be_bytes())
                .unwrap(),
            None
        );
        assert_eq!(
            transaction
                .get::<Vec<u8>>(&table, &1_u64.to_be_bytes())
                .unwrap(),
            Some(b"b".to_vec())
        );
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access
            .truncate_before(2, NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(access.append(&b"c".to_vec()).unwrap(), 2);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
    assert_eq!(
        transaction.get::<Vec<u8>>(&table, &[]).unwrap(),
        Some(metadata(2, 3).to_vec())
    );
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &1_u64.to_be_bytes())
            .unwrap(),
        None
    );
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &2_u64.to_be_bytes())
            .unwrap(),
        Some(b"c".to_vec())
    );
}

#[test]
fn an_empty_batch_does_not_materialize_log_metadata() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            log.access(transaction.access())
                .unwrap()
                .append_batch(&[])
                .unwrap(),
            0..0
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
    assert_eq!(transaction.get::<Vec<u8>>(&table, &[]).unwrap(), None);
}

#[test]
fn missing_or_invalid_metadata_is_corruption() {
    for invalid_metadata in [None, Some(vec![0; 15]), Some(metadata(2, 1).to_vec())] {
        let root = tempfile::tempdir().unwrap();
        let path = store_path(&root);
        let mut store = Store::create(&path).unwrap();
        store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
        drop(store);

        {
            let database = raw_database(&path);
            let transaction = database.begin_rw_txn().unwrap();
            let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
            if let Some(encoded) = invalid_metadata {
                transaction
                    .put(&table, [], &encoded, WriteFlags::UPSERT)
                    .unwrap();
            } else {
                transaction
                    .put(&table, 0_u64.to_be_bytes(), b"orphan", WriteFlags::UPSERT)
                    .unwrap();
            }
            assert!(!transaction.commit().unwrap());
        }

        let store = Store::open(&path).unwrap();
        let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        assert!(matches!(
            access.bounds(),
            Err(StoreError::CorruptAppendLog { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn a_gap_is_detected_before_any_scan_callback_runs() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access.append(&b"a".to_vec()).unwrap();
        access.append(&b"b".to_vec()).unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);
    {
        let database = raw_database(&path);
        let transaction = database.begin_rw_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        assert!(transaction.del(&table, 1_u64.to_be_bytes(), None).unwrap());
        assert!(!transaction.commit().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let called = Cell::new(false);
    assert!(matches!(
        access.scan::<StoreError>(0, ScanLimit::new(10, 1_024).unwrap(), |_| {
            called.set(true);
            Ok(())
        }),
        Err(StoreError::CorruptAppendLog { .. })
    ));
    assert!(!called.get());
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn truncation_gap_rolls_back_entries_deleted_before_the_failure() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    drop(store);
    {
        let database = raw_database(&path);
        let transaction = database.begin_rw_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        transaction
            .put(&table, [], metadata(0, 3), WriteFlags::UPSERT)
            .unwrap();
        transaction
            .put(&table, 0_u64.to_be_bytes(), b"a", WriteFlags::UPSERT)
            .unwrap();
        transaction
            .put(&table, 2_u64.to_be_bytes(), b"c", WriteFlags::UPSERT)
            .unwrap();
        assert!(!transaction.commit().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert!(matches!(
        access.truncate_before(3, NonZeroUsize::new(3).unwrap()),
        Err(StoreError::CorruptAppendLog { .. })
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &0_u64.to_be_bytes())
            .unwrap(),
        Some(b"a".to_vec())
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&table, &[]).unwrap(),
        Some(metadata(0, 3).to_vec())
    );
}

#[test]
fn scan_rejects_an_entry_at_the_recorded_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    drop(store);
    {
        let database = raw_database(&path);
        let transaction = database.begin_rw_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        transaction
            .put(&table, [], metadata(0, 1), WriteFlags::UPSERT)
            .unwrap();
        transaction
            .put(&table, 0_u64.to_be_bytes(), b"a", WriteFlags::UPSERT)
            .unwrap();
        transaction
            .put(&table, 1_u64.to_be_bytes(), b"extra", WriteFlags::UPSERT)
            .unwrap();
        assert!(!transaction.commit().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let called = Cell::new(false);
    assert!(matches!(
        access.scan::<StoreError>(0, ScanLimit::new(10, 1_024).unwrap(), |_| {
            called.set(true);
            Ok(())
        }),
        Err(StoreError::CorruptAppendLog { .. })
    ));
    assert!(!called.get());
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn tail_collision_and_offset_exhaustion_do_not_overwrite_data() {
    for (bounds, expected_error) in [
        (metadata(0, 0), "collision"),
        (metadata(u64::MAX, u64::MAX), "exhaustion"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = store_path(&root);
        let mut store = Store::create(&path).unwrap();
        store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
        drop(store);
        {
            let database = raw_database(&path);
            let transaction = database.begin_rw_txn().unwrap();
            let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
            transaction
                .put(&table, [], bounds, WriteFlags::UPSERT)
                .unwrap();
            if expected_error == "collision" {
                transaction
                    .put(&table, 0_u64.to_be_bytes(), b"existing", WriteFlags::UPSERT)
                    .unwrap();
            }
            assert!(!transaction.commit().unwrap());
        }

        let store = Store::open(&path).unwrap();
        let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        let error = access.append(&b"new".to_vec()).unwrap_err();
        if expected_error == "collision" {
            assert!(matches!(error, StoreError::CorruptAppendLog { .. }));
        } else {
            assert!(matches!(error, StoreError::LogOffsetExhausted));
        }
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn ordered_batch_append_rejects_a_physical_key_beyond_the_recorded_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    drop(store);
    {
        let database = raw_database(&path);
        let transaction = database.begin_rw_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        transaction
            .put(&table, [], metadata(0, 0), WriteFlags::UPSERT)
            .unwrap();
        transaction
            .put(&table, 1_u64.to_be_bytes(), b"future", WriteFlags::UPSERT)
            .unwrap();
        assert!(!transaction.commit().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert!(matches!(
        access.append_batch(&[b"a".to_vec(), b"b".to_vec()]),
        Err(StoreError::CorruptAppendLog { .. })
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &0_u64.to_be_bytes())
            .unwrap(),
        None
    );
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &1_u64.to_be_bytes())
            .unwrap(),
        Some(b"future".to_vec())
    );
}

#[test]
fn batch_offset_overflow_is_rejected_before_any_entry_is_written() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
    drop(store);
    {
        let database = raw_database(&path);
        let transaction = database.begin_rw_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        transaction
            .put(
                &table,
                [],
                metadata(u64::MAX - 1, u64::MAX - 1),
                WriteFlags::UPSERT,
            )
            .unwrap();
        assert!(!transaction.commit().unwrap());
    }

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert!(matches!(
        access.append_batch(&[b"a".to_vec(), b"b".to_vec()]),
        Err(StoreError::LogOffsetExhausted)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&table, &(u64::MAX - 1).to_be_bytes())
            .unwrap(),
        None
    );
}
