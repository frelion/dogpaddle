use std::{cell::Cell, num::NonZeroUsize, path::PathBuf};

use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError, Transactions};
use libmdbx::WriteFlags;

use crate::support::{raw_database, store_path};

const LOG_TABLE: &str = "d/00000000";

fn metadata(head: u64, tail: u64, retained_bytes: u64) -> [u8; 24] {
    let mut encoded = [0; 24];
    encoded[..8].copy_from_slice(&head.to_be_bytes());
    encoded[8..16].copy_from_slice(&tail.to_be_bytes());
    encoded[16..].copy_from_slice(&retained_bytes.to_be_bytes());
    encoded
}

fn entry(offset: u64, value: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (offset.to_be_bytes().to_vec(), value.to_vec())
}

struct RawLog {
    _root: tempfile::TempDir,
    path: PathBuf,
}

impl RawLog {
    fn empty() -> Self {
        Self::with_entries(Vec::new())
    }

    fn with_entries(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = store_path(&root);
        let mut store = Store::create(&path).unwrap();
        store.create_data::<AppendLog<Vec<u8>>>("log").unwrap();
        drop(store);

        if !entries.is_empty() {
            let database = raw_database(&path);
            let transaction = database.begin_rw_txn().unwrap();
            let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
            for (key, value) in entries {
                transaction
                    .put(&table, &key, &value, WriteFlags::UPSERT)
                    .unwrap();
            }
            assert!(!transaction.commit().unwrap());
        }
        Self { _root: root, path }
    }

    fn open(&self) -> (AppendLog<Vec<u8>>, Transactions) {
        let store = Store::open(&self.path).unwrap();
        let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
        (log, store.into_transactions())
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let database = raw_database(&self.path);
        let transaction = database.begin_ro_txn().unwrap();
        let table = transaction.open_table(Some(LOG_TABLE)).unwrap();
        transaction.get::<Vec<u8>>(&table, key).unwrap()
    }
}

fn assert_corrupt_bounds(fixture: &RawLog) {
    let (log, mut transactions) = fixture.open();
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

#[test]
fn append_log_has_a_stable_dedicated_layout() {
    let fixture = RawLog::empty();
    let (log, mut transactions) = fixture.open();
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
    assert_eq!(fixture.get(&[]), Some(metadata(1, 2, 9).to_vec()));
    assert_eq!(fixture.get(&0_u64.to_be_bytes()), None);
    assert_eq!(fixture.get(&1_u64.to_be_bytes()), Some(b"b".to_vec()));

    let (log, mut transactions) = fixture.open();
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
    assert_eq!(fixture.get(&[]), Some(metadata(2, 3, 9).to_vec()));
    assert_eq!(fixture.get(&1_u64.to_be_bytes()), None);
    assert_eq!(fixture.get(&2_u64.to_be_bytes()), Some(b"c".to_vec()));
}

#[test]
fn an_empty_batch_does_not_materialize_log_metadata() {
    let fixture = RawLog::empty();
    let (log, mut transactions) = fixture.open();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        log.access(transaction.access())
            .unwrap()
            .append_batch(&[])
            .unwrap(),
        0..0
    );
    transaction.commit().unwrap();
    drop(transactions);
    assert_eq!(fixture.get(&[]), None);
}

#[test]
fn missing_or_invalid_metadata_is_corruption() {
    for invalid in [
        None,
        Some(vec![0; 8]),
        Some(vec![0; 16]),
        Some(vec![0; 23]),
        Some(vec![0; 25]),
        Some(metadata(2, 1, 0).to_vec()),
        Some(metadata(2, 2, 1).to_vec()),
        Some(metadata(0, 2, 15).to_vec()),
    ] {
        let entries = invalid.map_or_else(
            || vec![entry(0, b"orphan")],
            |encoded| vec![(Vec::new(), encoded)],
        );
        assert_corrupt_bounds(&RawLog::with_entries(entries));
    }
}

#[test]
fn a_gap_is_detected_before_any_scan_callback_runs() {
    let fixture = RawLog::with_entries(vec![
        (Vec::new(), metadata(0, 2, 18).to_vec()),
        entry(0, b"a"),
    ]);
    let (log, mut transactions) = fixture.open();
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
fn corrupt_truncation_rolls_back_prior_deletes() {
    let cases = [
        (
            metadata(0, 3, 27),
            vec![entry(0, b"a"), entry(2, b"c")],
            3,
            3,
        ),
        (
            metadata(0, 3, 24),
            vec![entry(0, b"a"), entry(1, b"a"), entry(2, b"a")],
            1,
            1,
        ),
    ];
    for (metadata, mut entries, target, max_items) in cases {
        entries.push((Vec::new(), metadata.to_vec()));
        let fixture = RawLog::with_entries(entries);
        let (log, mut transactions) = fixture.open();
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert!(matches!(
            access.truncate_before(target, NonZeroUsize::new(max_items).unwrap()),
            Err(StoreError::CorruptAppendLog { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
        drop(transactions);
        assert_eq!(fixture.get(&[]), Some(metadata.to_vec()));
        assert_eq!(fixture.get(&0_u64.to_be_bytes()), Some(b"a".to_vec()));
    }
}

#[test]
fn scan_rejects_an_entry_at_the_recorded_tail() {
    let fixture = RawLog::with_entries(vec![
        (Vec::new(), metadata(0, 1, 9).to_vec()),
        entry(0, b"a"),
        entry(1, b"extra"),
    ]);
    let (log, mut transactions) = fixture.open();
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
    for (bounds, existing, exhausted) in [
        (metadata(0, 0, 0), Some(entry(0, b"existing")), false),
        (metadata(u64::MAX, u64::MAX, 0), None, true),
    ] {
        let mut entries = vec![(Vec::new(), bounds.to_vec())];
        entries.extend(existing);
        let fixture = RawLog::with_entries(entries);
        let (log, mut transactions) = fixture.open();
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        let error = access.append(&b"new".to_vec()).unwrap_err();
        assert!(if exhausted {
            matches!(error, StoreError::LogOffsetExhausted)
        } else {
            matches!(error, StoreError::CorruptAppendLog { .. })
        });
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn retained_byte_counter_exhaustion_rolls_back_the_append() {
    let fixture = RawLog::with_entries(vec![
        (Vec::new(), metadata(0, 1, u64::MAX).to_vec()),
        entry(0, b"a"),
    ]);
    let (log, mut transactions) = fixture.open();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert!(matches!(
        access.append(&b"b".to_vec()),
        Err(StoreError::LogRetainedBytesExhausted)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    drop(transactions);
    assert_eq!(fixture.get(&1_u64.to_be_bytes()), None);
    assert_eq!(fixture.get(&[]), Some(metadata(0, 1, u64::MAX).to_vec()));
}

#[test]
fn ordered_batch_append_rejects_a_physical_key_beyond_the_recorded_tail() {
    let fixture = RawLog::with_entries(vec![
        (Vec::new(), metadata(0, 0, 0).to_vec()),
        entry(1, b"future"),
    ]);
    let (log, mut transactions) = fixture.open();
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
    assert_eq!(fixture.get(&0_u64.to_be_bytes()), None);
    assert_eq!(fixture.get(&1_u64.to_be_bytes()), Some(b"future".to_vec()));
}

#[test]
fn batch_offset_overflow_is_rejected_before_any_entry_is_written() {
    let fixture = RawLog::with_entries(vec![(
        Vec::new(),
        metadata(u64::MAX - 1, u64::MAX - 1, 0).to_vec(),
    )]);
    let (log, mut transactions) = fixture.open();
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
    assert_eq!(fixture.get(&(u64::MAX - 1).to_be_bytes()), None);
}
