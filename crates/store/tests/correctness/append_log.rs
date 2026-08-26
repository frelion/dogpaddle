#[path = "append_log_errors.rs"]
mod errors;
#[path = "append_log_projection.rs"]
mod projection;

use std::num::NonZeroUsize;

use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError, StoreValue};

use crate::support::store_path;

fn create_log<T: StoreValue>(store: &mut Store, name: &str) -> AppendLog<T> {
    store.create_data(name).unwrap()
}

fn scan_values<T>(
    access: &dogpaddle_store::AppendLogAccess<'_, T>,
    offset: u64,
    limit: ScanLimit,
) -> (Vec<(u64, T)>, dogpaddle_store::AppendLogScan)
where
    T: StoreValue,
{
    let mut values = Vec::new();
    let scan = access
        .scan(offset, limit, |entry| -> Result<(), StoreError> {
            values.push((entry.offset(), entry.decode_owned()?));
            Ok(())
        })
        .unwrap();
    (values, scan)
}

#[test]
fn fresh_log_is_empty_and_append_offsets_are_stable() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<String>(&mut store, "log");
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.bounds().unwrap(), 0..0);
        let (_, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
        assert_eq!(scan.next_offset, 0);
        assert!(scan.caught_up);

        assert_eq!(access.append(&"a".to_owned()).unwrap(), 0);
        assert_eq!(access.append(&"bb".to_owned()).unwrap(), 1);
        assert_eq!(access.bounds().unwrap(), 0..2);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, "a".to_owned()), (1, "bb".to_owned())]);
    assert_eq!(scan.next_offset, 2);
    assert!(scan.caught_up);
}

#[test]
fn multiple_accesses_see_same_transaction_appends() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut first = log.access(transaction.access()).unwrap();
    let mut second = log.access(transaction.access()).unwrap();
    assert_eq!(first.append(&10).unwrap(), 0);
    assert_eq!(second.append(&20).unwrap(), 1);
    let (values, scan) = scan_values(&first, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 10), (1, 20)]);
    assert!(scan.caught_up);
    transaction.commit().unwrap();
}

#[test]
fn batch_append_is_ordered_and_visible_across_same_transaction_accesses() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut first = log.access(transaction.access()).unwrap();
        let mut second = log.access(transaction.access()).unwrap();
        assert_eq!(first.append_batch(&[]).unwrap(), 0..0);
        assert_eq!(first.append_batch(&[10, 20, 30]).unwrap(), 0..3);
        assert_eq!(second.bounds().unwrap(), 0..3);
        assert_eq!(second.append_batch(&[40, 50]).unwrap(), 3..5);
        assert_eq!(first.bounds().unwrap(), 0..5);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 10), (1, 20), (2, 30), (3, 40), (4, 50)]);
    assert!(scan.caught_up);
}

#[test]
fn item_and_byte_limits_produce_exact_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access.append(&b"aaa".to_vec()).unwrap();
        access.append(&b"bbbb".to_vec()).unwrap();
        access.append(&b"ccccc".to_vec()).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (first, first_scan) = scan_values(&access, 0, ScanLimit::new(2, 1_024).unwrap());
    assert_eq!(first, vec![(0, b"aaa".to_vec()), (1, b"bbbb".to_vec())]);
    assert_eq!(first_scan.next_offset, 2);
    assert!(!first_scan.caught_up);

    // Each item is charged for its eight-byte offset plus encoded value.
    let (second, second_scan) = scan_values(
        &access,
        first_scan.next_offset,
        ScanLimit::new(10, 13).unwrap(),
    );
    assert_eq!(second, vec![(2, b"ccccc".to_vec())]);
    assert!(second_scan.caught_up);
}

#[test]
fn oversized_first_entry_is_retryable_in_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&b"abc".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let error = access
        .scan::<StoreError>(0, ScanLimit::new(1, 10).unwrap(), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::ItemTooLarge {
            size: 11,
            limit: 10
        }
    ));
    let (_, scan) = scan_values(&access, 0, ScanLimit::new(1, 11).unwrap());
    assert!(scan.caught_up);
    transaction.commit().unwrap();
}

#[test]
fn truncation_is_bounded_and_never_reuses_offsets() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.append(&10).unwrap(), 0);
        assert_eq!(access.append(&20).unwrap(), 1);
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            1
        );
        assert_eq!(access.bounds().unwrap(), 1..2);
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            2
        );
        assert_eq!(access.bounds().unwrap(), 2..2);
        assert_eq!(access.append(&30).unwrap(), 2);
        assert_eq!(access.bounds().unwrap(), 2..3);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, _) = scan_values(&access, 2, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(2, 30)]);
}

#[test]
fn cursor_truncation_deletes_each_exact_prefix_entry_without_skipping() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert_eq!(access.append_batch(&[0, 1, 2, 3, 4]).unwrap(), 0..5);
    assert_eq!(
        access
            .truncate_before(4, NonZeroUsize::new(3).unwrap())
            .unwrap(),
        3
    );
    assert_eq!(access.bounds().unwrap(), 3..5);
    let (values, _) = scan_values(&access, 3, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(3, 3), (4, 4)]);
    assert_eq!(
        access
            .truncate_before(4, NonZeroUsize::new(2).unwrap())
            .unwrap(),
        4
    );
    let (values, _) = scan_values(&access, 4, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(4, 4)]);
    transaction.commit().unwrap();
}

#[test]
fn dropped_append_and_truncation_transactions_roll_back() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&1)
            .unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.bounds().unwrap(), 0..0);
        access.append(&1).unwrap();
        access.append(&2).unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .truncate_before(2, NonZeroUsize::new(2).unwrap())
            .unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<u64>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), 0..2);
    let (values, _) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 1), (1, 2)]);
}
