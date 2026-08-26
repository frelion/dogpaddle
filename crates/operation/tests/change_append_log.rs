use std::num::NonZeroI64;

use dogpaddle_operation::data::{Change, Record, Value};
use dogpaddle_store::{AppendLog, AppendLogAccess, ScanLimit, Store, StoreError};

fn change(diff: i64, value: u64) -> Change {
    let record = Record::try_new([("value".to_owned(), Value::U64(value))]).unwrap();
    Change::new(NonZeroI64::new(diff).unwrap(), record)
}

fn scan_changes(access: &AppendLogAccess<'_, Change>) -> Vec<(u64, Change)> {
    let mut changes = Vec::new();
    let scan = access
        .scan(0, ScanLimit::new(16, 64 * 1_024).unwrap(), |entry| {
            changes.push((entry.offset(), entry.decode_owned()?));
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert!(scan.caught_up);
    changes
}

#[test]
fn positive_and_negative_changes_continue_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let changes: AppendLog<Change> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut changes = changes.access(transaction.access()).unwrap();
        assert_eq!(changes.append(&change(2, 7)).unwrap(), 0);
        assert_eq!(changes.append(&change(-1, 7)).unwrap(), 1);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let changes: AppendLog<Change> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let changes = changes.access(transaction.access()).unwrap();
    assert_eq!(
        scan_changes(&changes),
        vec![(0, change(2, 7)), (1, change(-1, 7))]
    );
}

#[test]
fn diff_projection_and_raw_forwarding_share_one_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let input: AppendLog<Change> = store.create_data("input").unwrap();
    let output: AppendLog<Change> = store.create_data("output").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut input = input.access(transaction.access()).unwrap();
        input.append(&change(-1, 10)).unwrap();
        input.append(&change(3, 20)).unwrap();
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin().unwrap();
        let input = input.access(transaction.access()).unwrap();
        let mut output = output.access(transaction.access()).unwrap();
        let mut observed_diffs = Vec::new();
        let scan = input
            .scan(0, ScanLimit::new(16, 64 * 1_024).unwrap(), |entry| {
                let diff = entry.project(Change::project_diff)?;
                observed_diffs.push(diff.get());
                if diff.get() > 0 {
                    output.append_entry(&entry)?;
                }
                Ok::<(), StoreError>(())
            })
            .unwrap();
        assert!(scan.caught_up);
        assert_eq!(observed_diffs, [-1, 3]);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    assert_eq!(scan_changes(&output), vec![(0, change(3, 20))]);
}

#[test]
fn dropping_a_raw_forwarding_transaction_rolls_back_the_output() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let input: AppendLog<Change> = store.create_data("input").unwrap();
    let output: AppendLog<Change> = store.create_data("output").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append(&change(1, 42))
            .unwrap();
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin().unwrap();
        let input = input.access(transaction.access()).unwrap();
        let mut output = output.access(transaction.access()).unwrap();
        input
            .scan(0, ScanLimit::new(1, 64 * 1_024).unwrap(), |entry| {
                output.append_entry(&entry)?;
                Ok::<(), StoreError>(())
            })
            .unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    assert_eq!(output.bounds().unwrap(), 0..0);
}

#[test]
fn malformed_change_bytes_poison_full_decode_and_projection_transactions() {
    for project_only in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let mut store = Store::create(&path).unwrap();
        let raw: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin().unwrap();
            raw.access(transaction.access())
                .unwrap()
                .append(&b"not a change".to_vec())
                .unwrap();
            transaction.commit().unwrap();
        }
        drop(transactions);

        // Store intentionally validates physical placement rather than a
        // collection's value codec, so corrupt bytes can be injected this way.
        let store = Store::open(&path).unwrap();
        let changes: AppendLog<Change> = store.open_data("changes").unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin().unwrap();
        let changes = changes.access(transaction.access()).unwrap();
        let error = changes
            .scan(
                0,
                ScanLimit::new(1, 1_024).unwrap(),
                |entry| -> Result<(), StoreError> {
                    if project_only {
                        entry.project(Change::project_diff)?;
                    } else {
                        entry.decode_owned()?;
                    }
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(matches!(error, StoreError::Codec(_)));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn projection_skips_a_record_that_full_decoding_rejects() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let raw: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        raw.access(transaction.access())
            .unwrap()
            .append(&vec![
                0x00, 0x01, // version
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // diff
                0x00, 0x00, 0x00, 0x01, // one field, but no field bytes
            ])
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let changes: AppendLog<Change> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let changes = changes.access(transaction.access()).unwrap();
        changes
            .scan(
                0,
                ScanLimit::new(1, 1_024).unwrap(),
                |entry| -> Result<(), StoreError> {
                    assert_eq!(entry.project(Change::project_diff)?.get(), 2);
                    Ok(())
                },
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let changes = changes.access(transaction.access()).unwrap();
    let error = changes
        .scan(
            0,
            ScanLimit::new(1, 1_024).unwrap(),
            |entry| -> Result<(), StoreError> {
                entry.decode_owned()?;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::Codec(_)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
