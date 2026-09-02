use std::{collections::BTreeSet, num::NonZeroUsize, path::Path};

use dogpaddle_change::decode_change;
use dogpaddle_change_store_integration::{
    EncodedChanges, assert_change_eq, heterogeneous_pages_fixture, order_checksum,
};
use dogpaddle_store::{AppendLog, Cell, ScanLimit, Store, StoreError};

use super::support::{decode_entry, scan_raw};

#[test]
fn heterogeneous_entries_cover_rollback_item_and_byte_pages_copy_truncate_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = heterogeneous_pages_fixture(9, 3, 19);
    assert_ne!(expected.changes[0].schema(), expected.changes[1].schema());
    assert!(
        expected
            .encoded
            .iter()
            .map(Vec::len)
            .collect::<BTreeSet<_>>()
            .len()
            >= 3
    );

    create_after_rollback(&path, &expected);
    let item_paged = read_pages(&path, &expected, 2, usize::MAX);
    assert!(item_paged.pages > 1);
    assert_eq!(item_paged.entries, expected.encoded);
    let largest_charge = expected
        .encoded
        .iter()
        .map(|entry| entry.len() + size_of::<u64>())
        .max()
        .unwrap();
    let byte_paged = read_pages(&path, &expected, expected.encoded.len(), largest_charge);
    assert!(byte_paged.pages > 1);
    assert_eq!(byte_paged.entries, expected.encoded);

    copy_through_entry_boundary(&path, &expected, largest_charge);
    truncate_and_verify_reopen(&path, &expected, 5);
}

#[test]
fn corrupt_change_poisons_transaction_and_rolls_back_related_store_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let valid = heterogeneous_pages_fixture(2, 2, 7).encoded[0].clone();
    let corrupt = valid[..valid.len() - 1].to_vec();

    let mut store = Store::create(&path).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let cursor: Cell<u64> = store.create_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append_batch(&[valid.clone(), corrupt.clone()])
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let input_access = input.access(transaction.access()).unwrap();
    let mut output_access = output.access(transaction.access()).unwrap();
    let mut cursor_access = cursor.access(transaction.access()).unwrap();
    let mut successful_callbacks = 0_u64;
    let error = input_access
        .scan(
            0,
            ScanLimit::new(2, valid.len() + corrupt.len() + 2 * size_of::<u64>()).unwrap(),
            |entry| {
                entry.project(decode_entry)?;
                output_access.append_entry(&entry)?;
                successful_callbacks += 1;
                cursor_access.set(&successful_callbacks)?;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap_err();
    assert_eq!(successful_callbacks, 1);
    assert!(matches!(error, StoreError::Codec(_)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let input: AppendLog<Vec<u8>> = store.open_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("output").unwrap();
    let cursor: Cell<u64> = store.open_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        input
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..2
    );
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..0
    );
    assert_eq!(
        cursor.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
}

struct PageRead {
    pages: usize,
    entries: Vec<Vec<u8>>,
}

fn create_after_rollback(path: &Path, expected: &EncodedChanges) {
    let mut store = Store::create(path).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let _: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let _: Cell<u64> = store.create_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append_batch(&expected.encoded)
            .unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            input
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
            0..0
        );
    }
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append_batch(&expected.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
}

fn read_pages(
    path: &Path,
    expected: &EncodedChanges,
    page_items: usize,
    page_bytes: usize,
) -> PageRead {
    let store = Store::open(path).unwrap();
    let input: AppendLog<Vec<u8>> = store.open_data("input").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = input.access(transaction.access()).unwrap();
    assert_eq!(
        access.retained_bytes().unwrap(),
        u64::try_from(expected.scan_bytes()).unwrap()
    );
    let limit = ScanLimit::new(page_items, page_bytes).unwrap();
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut entries = Vec::new();
    loop {
        pages += 1;
        let before = offset;
        let progress = access
            .scan(offset, limit, |entry| {
                let index = usize::try_from(entry.offset()).unwrap();
                let raw = entry.project(|bytes| Ok(bytes.to_vec()))?;
                assert_eq!(raw, expected.encoded[index]);
                assert_change_eq(&entry.project(decode_entry)?, &expected.changes[index]);
                entries.push(raw);
                Ok::<(), StoreError>(())
            })
            .unwrap();
        offset = progress.next_offset;
        assert!(progress.caught_up || offset > before);
        if progress.caught_up {
            break;
        }
    }
    assert_eq!(offset, u64::try_from(expected.encoded.len()).unwrap());
    assert_eq!(order_checksum(&entries), expected.order_checksum());
    PageRead { pages, entries }
}

fn copy_through_entry_boundary(path: &Path, expected: &EncodedChanges, page_bytes: usize) {
    let store = Store::open(path).unwrap();
    let input: AppendLog<Vec<u8>> = store.open_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("output").unwrap();
    let cursor: Cell<u64> = store.open_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    let mut offset = 0_u64;
    while usize::try_from(offset).unwrap() < expected.encoded.len() {
        let transaction = transactions.begin().unwrap();
        let input_access = input.access(transaction.access()).unwrap();
        let mut output_access = output.access(transaction.access()).unwrap();
        let progress = input_access
            .scan(offset, ScanLimit::new(2, page_bytes).unwrap(), |entry| {
                entry.project(decode_entry)?;
                output_access.append_entry(&entry)?;
                Ok::<(), StoreError>(())
            })
            .unwrap();
        offset = progress.next_offset;
        cursor
            .access(transaction.access())
            .unwrap()
            .set(&offset)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("output").unwrap();
    let cursor: Cell<u64> = store.open_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = output.access(transaction.access()).unwrap();
    let copied = scan_raw(&access, 0, expected.encoded.len(), expected.scan_bytes());
    assert_eq!(
        copied.into_iter().map(|(_, raw)| raw).collect::<Vec<_>>(),
        expected.encoded
    );
    assert_eq!(
        cursor.access(transaction.access()).unwrap().get().unwrap(),
        Some(u64::try_from(expected.encoded.len()).unwrap())
    );
}

fn truncate_and_verify_reopen(path: &Path, expected: &EncodedChanges, retained_start: usize) {
    let target = u64::try_from(retained_start).unwrap();
    let store = Store::open(path).unwrap();
    let input: AppendLog<Vec<u8>> = store.open_data("input").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = input.access(transaction.access()).unwrap();
        let mut head = 0_u64;
        let mut calls = 0_usize;
        while head < target {
            head = access
                .truncate_before(target, NonZeroUsize::new(2).unwrap())
                .unwrap();
            calls += 1;
        }
        assert_eq!(head, target);
        assert!(calls > 1);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let input: AppendLog<Vec<u8>> = store.open_data("input").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = input.access(transaction.access()).unwrap();
    assert_eq!(
        access.bounds().unwrap(),
        target..u64::try_from(expected.encoded.len()).unwrap()
    );
    let retained_bytes = expected.encoded[retained_start..]
        .iter()
        .map(|entry| entry.len() + size_of::<u64>())
        .sum();
    assert_eq!(
        access.retained_bytes().unwrap(),
        u64::try_from(retained_bytes).unwrap()
    );
    let retained = scan_raw(
        &access,
        target,
        expected.encoded.len() - retained_start,
        retained_bytes,
    )
    .into_iter()
    .map(|(_, entry)| entry)
    .collect::<Vec<_>>();
    assert_eq!(retained, expected.encoded[retained_start..]);
    assert_eq!(
        order_checksum(&retained),
        order_checksum(&expected.encoded[retained_start..])
    );
    for (actual, expected) in retained
        .iter()
        .map(|entry| decode_change(entry).unwrap())
        .zip(&expected.changes[retained_start..])
    {
        assert_change_eq(&actual, expected);
    }
}
