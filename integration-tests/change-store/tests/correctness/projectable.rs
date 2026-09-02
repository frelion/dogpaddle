use std::num::NonZeroUsize;

use arrow_array::UInt64Array;
use arrow_schema::DataType;
use dogpaddle_change_store_integration::{assert_change_eq, projectable_fixture};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

use super::support::{decode_entry, decode_projected_entry, scan_raw};

#[test]
#[allow(clippy::too_many_lines)]
fn nested_projection_is_owned_across_transactions_truncate_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = (0..3)
        .map(|index| projectable_fixture(100 + index * 10, 4, 17))
        .collect::<Vec<_>>();
    let first_ids = expected[0]
        .change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(first_ids.values(), &[101, 102, 103, 104]);
    assert!(
        expected[0]
            .projected
            .schema()
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), DataType::Binary))
    );
    assert!(
        expected[0]
            .projected
            .schema()
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), DataType::List(_)))
    );

    let mut store = Store::create(&path).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("projectable").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(
                &expected
                    .iter()
                    .map(|item| item.encoded.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("projectable").unwrap();
    let mut transactions = store.into_transactions();
    let scan_bytes = expected
        .iter()
        .map(|item| item.encoded.len() + size_of::<u64>())
        .sum();
    let full = {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let mut changes = Vec::new();
        let progress = access
            .scan(
                0,
                ScanLimit::new(expected.len(), scan_bytes).unwrap(),
                |entry| {
                    changes.push(entry.project(decode_entry)?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up);
        changes
    };
    let projected = {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let mut changes = Vec::new();
        let progress = access
            .scan(
                0,
                ScanLimit::new(expected.len(), scan_bytes).unwrap(),
                |entry| {
                    let index = usize::try_from(entry.offset()).unwrap();
                    changes.push(entry.project(|bytes| {
                        decode_projected_entry(bytes, &expected[index].projection)
                    })?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up);
        changes
    };
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            2
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("projectable").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), 2..3);
    assert_eq!(
        scan_raw(&access, 2, 1, expected[2].encoded.len() + size_of::<u64>()),
        [(2, expected[2].encoded.clone())]
    );
    drop(transaction);
    drop(transactions);

    for ((actual_full, actual_projected), expected) in full.iter().zip(&projected).zip(&expected) {
        assert_change_eq(actual_full, &expected.change);
        assert_change_eq(actual_projected, &expected.projected);
    }
}
