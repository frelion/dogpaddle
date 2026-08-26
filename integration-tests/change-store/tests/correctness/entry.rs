use std::num::NonZeroUsize;

use arrow_array::UInt64Array;
use dogpaddle_change::{ChangeProjection, decode_change, encode_change};
use dogpaddle_change_store_integration::{
    StoreFixture, assert_change_eq, narrow_change, narrow_schema, wide_change,
};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

use super::support::{decode_projected_entry, scan_raw};

#[test]
fn entries_preserve_exact_stream_boundaries_and_self_describe_heterogeneous_schemas() {
    let fixture = StoreFixture::new();
    let narrow = narrow_change(&[7, 8, 7], &[1, 1, -1]);
    let wide = wide_change(40, 3, 31);
    let encoded = vec![
        encode_change(&narrow).unwrap(),
        encode_change(&wide).unwrap(),
    ];

    let mut store = Store::create(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.append_batch(&encoded).unwrap(), 0..2);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let exact_scan_bytes = encoded.iter().map(Vec::len).sum::<usize>() + 2 * size_of::<u64>();
    let actual = scan_raw(&access, 0, 2, exact_scan_bytes);

    assert_eq!(
        actual.iter().map(|(offset, _)| *offset).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        actual.iter().map(|(_, bytes)| bytes).collect::<Vec<_>>(),
        encoded.iter().collect::<Vec<_>>()
    );
    assert_change_eq(&decode_change(&actual[0].1).unwrap(), &narrow);
    assert_change_eq(&decode_change(&actual[1].1).unwrap(), &wide);
}

#[test]
fn append_entry_forwards_the_complete_stream_without_reencoding() {
    let fixture = StoreFixture::new();
    let change = wide_change(100, 4, 47);
    let encoded = encode_change(&change).unwrap();

    let mut store = Store::create(fixture.path()).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let input_access = input.access(transaction.access()).unwrap();
        let mut output_access = output.access(transaction.access()).unwrap();
        let progress = input_access
            .scan(
                0,
                ScanLimit::new(1, encoded.len() + size_of::<u64>()).unwrap(),
                |entry| {
                    assert_eq!(output_access.append_entry(&entry)?, 0);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = output.access(transaction.access()).unwrap();
    assert_eq!(scan_raw(&access, 0, 1, encoded.len() + 8), [(0, encoded)]);
}

#[test]
fn projected_changes_are_owned_across_transactions_reopen_and_truncate() {
    let fixture = StoreFixture::new();
    let changes = (0..3)
        .map(|index| wide_change(index * 10, 2, 23))
        .collect::<Vec<_>>();
    let encoded = changes
        .iter()
        .map(|change| encode_change(change).unwrap())
        .collect::<Vec<_>>();
    let selected = ChangeProjection::try_new(changes[0].schema(), [0, 2]).unwrap();
    let diffs_only = ChangeProjection::try_new(changes[0].schema(), []).unwrap();

    let mut store = Store::create(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(&encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let projected = {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let mut selected_changes = Vec::new();
        let limit = encoded.iter().map(Vec::len).sum::<usize>() + 3 * 8;
        let progress = access
            .scan(0, ScanLimit::new(3, limit).unwrap(), |entry| {
                selected_changes
                    .push(entry.project(|bytes| decode_projected_entry(bytes, &selected))?);
                Ok::<(), StoreError>(())
            })
            .unwrap();
        assert!(progress.caught_up);
        selected_changes
    };
    let diffs = {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let mut change = None;
        access
            .scan(
                0,
                ScanLimit::new(1, encoded[0].len() + 8).unwrap(),
                |entry| {
                    change =
                        Some(entry.project(|bytes| decode_projected_entry(bytes, &diffs_only))?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        change.unwrap()
    };

    assert_eq!(projected.len(), 3);
    assert_eq!(projected[0].schema(), selected.output_schema());
    assert_eq!(projected[0].diffs(), changes[0].diffs());
    let ids = projected[2]
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[20, 21]);
    assert_eq!(diffs.records().num_columns(), 0);
    assert_eq!(diffs.num_rows(), 2);
    assert_eq!(diffs.diffs(), changes[0].diffs());

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

    let store = Store::open(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), 2..3);
    let retained = scan_raw(&access, 2, 1, encoded[2].len() + 8);
    assert_eq!(retained, [(2, encoded[2].clone())]);

    // Compare every Arrow array after the source transactions, original Store,
    // and the first two persisted entries are gone. RecordBatch equality reads
    // both selected buffers, including the tail column that is not adjacent to
    // the selected ID column in the source Stream.
    for (actual, original) in projected.iter().zip(&changes) {
        assert_change_eq(actual, &original.try_project(&selected).unwrap());
    }
}

#[test]
fn projection_schema_mismatch_crosses_the_store_boundary_and_poisons_the_transaction() {
    let fixture = StoreFixture::new();
    let wide = encode_change(&wide_change(0, 2, 17)).unwrap();
    let narrow_projection = ChangeProjection::try_new(narrow_schema(), [0]).unwrap();

    let mut store = Store::create(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&wide)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let error = access
        .scan(
            0,
            ScanLimit::new(1, wide.len() + size_of::<u64>()).unwrap(),
            |entry| {
                entry.project(|bytes| decode_projected_entry(bytes, &narrow_projection))?;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::Codec(_)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
