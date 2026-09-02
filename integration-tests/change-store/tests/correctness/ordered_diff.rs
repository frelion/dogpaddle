use dogpaddle_change_store_integration::{flatten_ordered, ordered_diff_fixture};
use dogpaddle_store::{AppendLog, Store};

use super::support::scan_changes;

#[test]
fn ordered_differences_survive_duplicates_negative_diffs_and_stable_rebatching() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = ordered_diff_fixture();
    assert!(
        expected
            .events
            .windows(2)
            .any(|pair| pair[0].value == pair[1].value)
    );
    assert!(expected.events.iter().any(|event| event.diff < 0));
    assert!(expected.events.iter().any(|event| event.diff > 1));

    let mut store = Store::create(&path).unwrap();
    let coarse: AppendLog<Vec<u8>> = store.create_data("coarse").unwrap();
    let fine: AppendLog<Vec<u8>> = store.create_data("fine").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        coarse
            .access(transaction.access())
            .unwrap()
            .append_batch(&expected.coarse.encoded)
            .unwrap();
        fine.access(transaction.access())
            .unwrap()
            .append_batch(&expected.fine.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let coarse: AppendLog<Vec<u8>> = store.open_data("coarse").unwrap();
    let fine: AppendLog<Vec<u8>> = store.open_data("fine").unwrap();
    let mut transactions = store.into_transactions();
    let coarse_changes = {
        let transaction = transactions.begin().unwrap();
        let access = coarse.access(transaction.access()).unwrap();
        scan_changes(
            &access,
            0,
            expected.coarse.encoded.len(),
            expected.coarse.scan_bytes(),
        )
        .into_iter()
        .map(|(_, change)| change)
        .collect::<Vec<_>>()
    };
    let fine_changes = {
        let transaction = transactions.begin().unwrap();
        let access = fine.access(transaction.access()).unwrap();
        scan_changes(
            &access,
            0,
            expected.fine.encoded.len(),
            expected.fine.scan_bytes(),
        )
        .into_iter()
        .map(|(_, change)| change)
        .collect::<Vec<_>>()
    };

    assert_eq!(flatten_ordered(&coarse_changes), expected.events);
    assert_eq!(flatten_ordered(&fine_changes), expected.events);
    assert_ne!(coarse_changes[0].num_rows(), fine_changes[0].num_rows());
}
