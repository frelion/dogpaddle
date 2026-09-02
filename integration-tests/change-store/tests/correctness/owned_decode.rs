use dogpaddle_change_store_integration::{assert_change_eq, projectable_fixture};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

use super::support::{decode_entry, decode_projected_entry};

#[test]
fn full_and_projected_changes_are_owned_beyond_the_entry_transaction() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = projectable_fixture(100, 4, 17);

    let mut store = Store::create(&path).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&expected.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let (full, projected) = {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let mut decoded = None;
        let progress = access
            .scan(
                0,
                ScanLimit::new(1, expected.encoded.len() + size_of::<u64>()).unwrap(),
                |entry| {
                    assert_eq!(entry.offset(), 0);
                    entry.project(|bytes| {
                        assert_eq!(bytes, expected.encoded);
                        Ok(())
                    })?;
                    let full = entry.project(decode_entry)?;
                    let projected = entry
                        .project(|bytes| decode_projected_entry(bytes, &expected.projection))?;
                    decoded = Some((full, projected));
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up);
        decoded.unwrap()
    };
    drop(transactions);

    assert_change_eq(&full, &expected.change);
    assert_change_eq(&projected, &expected.projected);
}
