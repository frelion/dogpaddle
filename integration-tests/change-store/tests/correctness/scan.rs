use dogpaddle_change::encode_change;
use dogpaddle_change_store_integration::{StoreFixture, narrow_change};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

use super::support::decode_entry;

#[test]
fn scan_limit_charges_exactly_the_complete_stream_plus_eight_byte_offset() {
    let fixture = StoreFixture::new();
    let encoded = encode_change(&narrow_change(&[1, 2], &[1, -1])).unwrap();
    let charged_bytes = encoded.len() + size_of::<u64>();

    let mut store = Store::create(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let error = access
        .scan(0, ScanLimit::new(1, charged_bytes - 1).unwrap(), |_| {
            Ok::<(), StoreError>(())
        })
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::ItemTooLarge { size, limit }
            if size == charged_bytes && limit == charged_bytes - 1
    ));

    let mut rows = 0;
    let progress = access
        .scan(0, ScanLimit::new(1, charged_bytes).unwrap(), |entry| {
            rows += entry.project(decode_entry)?.num_rows();
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(rows, 2);
    assert!(progress.caught_up);
    transaction.commit().unwrap();
}
