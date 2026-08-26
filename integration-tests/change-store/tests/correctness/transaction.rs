use dogpaddle_change::encode_change;
use dogpaddle_change_store_integration::{StoreFixture, narrow_change};
use dogpaddle_store::{AppendLog, Cell, ScanLimit, Store, StoreError};

use super::support::decode_entry;

#[test]
fn corrupt_change_poisons_transaction_and_rolls_back_related_store_writes() {
    let fixture = StoreFixture::new();
    let valid = encode_change(&narrow_change(&[10, 11], &[1, -1])).unwrap();
    let corrupt = valid[..valid.len() - 1].to_vec();

    let mut store = Store::create(fixture.path()).unwrap();
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

    let store = Store::open(fixture.path()).unwrap();
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
