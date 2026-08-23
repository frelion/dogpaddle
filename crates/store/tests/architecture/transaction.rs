use std::panic::{AssertUnwindSafe, catch_unwind};

use dogpaddle_store::{DataPlacement, ScanDirection, ScanLimit, Store, StoreError};

use crate::support::store_path;

#[test]
fn commit_is_atomic_across_shared_and_dedicated_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let shared = store.create_data("shared", DataPlacement::Shared).unwrap();
    let dedicated = store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    {
        let mut shared = shared.access(&transaction).unwrap();
        let mut dedicated = dedicated.access(&transaction).unwrap();
        shared.put(b"key", b"shared").unwrap();
        dedicated.put(b"key", b"dedicated").unwrap();
        assert_eq!(shared.get(b"key").unwrap(), Some(b"shared".to_vec()));
        assert_eq!(dedicated.get(b"key").unwrap(), Some(b"dedicated".to_vec()));
    }
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let shared = store.open_data("shared").unwrap();
    let dedicated = store.open_data("dedicated").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        shared.access(&transaction).unwrap().get(b"key").unwrap(),
        Some(b"shared".to_vec())
    );
    assert_eq!(
        dedicated.access(&transaction).unwrap().get(b"key").unwrap(),
        Some(b"dedicated".to_vec())
    );
}

#[test]
fn dropping_a_transaction_rolls_back_every_namespace() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let shared = store.create_data("shared", DataPlacement::Shared).unwrap();
    let dedicated = store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        shared
            .access(&transaction)
            .unwrap()
            .put(b"key", b"shared")
            .unwrap();
        dedicated
            .access(&transaction)
            .unwrap()
            .put(b"key", b"dedicated")
            .unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        shared.access(&transaction).unwrap().get(b"key").unwrap(),
        None
    );
    assert_eq!(
        dedicated.access(&transaction).unwrap().get(b"key").unwrap(),
        None
    );
}

#[test]
fn panic_rolls_back_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = store.create_data("data", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let transaction = transactions.begin().unwrap();
        data.access(&transaction)
            .unwrap()
            .put(b"key", b"value")
            .unwrap();
        panic!("stop the attempt");
    }));
    assert!(panic.is_err());

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(&transaction).unwrap().get(b"key").unwrap(),
        None
    );
}

#[test]
fn wrong_store_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut first_store = Store::create(root.path().join("first")).unwrap();
    let first = first_store
        .create_data("data", DataPlacement::Shared)
        .unwrap();
    let mut first_transactions = first_store.into_transactions();

    let mut second_store = Store::create(root.path().join("second")).unwrap();
    let second = second_store
        .create_data("data", DataPlacement::Shared)
        .unwrap();

    let transaction = first_transactions.begin().unwrap();
    let mut first_access = first.access(&transaction).unwrap();
    first_access.put(b"key", b"value").unwrap();
    assert!(matches!(
        second.access(&transaction),
        Err(StoreError::WrongStore)
    ));
    assert!(matches!(
        first_access.get(b"key"),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = first_transactions.begin().unwrap();
    assert_eq!(
        first.access(&transaction).unwrap().get(b"key").unwrap(),
        None
    );
}

#[test]
fn handles_from_a_previous_open_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let stale = store.create_data("data", DataPlacement::Shared).unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let current = store.open_data("data").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert!(matches!(
        stale.access(&transaction),
        Err(StoreError::WrongStore)
    ));
    assert!(matches!(
        current.access(&transaction),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BrokenInvariant;

#[test]
fn client_errors_can_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = store.create_data("data", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(&transaction).unwrap();
    access.put(b"key", b"value").unwrap();
    assert_eq!(
        access.poison_on_error(Err::<(), _>(BrokenInvariant)),
        Err(BrokenInvariant)
    );
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(&transaction).unwrap().get(b"key").unwrap(),
        None
    );
}

#[test]
fn scan_admission_errors_are_soft() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = store.create_data("data", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    data.access(&transaction)
        .unwrap()
        .put(b"key", b"wide")
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(&transaction).unwrap();
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    access.put(b"second", b"still writable").unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(&transaction).unwrap().get(b"second").unwrap(),
        Some(b"still writable".to_vec())
    );
}

#[test]
fn transaction_capability_can_move_to_a_stage_thread() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = store.create_data("data", DataPlacement::Shared).unwrap();
    let transactions = store.into_transactions();

    let (mut transactions, data) = std::thread::spawn(move || {
        let mut transactions = transactions;
        let transaction = transactions.begin().unwrap();
        data.access(&transaction)
            .unwrap()
            .put(b"key", b"value")
            .unwrap();
        transaction.commit().unwrap();
        (transactions, data)
    })
    .join()
    .unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(&transaction).unwrap().get(b"key").unwrap(),
        Some(b"value".to_vec())
    );
}
