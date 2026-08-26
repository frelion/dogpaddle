use std::panic::{AssertUnwindSafe, catch_unwind};

use dogpaddle_store::{
    Large, ScanDirection, ScanLimit, Small, Store, StoreError, TransactionAccess,
};

use crate::support::{ByteMap, create_byte_map, open_byte_map, store_path};

fn write_pair(
    access: TransactionAccess<'_>,
    small: &ByteMap<Small>,
    large: &ByteMap<Large>,
) -> Result<(), StoreError> {
    small
        .access(access)?
        .put(&b"key".to_vec(), &b"small".to_vec())?;
    large
        .access(access)?
        .put(&b"key".to_vec(), &b"large".to_vec())
}

#[test]
fn transaction_access_can_bind_multiple_objects_without_owning_commit() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let small = create_byte_map::<Small>(&mut store, "small").unwrap();
    let large = create_byte_map::<Large>(&mut store, "large").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    write_pair(transaction.access(), &small, &large).unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        small
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"small".to_vec())
    );
    assert_eq!(
        large
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"large".to_vec())
    );
}

#[test]
fn commit_is_atomic_across_small_and_large_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let small = create_byte_map::<Small>(&mut store, "small").unwrap();
    let large = create_byte_map::<Large>(&mut store, "large").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut small = small.access(transaction.access()).unwrap();
        let mut large = large.access(transaction.access()).unwrap();
        small.put(&b"key".to_vec(), &b"small".to_vec()).unwrap();
        large.put(&b"key".to_vec(), &b"large".to_vec()).unwrap();
        assert_eq!(
            small.get(&b"key".to_vec()).unwrap(),
            Some(b"small".to_vec())
        );
        assert_eq!(
            large.get(&b"key".to_vec()).unwrap(),
            Some(b"large".to_vec())
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let small = open_byte_map::<Small>(&store, "small").unwrap();
    let large = open_byte_map::<Large>(&store, "large").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        small
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"small".to_vec())
    );
    assert_eq!(
        large
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"large".to_vec())
    );
}

#[test]
fn dropping_a_transaction_rolls_back_every_data_object() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let small = create_byte_map::<Small>(&mut store, "small").unwrap();
    let large = create_byte_map::<Large>(&mut store, "large").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        small
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"small".to_vec())
            .unwrap();
        large
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"large".to_vec())
            .unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        small
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        None
    );
    assert_eq!(
        large
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn panic_rolls_back_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let transaction = transactions.begin().unwrap();
        data.access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"value".to_vec())
            .unwrap();
        panic!("stop the attempt");
    }));
    assert!(panic.is_err());

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn wrong_store_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut first_store = Store::create(root.path().join("first")).unwrap();
    let first = create_byte_map::<Small>(&mut first_store, "data").unwrap();
    let mut first_transactions = first_store.into_transactions();

    let mut second_store = Store::create(root.path().join("second")).unwrap();
    let second = create_byte_map::<Small>(&mut second_store, "data").unwrap();

    let transaction = first_transactions.begin().unwrap();
    let access = transaction.access();
    let mut first_access = first.access(access).unwrap();
    first_access
        .put(&b"key".to_vec(), &b"value".to_vec())
        .unwrap();
    assert!(matches!(second.access(access), Err(StoreError::WrongStore)));
    assert!(matches!(
        first_access.get(&b"key".to_vec()),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = first_transactions.begin().unwrap();
    assert_eq!(
        first
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn data_objects_from_a_previous_open_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let stale = create_byte_map::<Small>(&mut store, "data").unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let current = open_byte_map::<Small>(&store, "data").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert!(matches!(
        stale.access(transaction.access()),
        Err(StoreError::WrongStore)
    ));
    assert!(matches!(
        current.access(transaction.access()),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn scan_admission_errors_are_soft() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    data.access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"wide".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    let mut visited = false;
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
            |_| {
                visited = true;
                Ok::<(), StoreError>(())
            },
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    assert!(!visited);
    access
        .put(&b"second".to_vec(), &b"still writable".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"second".to_vec())
            .unwrap(),
        Some(b"still writable".to_vec())
    );
}

#[test]
fn transaction_capability_can_move_to_a_stage_thread() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let transactions = store.into_transactions();

    let (mut transactions, data) = std::thread::spawn(move || {
        let mut transactions = transactions;
        let transaction = transactions.begin().unwrap();
        data.access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"value".to_vec())
            .unwrap();
        transaction.commit().unwrap();
        (transactions, data)
    })
    .join()
    .unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"value".to_vec())
    );
}
