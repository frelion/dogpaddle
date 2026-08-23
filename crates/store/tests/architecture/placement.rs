use dogpaddle_store::{DataPlacement, ScanDirection, ScanLimit, Store, StoreError};

use crate::support::{raw_database, store_path};

#[test]
fn shared_and_dedicated_have_identical_data_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let shared = store.create_data("shared", DataPlacement::Shared).unwrap();
    let dedicated = store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    {
        let mut shared = shared.access(&transaction).unwrap();
        let mut dedicated = dedicated.access(&transaction).unwrap();
        for number in 0_u64..10 {
            let key = number.to_be_bytes();
            let value = format!("value-{number}");
            shared.put(&key, value.as_bytes()).unwrap();
            dedicated.put(&key, value.as_bytes()).unwrap();
        }
    }
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let shared = shared.access(&transaction).unwrap();
    let dedicated = dedicated.access(&transaction).unwrap();
    let limit = ScanLimit::new(100, 4_096).unwrap();
    assert_eq!(
        shared
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap(),
        dedicated
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap()
    );
}

#[test]
fn catalog_recovers_placement_without_redeclaring_it() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data("shared", DataPlacement::Shared).unwrap();
    store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let shared = store.open_data("shared").unwrap();
    let dedicated = store.open_data("dedicated").unwrap();
    let mut transactions = store.into_transactions();
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
    transaction.commit().unwrap();

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
fn physical_placement_uses_shared_prefixes_and_raw_dedicated_keys() {
    // White-box adapter test. These bytes are private implementation details,
    // so an intentional disk-layout refactor must update this test with them.
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let shared_zero = store
        .create_data("shared-zero", DataPlacement::Shared)
        .unwrap();
    let shared_one = store
        .create_data("shared-one", DataPlacement::Shared)
        .unwrap();
    let dedicated_zero = store
        .create_data("dedicated-zero", DataPlacement::Dedicated)
        .unwrap();
    let dedicated_one = store
        .create_data("dedicated-one", DataPlacement::Dedicated)
        .unwrap();
    let dedicated_cell = store
        .create_data("dedicated-cell", DataPlacement::Dedicated)
        .unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    shared_zero
        .access(&transaction)
        .unwrap()
        .put(b"key", b"shared-zero")
        .unwrap();
    shared_one
        .access(&transaction)
        .unwrap()
        .put(b"key", b"shared-one")
        .unwrap();
    dedicated_zero
        .access(&transaction)
        .unwrap()
        .put(b"key", b"dedicated-zero")
        .unwrap();
    dedicated_one
        .access(&transaction)
        .unwrap()
        .put(b"key", b"dedicated-one")
        .unwrap();
    dedicated_cell
        .access(&transaction)
        .unwrap()
        .put(b"", b"cell")
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let main = transaction.open_table(None).unwrap();
    let dedicated_zero = transaction.open_table(Some("d/00000000")).unwrap();
    let dedicated_one = transaction.open_table(Some("d/00000001")).unwrap();
    let dedicated_cell = transaction.open_table(Some("d/00000002")).unwrap();

    let mut shared_zero_key = vec![3, 0, 0, 0, 0];
    shared_zero_key.extend_from_slice(b"key");
    let mut shared_one_key = vec![3, 0, 0, 0, 1];
    shared_one_key.extend_from_slice(b"key");
    assert_eq!(
        transaction.get::<Vec<u8>>(&main, &shared_zero_key).unwrap(),
        Some(b"shared-zero".to_vec())
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&main, &shared_one_key).unwrap(),
        Some(b"shared-one".to_vec())
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&dedicated_zero, b"key").unwrap(),
        Some(b"dedicated-zero".to_vec())
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&dedicated_one, b"key").unwrap(),
        Some(b"dedicated-one".to_vec())
    );
    assert_eq!(
        transaction
            .get::<Vec<u8>>(&dedicated_zero, &shared_zero_key)
            .unwrap(),
        None
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&dedicated_cell, b"").unwrap(),
        Some(b"cell".to_vec())
    );
}

#[test]
fn dedicated_capacity_does_not_limit_shared_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    for index in 0..Store::DEDICATED_CAPACITY {
        store
            .create_data(&format!("dedicated-{index}"), DataPlacement::Dedicated)
            .unwrap();
    }
    assert!(matches!(
        store.create_data("one-too-many", DataPlacement::Dedicated),
        Err(StoreError::DedicatedCapacityExhausted)
    ));
    let shared = store
        .create_data("one-too-many", DataPlacement::Shared)
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    shared
        .access(&transaction)
        .unwrap()
        .put(b"key", b"value")
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(path).unwrap();
    let shared = store.open_data("one-too-many").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        shared.access(&transaction).unwrap().get(b"key").unwrap(),
        Some(b"value".to_vec())
    );
}
