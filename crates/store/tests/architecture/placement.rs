use dogpaddle_store::{Cell, Large, ScanDirection, ScanLimit, Small, Store, StoreError};

use crate::support::{ByteMap, create_byte_map, open_byte_map, raw_database, store_path};

#[test]
fn small_and_large_have_identical_data_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let small = create_byte_map::<Small>(&mut store, "small").unwrap();
    let large = create_byte_map::<Large>(&mut store, "large").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut small = small.access(transaction.access()).unwrap();
        let mut large = large.access(transaction.access()).unwrap();
        for number in 0_u64..10 {
            let key = number.to_be_bytes().to_vec();
            let value = format!("value-{number}").into_bytes();
            small.put(&key, &value).unwrap();
            large.put(&key, &value).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let small = small.access(transaction.access()).unwrap();
    let large = large.access(transaction.access()).unwrap();
    let limit = ScanLimit::new(100, 4_096).unwrap();
    assert_eq!(
        small
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap(),
        large
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap()
    );
}

#[test]
fn catalog_reopens_each_declared_size() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "small").unwrap();
    create_byte_map::<Large>(&mut store, "large").unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let small = open_byte_map::<Small>(&store, "small").unwrap();
    let large = open_byte_map::<Large>(&store, "large").unwrap();
    let mut transactions = store.into_transactions();
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
fn physical_layout_uses_shared_prefixes_and_dedicated_keys() {
    // White-box adapter test. These bytes are private implementation details,
    // so an intentional disk-layout refactor must update this test with them.
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let shared_zero = create_byte_map::<Small>(&mut store, "shared-zero").unwrap();
    let shared_one = create_byte_map::<Small>(&mut store, "shared-one").unwrap();
    let dedicated_zero = create_byte_map::<Large>(&mut store, "dedicated-zero").unwrap();
    let dedicated_one = create_byte_map::<Large>(&mut store, "dedicated-one").unwrap();
    let shared_cell = store.create_data::<Cell<Vec<u8>>>("shared-cell").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    shared_zero
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"shared-zero".to_vec())
        .unwrap();
    shared_one
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"shared-one".to_vec())
        .unwrap();
    dedicated_zero
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"dedicated-zero".to_vec())
        .unwrap();
    dedicated_one
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"dedicated-one".to_vec())
        .unwrap();
    shared_cell
        .access(transaction.access())
        .unwrap()
        .set(&b"cell".to_vec())
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let database = raw_database(&path);
    let transaction = database.begin_ro_txn().unwrap();
    let main = transaction.open_table(None).unwrap();
    let dedicated_zero = transaction.open_table(Some("d/00000000")).unwrap();
    let dedicated_one = transaction.open_table(Some("d/00000001")).unwrap();

    let mut shared_zero_key = vec![3, 0, 0, 0, 0];
    shared_zero_key.extend_from_slice(b"key");
    let mut shared_one_key = vec![3, 0, 0, 0, 1];
    shared_one_key.extend_from_slice(b"key");
    let shared_cell_key = [3, 0, 0, 0, 2];
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
        transaction.get::<Vec<u8>>(&main, &shared_cell_key).unwrap(),
        Some(b"cell".to_vec())
    );
}

#[test]
fn large_capacity_does_not_limit_small_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    for index in 0..Store::LARGE_DATA_CAPACITY {
        create_byte_map::<Large>(&mut store, &format!("large-{index}")).unwrap();
    }
    assert!(matches!(
        store.create_data::<ByteMap<Large>>("one-too-many"),
        Err(StoreError::LargeDataCapacityExhausted)
    ));
    let small = create_byte_map::<Small>(&mut store, "one-too-many").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    small
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"value".to_vec())
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(path).unwrap();
    let small = open_byte_map::<Small>(&store, "one-too-many").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        small
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"value".to_vec())
    );
}
