use std::fs;

use dogpaddle_store::{Large, Small, Store, StoreError};
use libmdbx::WriteFlags;

use crate::support::{ByteMap, create_byte_map, open_byte_map, raw_database, store_path};

#[test]
fn creates_opens_and_recovers_named_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "small").unwrap();
    create_byte_map::<Large>(&mut store, "large").unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    open_byte_map::<Small>(&store, "small").unwrap();
    open_byte_map::<Large>(&store, "large").unwrap();
    assert!(matches!(
        store.open_data::<ByteMap<Small>>("missing"),
        Err(StoreError::DataNotFound(name)) if name == "missing"
    ));
}

#[test]
fn typed_open_rejects_a_different_durable_size() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "small").unwrap();
    create_byte_map::<Large>(&mut store, "large").unwrap();
    drop(store);

    let store = Store::open(path).unwrap();
    assert!(matches!(
        open_byte_map::<Large>(&store, "small"),
        Err(StoreError::DataSizeMismatch {
            name,
            expected: "large",
            actual: "small",
        }) if name == "small"
    ));
    assert!(matches!(
        open_byte_map::<Small>(&store, "large"),
        Err(StoreError::DataSizeMismatch {
            name,
            expected: "small",
            actual: "large",
        }) if name == "large"
    ));
}

#[test]
fn creation_requires_an_unused_path_without_deleting_its_contents() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    fs::create_dir(&path).unwrap();
    let keep = path.join("keep.txt");
    fs::write(&keep, "keep").unwrap();

    assert!(matches!(
        Store::create(&path),
        Err(StoreError::PathExists(_))
    ));
    assert_eq!(fs::read_to_string(keep).unwrap(), "keep");
}

#[test]
fn opening_rejects_missing_and_partial_directories() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing");
    assert!(matches!(
        Store::open(&missing),
        Err(StoreError::StoreNotFound(_))
    ));

    let partial = root.path().join("partial");
    fs::create_dir(&partial).unwrap();
    assert!(matches!(
        Store::open(&partial),
        Err(StoreError::StoreNotFound(_))
    ));
    assert_eq!(fs::read_dir(&partial).unwrap().count(), 0);
}

#[test]
fn data_names_are_validated_and_unique_across_sizes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();

    for name in [String::new(), "bad\0name".to_owned(), "x".repeat(256)] {
        assert!(matches!(
            store.create_data::<ByteMap<Small>>(&name),
            Err(StoreError::InvalidName { .. })
        ));
    }

    create_byte_map::<Small>(&mut store, "data").unwrap();
    assert!(matches!(
        store.create_data::<ByteMap<Large>>("data"),
        Err(StoreError::DataAlreadyExists(name)) if name == "data"
    ));
}

#[test]
fn opening_rejects_an_invalid_store_marker() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    drop(Store::create(&path).unwrap());

    let database = raw_database(&path);
    let transaction = database.begin_rw_txn().unwrap();
    let table = transaction.open_table(None).unwrap();
    transaction
        .put(&table, [0], b"not-dogpaddle", WriteFlags::UPSERT)
        .unwrap();
    assert!(!transaction.commit().unwrap());
    drop(database);

    assert!(matches!(Store::open(&path), Err(StoreError::InvalidStore)));
}

#[test]
fn opening_rejects_a_corrupt_catalog_binding() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "data").unwrap();
    drop(store);

    let database = raw_database(&path);
    let transaction = database.begin_rw_txn().unwrap();
    let table = transaction.open_table(None).unwrap();
    let mut catalog_key = vec![2];
    catalog_key.extend_from_slice(b"data");
    transaction
        .put(&table, &catalog_key, [9, 0, 0, 0, 0], WriteFlags::UPSERT)
        .unwrap();
    assert!(!transaction.commit().unwrap());
    drop(database);

    assert!(matches!(Store::open(&path), Err(StoreError::InvalidStore)));
}

#[test]
fn opening_rejects_duplicate_physical_catalog_bindings() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "left").unwrap();
    create_byte_map::<Small>(&mut store, "right").unwrap();
    drop(store);

    let database = raw_database(&path);
    let transaction = database.begin_rw_txn().unwrap();
    let table = transaction.open_table(None).unwrap();
    let mut right_catalog_key = vec![2];
    right_catalog_key.extend_from_slice(b"right");
    transaction
        .put(
            &table,
            &right_catalog_key,
            [0, 0, 0, 0, 0],
            WriteFlags::UPSERT,
        )
        .unwrap();
    assert!(!transaction.commit().unwrap());
    drop(database);

    assert!(matches!(Store::open(&path), Err(StoreError::InvalidStore)));
}

#[test]
fn opening_rejects_a_catalog_counter_behind_its_bindings() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map::<Small>(&mut store, "zero").unwrap();
    create_byte_map::<Small>(&mut store, "one").unwrap();
    drop(store);

    let database = raw_database(&path);
    let transaction = database.begin_rw_txn().unwrap();
    let table = transaction.open_table(None).unwrap();
    transaction
        .put(&table, [1], 1_u32.to_be_bytes(), WriteFlags::UPSERT)
        .unwrap();
    assert!(!transaction.commit().unwrap());
    drop(database);

    assert!(matches!(Store::open(&path), Err(StoreError::InvalidStore)));
}

#[cfg(unix)]
#[test]
fn opening_preserves_non_not_found_metadata_errors() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    symlink("store", &path).unwrap();

    assert!(matches!(
        Store::open(&path),
        Err(StoreError::Storage {
            operation: "inspect store directory",
            ..
        })
    ));
}
