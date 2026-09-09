use std::fs;

use dogpaddle_store::{Cell, OrderedMap, Store, StoreError};

use crate::support::{ByteMap, create_byte_map, open_byte_map, store_path};

#[test]
fn typed_open_rejects_a_different_collection_kind() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store.create_data::<Cell<u64>>("cell").unwrap();
    create_byte_map(&mut store, "map").unwrap();
    drop(store);

    let store = Store::open(path).unwrap();
    assert!(matches!(
        store.open_data::<ByteMap>("cell"),
        Err(StoreError::DataKindMismatch {
            name,
            expected: "ordered map",
            actual: "cell",
        }) if name == "cell"
    ));
    assert!(matches!(
        store.open_data::<Cell<u64>>("map"),
        Err(StoreError::DataKindMismatch {
            name,
            expected: "cell",
            actual: "ordered map",
        }) if name == "map"
    ));
    assert!(matches!(
        open_byte_map(&store, "missing"),
        Err(StoreError::DataNotFound(name)) if name == "missing"
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
    let keep = partial.join("keep.txt");
    fs::write(&keep, "keep").unwrap();
    assert!(Store::open(&partial).is_err());
    assert_eq!(fs::read_to_string(keep).unwrap(), "keep");
}

#[test]
fn data_names_are_validated_and_unique_across_collection_kinds() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();

    for name in [String::new(), "bad\0name".to_owned(), "x".repeat(256)] {
        assert!(matches!(
            store.create_data::<ByteMap>(&name),
            Err(StoreError::InvalidName { .. })
        ));
    }

    create_byte_map(&mut store, "data").unwrap();
    assert!(matches!(
        store.create_data::<Cell<Vec<u8>>>("data"),
        Err(StoreError::DataAlreadyExists(name)) if name == "data"
    ));
}

#[test]
fn catalog_reopens_named_collections_with_isolated_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let left = create_byte_map(&mut store, "left").unwrap();
    let right = create_byte_map(&mut store, "right").unwrap();
    let marker = store.create_data::<Cell<u64>>("marker").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    left.access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"left".to_vec())
        .unwrap();
    right
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"right".to_vec())
        .unwrap();
    marker
        .access(transaction.access())
        .unwrap()
        .set(&42)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(path).unwrap();
    let left = open_byte_map(&store, "left").unwrap();
    let right = open_byte_map(&store, "right").unwrap();
    let marker = store.open_data::<Cell<u64>>("marker").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        left.read(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"left".to_vec())
    );
    assert_eq!(
        right
            .read(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"right".to_vec())
    );
    assert_eq!(
        marker.read(transaction.access()).unwrap().get().unwrap(),
        Some(42)
    );
}

#[test]
fn staged_setup_publishes_catalog_and_initial_data_atomically() {
    let root = tempfile::tempdir().unwrap();
    let failed_path = root.path().join("failed");
    let mut setup = Store::setup(&failed_path).unwrap();
    let value = setup.create_data::<Cell<u64>>("value").unwrap();
    assert!(matches!(
        setup.commit(|access| {
            value.access(access)?.set(&42)?;
            Err(StoreError::InvalidScanLimit)
        }),
        Err(StoreError::InvalidScanLimit)
    ));

    let mut store = Store::open(&failed_path).unwrap();
    assert!(matches!(
        store.open_data::<Cell<u64>>("value"),
        Err(StoreError::DataNotFound(name)) if name == "value"
    ));
    let value = store.create_data::<Cell<u64>>("value").unwrap();
    let transactions = store.into_transactions();
    let (_, reads) = transactions.split();
    let transaction = reads.begin();
    assert_eq!(
        value.read(transaction.access()).unwrap().get().unwrap(),
        None
    );

    let complete_path = root.path().join("complete");
    let mut setup = Store::setup(&complete_path).unwrap();
    let value = setup.create_data::<Cell<u64>>("value").unwrap();
    let initialized = value.clone();
    let transactions = setup
        .commit(|access| initialized.access(access)?.set(&42))
        .unwrap();
    drop(transactions);

    let store = Store::open(complete_path).unwrap();
    let value = store.open_data::<Cell<u64>>("value").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        value.read(transaction.access()).unwrap().get().unwrap(),
        Some(42)
    );
}

#[test]
fn reopening_after_more_catalog_entries_keeps_existing_bindings() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    create_byte_map(&mut store, "first").unwrap();
    drop(store);

    let mut store = Store::open(&path).unwrap();
    create_byte_map(&mut store, "second").unwrap();
    drop(store);

    let store = Store::open(path).unwrap();
    store
        .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("first")
        .unwrap();
    store
        .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("second")
        .unwrap();
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
