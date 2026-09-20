use std::fs;

use dogpaddle_store::{Cell, OrderedMap, Store, StoreError, StoreSetup};

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
fn dropping_a_setup_draft_never_creates_its_future_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("draft");
    let mut setup = StoreSetup::new();
    setup.create_data::<Cell<u64>>("value").unwrap();
    drop(setup);
    assert!(!path.exists());
}

#[test]
fn staged_setup_publishes_catalog_and_initial_data_atomically() {
    let root = tempfile::tempdir().unwrap();
    let failed_path = root.path().join("failed");
    let mut setup = StoreSetup::new();
    let value = setup.create_data::<Cell<u64>>("value").unwrap();
    assert!(!failed_path.exists());
    assert!(matches!(
        setup.commit(&failed_path, |access| {
            value.access(access)?.set(&42)?;
            Err(StoreError::InvalidScanLimit)
        }),
        Err(StoreError::InvalidScanLimit)
    ));
    assert!(failed_path.exists());
    assert!(matches!(
        Store::open(&failed_path),
        Err(StoreError::InvalidStore)
    ));

    let complete_path = root.path().join("complete");
    let mut setup = StoreSetup::new();
    let value = setup.create_data::<Cell<u64>>("value").unwrap();
    let initialized = value.clone();
    let transactions = setup
        .commit(&complete_path, |access| {
            initialized.access(access)?.set(&42)
        })
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
fn data_scope_strictly_declares_or_looks_up_typed_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut setup = StoreSetup::new();
    let declared = setup.data_scope().data::<Cell<u64>>("value").unwrap();
    assert!(matches!(
        setup.data_scope().data::<Cell<u64>>("value"),
        Err(StoreError::DataAlreadyExists(name)) if name == "value"
    ));
    setup
        .commit(&path, |access| declared.access(access)?.set(&7))
        .unwrap();

    let store = Store::open(path).unwrap();
    let existing = store.data_scope().data::<Cell<u64>>("value").unwrap();
    assert!(matches!(
        store.data_scope().data::<OrderedMap<u64, u64>>("value"),
        Err(StoreError::DataKindMismatch { .. })
    ));
    assert!(matches!(
        store.data_scope().data::<Cell<u64>>("missing"),
        Err(StoreError::DataNotFound(name)) if name == "missing"
    ));
    let transaction = store.read_transaction();
    assert_eq!(
        existing.read(transaction.access()).unwrap().get().unwrap(),
        Some(7)
    );
}

#[test]
fn scoped_data_preserves_literal_names_and_reopens_the_same_binding() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut setup = StoreSetup::new();
    let declared = {
        let mut data = setup.data_scope();
        let mut operation = data.scoped("station/0");
        let mut nested = operation.scoped("../operation//0/");
        let value = nested.data::<Cell<u64>>("count").unwrap();
        assert!(matches!(
            nested.data::<Cell<u64>>("count"),
            Err(StoreError::DataAlreadyExists(name))
                if name == "station/0/../operation//0//count"
        ));
        value
    };
    let sibling = setup
        .data_scope()
        .scoped("station/1")
        .scoped("../operation//0/")
        .data::<Cell<u64>>("count")
        .unwrap();
    setup
        .commit(&path, |access| {
            declared.access(access)?.set(&42)?;
            sibling.access(access)?.set(&17)
        })
        .unwrap();
    let store = Store::open(&path).unwrap();
    let value = store
        .data_scope()
        .scoped("station/0")
        .scoped("../operation//0/")
        .data::<Cell<u64>>("count")
        .unwrap();
    assert!(matches!(
        store.data_scope().scoped("station/0").scoped("../operation//0/")
            .data::<OrderedMap<u64, u64>>("count"),
        Err(StoreError::DataKindMismatch { name, .. })
            if name == "station/0/../operation//0//count"
    ));
    assert!(matches!(
        store.data_scope().scoped("station/0").data::<Cell<u64>>("missing"),
        Err(StoreError::DataNotFound(name)) if name == "station/0/missing"
    ));
    // The literal catalog name is also available through the root Store API.
    store
        .open_data::<Cell<u64>>("station/0/../operation//0//count")
        .unwrap();
    let sibling = store
        .data_scope()
        .scoped("station/1")
        .scoped("../operation//0/")
        .data::<Cell<u64>>("count")
        .unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        value.read(transaction.access()).unwrap().get().unwrap(),
        Some(42)
    );
    assert_eq!(
        sibling.read(transaction.access()).unwrap().get().unwrap(),
        Some(17)
    );
}

#[test]
fn child_scope_does_not_change_its_parent_or_normalize_empty_prefixes() {
    let mut setup = StoreSetup::new();
    let mut root = setup.data_scope();
    root.data::<Cell<u64>>("value").unwrap();
    root.scoped("").data::<Cell<u64>>("value").unwrap();
    root.scoped("")
        .scoped("")
        .data::<Cell<u64>>("value")
        .unwrap();
    assert!(matches!(root.data::<Cell<u64>>("/value"),
        Err(StoreError::DataAlreadyExists(name)) if name == "/value"));
    assert!(matches!(root.data::<Cell<u64>>("//value"),
        Err(StoreError::DataAlreadyExists(name)) if name == "//value"));
    assert!(matches!(root.data::<Cell<u64>>("value"),
        Err(StoreError::DataAlreadyExists(name)) if name == "value"));
}

#[test]
fn scoped_names_are_validated_only_when_data_is_requested() {
    let mut setup = StoreSetup::new();
    let mut root = setup.data_scope();
    drop(root.scoped("unused\0prefix"));
    assert!(
        matches!(root.scoped("bad\0prefix").data::<Cell<u64>>("value"),
        Err(StoreError::InvalidName { name, .. }) if name == "bad\0prefix/value")
    );
    let prefix = "x".repeat(254);
    let expected = format!("{prefix}/x");
    assert!(matches!(root.scoped(&prefix).data::<Cell<u64>>("x"),
        Err(StoreError::InvalidName { name, .. }) if name == expected));
    root.data::<Cell<u64>>("valid").unwrap();
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
