use std::{ops::Bound, path::Path};

use dogpaddle_store::{
    Cell, CodecError, DataPlacement, Map, ScanDirection, ScanLimit, Store, StoreError, StoreKey,
    StoreValue, Transactions,
};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};
use tempfile::TempDir;

#[derive(Clone, Debug, Eq, PartialEq)]
struct User(u64);

impl StoreValue for User {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        self.0.encode_value()
    }

    fn decode_value(bytes: &[u8]) -> Result<Self, CodecError> {
        u64::decode_value(bytes).map(Self)
    }
}

fn path(root: &TempDir) -> std::path::PathBuf {
    root.path().join("store")
}

fn create_store(path: &Path) -> (Transactions, Cell<u64>, Map<u64, User>) {
    let mut store = Store::create(path).expect("create store");
    let counter = create_cell(&mut store, "counter").expect("create counter");
    let users = create_map(&mut store, "users").expect("create users");
    (store.into_transactions(), counter, users)
}

fn create_cell<T: StoreValue>(store: &mut Store, name: &str) -> Result<Cell<T>, StoreError> {
    Ok(Cell::new(store.create_data(name, DataPlacement::Shared)?))
}

fn open_cell<T: StoreValue>(store: &Store, name: &str) -> Result<Cell<T>, StoreError> {
    Ok(Cell::new(store.open_data(name)?))
}

fn create_map<K: StoreKey, V: StoreValue>(
    store: &mut Store,
    name: &str,
) -> Result<Map<K, V>, StoreError> {
    Ok(Map::new(store.create_data(name, DataPlacement::Shared)?))
}

fn open_map<K: StoreKey, V: StoreValue>(
    store: &Store,
    name: &str,
) -> Result<Map<K, V>, StoreError> {
    Ok(Map::new(store.open_data(name)?))
}

#[test]
fn generic_data_namespaces_are_isolated_and_transactional() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    let data = store.create_data("raw", DataPlacement::Shared).unwrap();
    let other = store.create_data("other", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    {
        let mut data = data.access(&transaction).unwrap();
        let mut other = other.access(&transaction).unwrap();
        data.put(b"key", b"value").unwrap();
        data.put(b"z", b"last").unwrap();
        other.put(b"key", b"other value").unwrap();
        other.put(b"z", b"other last").unwrap();
        assert_eq!(data.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(other.get(b"key").unwrap(), Some(b"other value".to_vec()));
    }
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let data = data.access(&transaction).unwrap();
    let other = other.access(&transaction).unwrap();
    assert_eq!(data.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(other.get(b"key").unwrap(), Some(b"other value".to_vec()));
    let limit = ScanLimit::new(10, 1_024).unwrap();
    let data_batch = data
        .scan(.., ScanDirection::Ascending, None, limit)
        .unwrap();
    assert_eq!(
        data_batch.items,
        vec![
            (b"key".to_vec(), b"value".to_vec()),
            (b"z".to_vec(), b"last".to_vec()),
        ]
    );
    assert_eq!(data_batch.continuation, None);
    let other_batch = other
        .scan(.., ScanDirection::Descending, None, limit)
        .unwrap();
    assert_eq!(
        other_batch.items,
        vec![
            (b"z".to_vec(), b"other last".to_vec()),
            (b"key".to_vec(), b"other value".to_vec()),
        ]
    );
    assert_eq!(other_batch.continuation, None);
}

#[test]
fn cell_and_map_commit_atomically_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let store_path = path(&root);
    let mut store = Store::create(&store_path).unwrap();
    let counter = create_cell::<u64>(&mut store, "counter").unwrap();
    let users = Map::<u64, User>::new(
        store
            .create_data("users", DataPlacement::Dedicated)
            .unwrap(),
    );
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        counter.access(&transaction).unwrap().set(&6).unwrap();
        users
            .access(&transaction)
            .unwrap()
            .put(&41, &User(8))
            .unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(counter.access(&transaction).unwrap().get().unwrap(), None);
    assert_eq!(users.access(&transaction).unwrap().get(&41).unwrap(), None);
    drop(transaction);

    let transaction = transactions.begin().unwrap();
    {
        let mut counter = counter.access(&transaction).unwrap();
        let mut users = users.access(&transaction).unwrap();
        counter.set(&7).unwrap();
        users.put(&42, &User(9)).unwrap();
        assert_eq!(counter.get().unwrap(), Some(7));
        assert_eq!(users.get(&42).unwrap(), Some(User(9)));
    }
    transaction.commit().unwrap();
    drop(transactions);

    let reopened = Store::open(&store_path).unwrap();
    let counter = open_cell::<u64>(&reopened, "counter").unwrap();
    let users = open_map::<u64, User>(&reopened, "users").unwrap();
    let mut transactions = reopened.into_transactions();
    let transaction = transactions.begin().unwrap();
    let counter = counter.access(&transaction).unwrap();
    let users = users.access(&transaction).unwrap();
    assert_eq!(counter.get().unwrap(), Some(7));
    assert_eq!(users.get(&42).unwrap(), Some(User(9)));
}

#[test]
fn dropping_a_transaction_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let (mut transactions, counter, users) = create_store(&path(&root));

    {
        let transaction = transactions.begin().unwrap();
        let mut counter = counter.access(&transaction).unwrap();
        let mut users = users.access(&transaction).unwrap();
        counter.set(&1).unwrap();
        users.put(&1, &User(1)).unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let counter = counter.access(&transaction).unwrap();
    let users = users.access(&transaction).unwrap();
    assert_eq!(counter.get().unwrap(), None);
    assert_eq!(users.get(&1).unwrap(), None);
}

#[test]
fn ordered_scans_support_ranges_directions_and_exclusive_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    let values = Map::<i64, String>::new(
        store
            .create_data("values", DataPlacement::Dedicated)
            .unwrap(),
    );
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    {
        let mut values = values.access(&transaction).unwrap();
        for key in [-2, -1, 0, 1, 2, 3] {
            values.put(&key, &format!("v{key}")).unwrap();
        }
    }
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let values = values.access(&transaction).unwrap();
    let limit = ScanLimit::new(2, 1_024).unwrap();
    let first = values
        .scan(
            (Bound::Included(-1), Bound::Included(2)),
            ScanDirection::Ascending,
            None,
            limit,
        )
        .unwrap();
    assert_eq!(first.items, vec![(-1, "v-1".into()), (0, "v0".into())]);
    assert_eq!(first.continuation, Some(0));
    let second = values
        .scan(
            (Bound::Included(-1), Bound::Included(2)),
            ScanDirection::Ascending,
            first.continuation.as_ref(),
            limit,
        )
        .unwrap();
    assert_eq!(second.items, vec![(1, "v1".into()), (2, "v2".into())]);
    assert_eq!(second.continuation, None);

    let descending_first = values
        .scan(
            (Bound::Included(-1), Bound::Included(2)),
            ScanDirection::Descending,
            None,
            limit,
        )
        .unwrap();
    assert_eq!(
        descending_first.items,
        vec![(2, "v2".into()), (1, "v1".into())]
    );
    assert_eq!(descending_first.continuation, Some(1));
    let descending_second = values
        .scan(
            (Bound::Included(-1), Bound::Included(2)),
            ScanDirection::Descending,
            descending_first.continuation.as_ref(),
            limit,
        )
        .unwrap();
    assert_eq!(
        descending_second.items,
        vec![(0, "v0".into()), (-1, "v-1".into())]
    );
    assert_eq!(descending_second.continuation, None);

    let exclusive = values
        .scan(
            (Bound::Excluded(-1), Bound::Excluded(2)),
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
        )
        .unwrap();
    assert_eq!(exclusive.items, vec![(0, "v0".into()), (1, "v1".into())]);
    assert_eq!(exclusive.continuation, None);

    let byte_limited = values
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 22).unwrap(),
        )
        .unwrap();
    assert_eq!(
        byte_limited.items,
        vec![(-2, "v-2".into()), (-1, "v-1".into())]
    );
    assert_eq!(byte_limited.continuation, Some(-1));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BrokenInvariant;

#[test]
fn custom_collection_hard_error_poison_aborts_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    let data = store.create_data("custom", DataPlacement::Shared).unwrap();
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
fn wrong_store_handle_poison_aborts_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let (mut first, first_counter, _) = create_store(&root.path().join("first"));
    let (mut second, second_counter, _) = create_store(&root.path().join("second"));

    let transaction = first.begin().unwrap();
    let mut first_access = first_counter.access(&transaction).unwrap();
    first_access.set(&10).unwrap();
    assert!(matches!(
        second_counter.access(&transaction),
        Err(StoreError::WrongStore)
    ));
    assert!(matches!(
        first_access.get(),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let first_tx = first.begin().unwrap();
    assert_eq!(
        first_counter.access(&first_tx).unwrap().get().unwrap(),
        None
    );
    let second_tx = second.begin().unwrap();
    assert_eq!(
        second_counter.access(&second_tx).unwrap().get().unwrap(),
        None
    );
}

struct BrokenValue;

impl StoreValue for BrokenValue {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::new("intentional encode failure"))
    }

    fn decode_value(_bytes: &[u8]) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional decode failure"))
    }
}

#[test]
fn codec_error_poison_aborts_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    let counter = create_cell::<u64>(&mut store, "counter").unwrap();
    let broken_data = store.create_data("broken", DataPlacement::Shared).unwrap();
    let broken = Cell::<BrokenValue>::new(broken_data.clone());
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut counter_access = counter.access(&transaction).unwrap();
    let mut broken_access = broken.access(&transaction).unwrap();
    counter_access.set(&99).unwrap();
    assert!(matches!(
        broken_access.set(&BrokenValue),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    let transaction = transactions.begin().unwrap();
    assert_eq!(counter.access(&transaction).unwrap().get().unwrap(), None);
    drop(transaction);

    let transaction = transactions.begin().unwrap();
    let mut counter_access = counter.access(&transaction).unwrap();
    let mut raw_access = broken_data.access(&transaction).unwrap();
    counter_access.set(&100).unwrap();
    raw_access.put(b"", b"invalid").unwrap();
    let broken_access = broken.access(&transaction).unwrap();
    assert!(matches!(broken_access.get(), Err(StoreError::Codec(_))));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    let transaction = transactions.begin().unwrap();
    assert_eq!(counter.access(&transaction).unwrap().get().unwrap(), None);
    assert_eq!(
        broken_data.access(&transaction).unwrap().get(b"").unwrap(),
        None
    );
}

#[test]
fn data_handle_owns_name_and_collections_only_wrap_it() {
    let root = tempfile::tempdir().unwrap();
    let store_path = path(&root);
    let (store, _, _) = create_store(&store_path);
    drop(store);

    let store = Store::open(&store_path).unwrap();
    assert!(matches!(
        open_cell::<u64>(&store, "missing"),
        Err(StoreError::DataNotFound(_))
    ));
    let counter_data = store.open_data("counter").unwrap();
    assert_eq!(counter_data.name(), "counter");
    let _counter = Cell::<u64>::new(counter_data);
}

#[test]
fn data_names_are_validated() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    assert!(matches!(
        create_cell::<u64>(&mut store, ""),
        Err(StoreError::InvalidName { .. })
    ));
    assert!(matches!(
        create_cell::<u64>(&mut store, "bad\0name"),
        Err(StoreError::InvalidName { .. })
    ));
    assert!(matches!(
        store.create_data("bad\0raw", DataPlacement::Shared),
        Err(StoreError::InvalidName { .. })
    ));
}

#[test]
fn remove_and_clear_report_presence() {
    let root = tempfile::tempdir().unwrap();
    let (mut transactions, counter, users) = create_store(&path(&root));
    let transaction = transactions.begin().unwrap();
    let mut counter = counter.access(&transaction).unwrap();
    let mut users = users.access(&transaction).unwrap();
    counter.set(&5).unwrap();
    users.put(&5, &User(5)).unwrap();
    assert!(counter.clear().unwrap());
    assert!(!counter.clear().unwrap());
    assert!(users.remove(&5).unwrap());
    assert!(!users.remove(&5).unwrap());
    transaction.commit().unwrap();
}

#[test]
fn named_data_handle_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let store_path = path(&root);
    let mut store = Store::create(&store_path).unwrap();
    let data = store.create_data("raw", DataPlacement::Shared).unwrap();
    assert_eq!(data.name(), "raw");
    assert!(matches!(
        store.create_data("raw", DataPlacement::Shared),
        Err(StoreError::DataAlreadyExists(_))
    ));
    drop(store);

    let store = Store::open(&store_path).unwrap();
    assert_eq!(store.open_data("raw").unwrap().name(), "raw");
    assert!(matches!(
        Store::create(&store_path),
        Err(StoreError::PathExists(_))
    ));
}

#[test]
fn scan_limits_remain_soft_errors() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();
    let values = create_map::<u64, String>(&mut store, "values").unwrap();
    assert!(matches!(
        ScanLimit::new(0, 1),
        Err(StoreError::InvalidScanLimit)
    ));
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut values_access = values.access(&transaction).unwrap();
    values_access.put(&1, &"wide".to_owned()).unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let mut values_access = values.access(&transaction).unwrap();
    assert!(matches!(
        values_access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    values_access.put(&2, &"still writable".to_owned()).unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let values = values.access(&transaction).unwrap();
    assert_eq!(values.get(&1).unwrap(), Some("wide".to_owned()));
    assert_eq!(values.get(&2).unwrap(), Some("still writable".to_owned()));
}

#[test]
fn transactions_can_move_to_the_stage_thread() {
    let root = tempfile::tempdir().unwrap();
    let (transactions, counter, _) = create_store(&path(&root));

    let (mut transactions, counter) = std::thread::spawn(move || {
        let mut transactions = transactions;
        let transaction = transactions.begin().unwrap();
        counter.access(&transaction).unwrap().set(&7).unwrap();
        transaction.commit().unwrap();
        (transactions, counter)
    })
    .join()
    .unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        counter.access(&transaction).unwrap().get().unwrap(),
        Some(7)
    );
}

#[test]
fn dedicated_data_capacity_is_explicit_and_shared_data_remains_available() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(path(&root)).unwrap();

    for index in 0..Store::DEDICATED_CAPACITY {
        store
            .create_data(&format!("large-{index}"), DataPlacement::Dedicated)
            .unwrap();
    }

    assert!(matches!(
        store.create_data("one-too-many", DataPlacement::Dedicated),
        Err(StoreError::DedicatedCapacityExhausted)
    ));
    store.create_data("small", DataPlacement::Shared).unwrap();
}

#[test]
fn placement_has_a_stable_physical_layout() {
    let root = tempfile::tempdir().unwrap();
    let store_path = path(&root);
    let mut store = Store::create(&store_path).unwrap();
    let shared = store.create_data("shared", DataPlacement::Shared).unwrap();
    let dedicated = store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    shared
        .access(&transaction)
        .unwrap()
        .put(b"key", b"shared-value")
        .unwrap();
    dedicated
        .access(&transaction)
        .unwrap()
        .put(b"key", b"dedicated-value")
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let database = Database::<NoWriteMap>::open_with_options(
        &store_path,
        DatabaseOptions {
            permissions: Some(0o600),
            max_tables: Some(u64::from(Store::DEDICATED_CAPACITY)),
            exclusive: true,
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let transaction = database.begin_ro_txn().unwrap();
    let main = transaction.open_table(None).unwrap();
    let dedicated = transaction.open_table(Some("d/00000000")).unwrap();

    let mut shared_key = vec![3, 0, 0, 0, 0];
    shared_key.extend_from_slice(b"key");
    assert_eq!(
        transaction.get::<Vec<u8>>(&main, &shared_key).unwrap(),
        Some(b"shared-value".to_vec())
    );
    assert_eq!(
        transaction.get::<Vec<u8>>(&dedicated, b"key").unwrap(),
        Some(b"dedicated-value".to_vec())
    );
}
