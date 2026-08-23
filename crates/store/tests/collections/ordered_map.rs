use dogpaddle_store::{
    CodecError, DataPlacement, OrderedMap, ScanDirection, ScanLimit, Store, StoreError, StoreKey,
    StoreValue,
};

use crate::support::{PLACEMENTS, TestValue, create_map, store_path};

fn open_map<K: StoreKey, V: StoreValue>(
    store: &Store,
    name: &str,
) -> Result<OrderedMap<K, V>, StoreError> {
    Ok(OrderedMap::new(store.open_data(name)?))
}

#[test]
fn ordered_map_point_operations_are_exact() {
    for placement in PLACEMENTS {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let map = create_map::<u64, String>(&mut store, "map", placement).unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin().unwrap();
        let mut access = map.access(&transaction).unwrap();
        assert_eq!(access.get(&7).unwrap(), None);
        access.put(&7, &"first".to_owned()).unwrap();
        assert_eq!(access.get(&7).unwrap(), Some("first".to_owned()));
        access.put(&7, &"second".to_owned()).unwrap();
        assert_eq!(access.get(&7).unwrap(), Some("second".to_owned()));
        assert!(access.remove(&7).unwrap());
        assert!(!access.remove(&7).unwrap());
        assert_eq!(access.get(&7).unwrap(), None);
        transaction.commit().unwrap();
    }
}

#[test]
fn ordered_map_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let map = create_map::<u64, TestValue>(&mut store, "map", DataPlacement::Dedicated).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    map.access(&transaction)
        .unwrap()
        .put(&42, &TestValue(9))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let map = open_map::<u64, TestValue>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(&transaction).unwrap().get(&42).unwrap(),
        Some(TestValue(9))
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestKey(u64);

impl StoreKey for TestKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        u64::decode_key(bytes).map(Self)
    }
}

#[test]
fn ordered_map_accepts_external_key_and_value_codecs() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<TestKey, TestValue>(&mut store, "map", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    map.access(&transaction)
        .unwrap()
        .put(&TestKey(3), &TestValue(4))
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(&transaction).unwrap().get(&TestKey(3)).unwrap(),
        Some(TestValue(4))
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BrokenKey;

impl StoreKey for BrokenKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::new("intentional key failure"))
    }

    fn decode_key(_bytes: &[u8]) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional key failure"))
    }
}

impl StoreValue for BrokenKey {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::new("intentional value failure"))
    }

    fn decode_value(_bytes: &[u8]) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional value failure"))
    }
}

#[test]
fn key_codec_errors_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_map::<u64, u64>(&mut store, "safe", DataPlacement::Shared).unwrap();
    let broken =
        create_map::<BrokenKey, BrokenKey>(&mut store, "broken", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(&transaction).unwrap().put(&1, &1).unwrap();
    assert!(matches!(
        broken.access(&transaction).unwrap().get(&BrokenKey),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(safe.access(&transaction).unwrap().get(&1).unwrap(), None);
}

#[test]
fn scan_decode_errors_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_map::<u64, u64>(&mut store, "safe", DataPlacement::Shared).unwrap();
    let raw = store.create_data("raw", DataPlacement::Shared).unwrap();
    let malformed = OrderedMap::<u64, u64>::new(raw.clone());
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(&transaction).unwrap().put(&1, &1).unwrap();
    raw.access(&transaction)
        .unwrap()
        .put(&[0], &0_u64.to_be_bytes())
        .unwrap();
    assert!(matches!(
        malformed.access(&transaction).unwrap().scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
        ),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(safe.access(&transaction).unwrap().get(&1).unwrap(), None);
    assert_eq!(raw.access(&transaction).unwrap().get(&[0]).unwrap(), None);
}
