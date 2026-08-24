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
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self, CodecError> {
        u64::decode_key(bytes).map(Self)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BorrowedKey(Vec<u8>);

impl StoreKey for BorrowedKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.0.as_slice())
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self, CodecError> {
        Ok(Self(bytes))
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

#[test]
fn borrowed_key_codecs_support_points_ranges_and_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<BorrowedKey, u64>(&mut store, "map", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let keys = [
        BorrowedKey(b"a".to_vec()),
        BorrowedKey(b"b".to_vec()),
        BorrowedKey(b"c".to_vec()),
    ];
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(&transaction).unwrap();
        for (value, key) in keys.iter().enumerate() {
            access.put(key, &(value as u64)).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(&transaction).unwrap();
    assert_eq!(access.get(&keys[1]).unwrap(), Some(1));
    let limit = ScanLimit::new(1, 1_024).unwrap();
    let ascending = access
        .scan(
            (
                std::ops::Bound::Included(&keys[0]),
                std::ops::Bound::Included(&keys[2]),
            ),
            ScanDirection::Ascending,
            None,
            limit,
        )
        .unwrap();
    assert_eq!(ascending.items, vec![(keys[0].clone(), 0)]);
    assert_eq!(ascending.continuation, Some(keys[0].clone()));
    let descending = access
        .scan(.., ScanDirection::Descending, None, limit)
        .unwrap();
    assert_eq!(descending.items, vec![(keys[2].clone(), 2)]);
    assert_eq!(descending.continuation, Some(keys[2].clone()));
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BrokenKey;

impl StoreKey for BrokenKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional key failure"))
    }

    fn decode_key(_bytes: Vec<u8>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional key failure"))
    }
}

impl StoreValue for BrokenKey {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional value failure"))
    }

    fn decode_value(_bytes: Vec<u8>) -> Result<Self, CodecError> {
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
