#[path = "ordered_map_errors.rs"]
mod errors;
#[path = "ordered_map_scans.rs"]
mod scans;

use std::borrow::Cow;

use dogpaddle_store::{
    CodecError, OrderedMap, ScanDirection, ScanLimit, Store, StoreError, StoreKey, StoreValue,
};

use crate::support::{TestValue, create_byte_map, create_map, store_path};

fn open_map<K: StoreKey, V: StoreValue>(
    store: &Store,
    name: &str,
) -> Result<OrderedMap<K, V>, StoreError> {
    store.open_data(name)
}

#[test]
fn ordered_map_point_operations_are_exact() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, String>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    let mut access = map.access(transaction.access()).unwrap();
    assert_eq!(access.get(&7).unwrap(), None);
    access.put(&7, &"first".to_owned()).unwrap();
    assert_eq!(access.get(&7).unwrap(), Some("first".to_owned()));
    access.put(&7, &"second".to_owned()).unwrap();
    assert_eq!(access.get(&7).unwrap(), Some("second".to_owned()));
    access.erase(&7).unwrap();
    assert_eq!(access.get(&7).unwrap(), None);
    access.erase(&7).unwrap();
    access.put(&7, &"third".to_owned()).unwrap();
    assert!(access.remove(&7).unwrap());
    assert!(!access.remove(&7).unwrap());
    assert_eq!(access.get(&7).unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn ordered_map_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let map = create_map::<u64, TestValue>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    map.access(transaction.access())
        .unwrap()
        .put(&42, &TestValue(9))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let map = open_map::<u64, TestValue>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        map.access(transaction.access()).unwrap().get(&42).unwrap(),
        Some(TestValue(9))
    );
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestKey(u64);

impl StoreKey for TestKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        u64::decode_key(bytes).map(Self)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SliceKey(Vec<u8>);

impl StoreKey for SliceKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.0.as_slice())
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Ok(Self(bytes.into_owned()))
    }
}

#[test]
fn ordered_map_accepts_external_key_and_value_codecs() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<TestKey, TestValue>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    map.access(transaction.access())
        .unwrap()
        .put(&TestKey(3), &TestValue(4))
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    assert_eq!(
        map.access(transaction.access())
            .unwrap()
            .get(&TestKey(3))
            .unwrap(),
        Some(TestValue(4))
    );
}

#[test]
fn slice_backed_key_codecs_support_points_ranges_and_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<SliceKey, u64>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let keys = [
        SliceKey(b"a".to_vec()),
        SliceKey(b"b".to_vec()),
        SliceKey(b"c".to_vec()),
    ];
    {
        let transaction = transactions.begin();
        let mut access = map.access(transaction.access()).unwrap();
        for (value, key) in keys.iter().enumerate() {
            access.put(key, &(value as u64)).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
    let access = map.access(transaction.access()).unwrap();
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
    assert_eq!(ascending.entries, vec![(keys[0].clone(), 0)]);
    assert_eq!(ascending.continuation, Some(keys[0].clone()));
    let descending = access
        .scan(.., ScanDirection::Descending, None, limit)
        .unwrap();
    assert_eq!(descending.entries, vec![(keys[2].clone(), 2)]);
    assert_eq!(descending.continuation, Some(keys[2].clone()));
}

#[test]
fn data_objects_isolate_identical_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let left = create_byte_map(&mut store, "left").unwrap();
    let right = create_byte_map(&mut store, "right").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin();
        let mut left = left.access(transaction.access()).unwrap();
        let mut right = right.access(transaction.access()).unwrap();
        left.put(&Vec::new(), &b"left-empty".to_vec()).unwrap();
        left.put(&b"key".to_vec(), &b"left".to_vec()).unwrap();
        right.put(&Vec::new(), &b"right-empty".to_vec()).unwrap();
        right.put(&b"key".to_vec(), &b"right".to_vec()).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
    let left = left.access(transaction.access()).unwrap();
    let right = right.access(transaction.access()).unwrap();
    assert_eq!(left.get(&Vec::new()).unwrap(), Some(b"left-empty".to_vec()));
    assert_eq!(left.get(&b"key".to_vec()).unwrap(), Some(b"left".to_vec()));
    assert_eq!(
        right.get(&Vec::new()).unwrap(),
        Some(b"right-empty".to_vec())
    );
    assert_eq!(
        right.get(&b"key".to_vec()).unwrap(),
        Some(b"right".to_vec())
    );
}

#[test]
fn bounded_point_read_checks_value_length_and_can_retry_without_poisoning() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let map = create_map::<u64, Vec<u8>>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        let mut access = map.access(transaction.access()).unwrap();
        access.put(&7, &vec![1; 64]).unwrap();
        assert!(matches!(
            access.get_bounded(&7, 63),
            Err(StoreError::ItemTooLarge {
                size: 64,
                limit: 63
            })
        ));
        assert_eq!(access.get_bounded(&7, 64).unwrap(), Some(vec![1; 64]));
        assert_eq!(access.get_bounded(&9, 0).unwrap(), None);
        transaction.commit().unwrap();
    }
    drop(transactions);
    let store = Store::open(&path).unwrap();
    let map = open_map::<u64, Vec<u8>>(&store, "map").unwrap();
    let snapshot = store.read_transaction();
    let access = map.read(snapshot.access()).unwrap();
    assert!(matches!(
        access.get_bounded(&7, 0),
        Err(StoreError::ItemTooLarge { .. })
    ));
    assert_eq!(access.get_bounded(&7, 64).unwrap(), Some(vec![1; 64]));
}

#[test]
fn non_clone_keys_continue_owned_pages_in_both_directions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<TestKey, u64>(&mut store, "map").unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    {
        let transaction = writes.begin();
        let mut access = map.access(transaction.access()).unwrap();
        for key in 1..=3 {
            access.put(&TestKey(key), &key).unwrap();
        }
        transaction.commit().unwrap();
    }
    for (direction, expected) in [
        (ScanDirection::Ascending, vec![1, 2, 3]),
        (ScanDirection::Descending, vec![3, 2, 1]),
    ] {
        let mut continuation = None;
        let mut actual = Vec::new();
        loop {
            let page = {
                let snapshot = reads.begin();
                map.read(snapshot.access())
                    .unwrap()
                    .scan(
                        ..,
                        direction,
                        continuation.as_ref(),
                        ScanLimit::new(1, 16).unwrap(),
                    )
                    .unwrap()
            };
            assert_eq!(page.entries.len(), 1);
            let (key, value) = &page.entries[0];
            assert_eq!(key.0, *value);
            actual.push(key.0);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
            assert!(actual.len() < 3);
        }
        assert_eq!(actual, expected);
    }
}
