#[path = "ordered_map_errors.rs"]
mod errors;
#[path = "ordered_map_scans.rs"]
mod scans;

use std::borrow::Cow;

use dogpaddle_store::{
    CodecError, Large, OrderedMap, ScanDirection, ScanLimit, Small, Store, StoreData, StoreError,
    StoreKey, StoreValue,
};

use crate::support::{ByteMap, TestValue, create_byte_map, create_map, store_path};

fn open_map<K: StoreKey, V: StoreValue, SIZE>(
    store: &Store,
    name: &str,
) -> Result<OrderedMap<K, V, SIZE>, StoreError>
where
    OrderedMap<K, V, SIZE>: StoreData,
{
    store.open_data(name)
}

#[test]
fn ordered_map_point_operations_are_exact() {
    assert_ordered_map_point_operations::<Small>();
    assert_ordered_map_point_operations::<Large>();
}

fn assert_ordered_map_point_operations<SIZE>()
where
    OrderedMap<u64, String, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, String, SIZE>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = map.access(transaction.access()).unwrap();
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

#[test]
fn ordered_map_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let map = create_map::<u64, TestValue, Large>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    map.access(transaction.access())
        .unwrap()
        .put(&42, &TestValue(9))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let map = open_map::<u64, TestValue, Large>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(transaction.access()).unwrap().get(&42).unwrap(),
        Some(TestValue(9))
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
struct BorrowedKey(Vec<u8>);

impl StoreKey for BorrowedKey {
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
    let map = create_map::<TestKey, TestValue, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    map.access(transaction.access())
        .unwrap()
        .put(&TestKey(3), &TestValue(4))
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(transaction.access())
            .unwrap()
            .get(&TestKey(3))
            .unwrap(),
        Some(TestValue(4))
    );
}

#[test]
fn borrowed_key_codecs_support_points_ranges_and_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<BorrowedKey, u64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let keys = [
        BorrowedKey(b"a".to_vec()),
        BorrowedKey(b"b".to_vec()),
        BorrowedKey(b"c".to_vec()),
    ];
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(transaction.access()).unwrap();
        for (value, key) in keys.iter().enumerate() {
            access.put(key, &(value as u64)).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    assert_eq!(access.get(&keys[1]).unwrap(), Some(1));
    let limit = ScanLimit::new(1, 1_024).unwrap();
    let mut ascending_items = Vec::new();
    let ascending_continuation = access
        .scan(
            (
                std::ops::Bound::Included(&keys[0]),
                std::ops::Bound::Included(&keys[2]),
            ),
            ScanDirection::Ascending,
            None,
            limit,
            |entry| {
                ascending_items.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(ascending_items, vec![(keys[0].clone(), 0)]);
    assert_eq!(ascending_continuation, Some(keys[0].clone()));
    let mut descending_items = Vec::new();
    let descending_continuation = access
        .scan(.., ScanDirection::Descending, None, limit, |entry| {
            descending_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(descending_items, vec![(keys[2].clone(), 2)]);
    assert_eq!(descending_continuation, Some(keys[2].clone()));
}

#[test]
fn data_objects_isolate_identical_keys() {
    assert_data_objects_isolate_identical_keys::<Small>();
    assert_data_objects_isolate_identical_keys::<Large>();
}

fn assert_data_objects_isolate_identical_keys<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let left = create_byte_map::<SIZE>(&mut store, "left").unwrap();
    let right = create_byte_map::<SIZE>(&mut store, "right").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut left = left.access(transaction.access()).unwrap();
        let mut right = right.access(transaction.access()).unwrap();
        left.put(&Vec::new(), &b"left-empty".to_vec()).unwrap();
        left.put(&b"key".to_vec(), &b"left".to_vec()).unwrap();
        right.put(&Vec::new(), &b"right-empty".to_vec()).unwrap();
        right.put(&b"key".to_vec(), &b"right".to_vec()).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
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

    let limit = ScanLimit::new(10, 1_024).unwrap();
    let mut left_items = Vec::new();
    let left_continuation = left
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            left_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    let mut right_items = Vec::new();
    let right_continuation = right
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            right_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(
        left_items,
        vec![
            (Vec::new(), b"left-empty".to_vec()),
            (b"key".to_vec(), b"left".to_vec()),
        ]
    );
    assert_eq!(left_continuation, None);
    assert_eq!(
        right_items,
        vec![
            (Vec::new(), b"right-empty".to_vec()),
            (b"key".to_vec(), b"right".to_vec()),
        ]
    );
    assert_eq!(right_continuation, None);
}
