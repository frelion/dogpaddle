use dogpaddle_store::{Large, ScanDirection, ScanLimit, Small, Store, StoreData, StoreError};

use crate::support::{ByteMap, create_byte_map, store_path};

#[test]
fn byte_maps_support_get_put_replace_and_delete() {
    assert_byte_map_operations::<Small>();
    assert_byte_map_operations::<Large>();
}

fn assert_byte_map_operations<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<SIZE>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let key = b"key".to_vec();
    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    assert_eq!(access.get(&key).unwrap(), None);
    access.put(&key, &b"first".to_vec()).unwrap();
    assert_eq!(access.get(&key).unwrap(), Some(b"first".to_vec()));
    access.put(&key, &b"second".to_vec()).unwrap();
    assert_eq!(access.get(&key).unwrap(), Some(b"second".to_vec()));
    assert!(access.remove(&key).unwrap());
    assert!(!access.remove(&key).unwrap());
    assert_eq!(access.get(&key).unwrap(), None);
    transaction.commit().unwrap();
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

#[test]
fn byte_map_writes_are_visible_inside_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    access.put(&b"a".to_vec(), &b"one".to_vec()).unwrap();
    access.put(&b"b".to_vec(), &b"two".to_vec()).unwrap();
    assert_eq!(access.get(&b"a".to_vec()).unwrap(), Some(b"one".to_vec()));
    let mut items = Vec::new();
    let continuation = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
            |entry| {
                items.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(
        items,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    );
    assert_eq!(continuation, None);
}

#[test]
fn byte_map_binary_keys_page_in_both_directions() {
    assert_binary_key_pages::<Small>();
    assert_binary_key_pages::<Large>();
}

fn assert_binary_key_pages<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
    let keys = [
        Vec::new(),
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![0x7f; 59],
        vec![0x7f; 60],
        vec![0xff],
        vec![0xff, 0],
        vec![0xff; 128],
    ];

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<SIZE>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = data.access(transaction.access()).unwrap();
        for key in &keys {
            access.put(key, key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = data.access(transaction.access()).unwrap();
    for key in &keys {
        assert_eq!(access.get(key).unwrap(), Some(key.clone()));
    }
    for direction in [ScanDirection::Ascending, ScanDirection::Descending] {
        let mut expected = keys
            .iter()
            .map(|key| (key.clone(), key.clone()))
            .collect::<Vec<_>>();
        if direction == ScanDirection::Descending {
            expected.reverse();
        }

        let mut actual = Vec::new();
        let mut continuation = None;
        loop {
            let mut page = Vec::new();
            let next = access
                .scan(
                    ..,
                    direction,
                    continuation.as_ref(),
                    ScanLimit::new(1, 1_024).unwrap(),
                    |entry| {
                        page.push(entry.decode_owned()?);
                        Ok::<(), StoreError>(())
                    },
                )
                .unwrap();
            assert!(page.len() <= 1);
            assert_eq!(page, expected[actual.len()..actual.len() + page.len()]);
            let has_more = actual.len() + page.len() < expected.len();
            assert_eq!(next.is_some(), has_more);
            actual.extend(page);
            if let Some(next) = next {
                assert!(!actual.is_empty());
                continuation = Some(next);
            } else {
                break;
            }
        }
        assert_eq!(actual, expected);
    }
}

#[test]
fn scan_limits_must_be_nonzero() {
    assert!(matches!(
        ScanLimit::new(0, 1),
        Err(StoreError::InvalidScanLimit)
    ));
    assert!(matches!(
        ScanLimit::new(1, 0),
        Err(StoreError::InvalidScanLimit)
    ));
    let limit = ScanLimit::new(3, 7).unwrap();
    assert_eq!(limit.max_items(), 3);
    assert_eq!(limit.max_bytes(), 7);
}
