use dogpaddle_store::{DataPlacement, ScanDirection, ScanLimit, Store, StoreError};

use crate::support::{PLACEMENTS, store_path};

#[test]
fn raw_data_supports_get_put_replace_and_delete() {
    for placement in PLACEMENTS {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let data = store.create_data("data", placement).unwrap();
        assert_eq!(data.name(), "data");
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin().unwrap();
        let mut access = data.access(&transaction).unwrap();
        assert_eq!(access.get(b"key").unwrap(), None);
        access.put(b"key", b"first").unwrap();
        assert_eq!(access.get(b"key").unwrap(), Some(b"first".to_vec()));
        access.put(b"key", b"second").unwrap();
        assert_eq!(access.get(b"key").unwrap(), Some(b"second".to_vec()));
        assert!(access.delete(b"key").unwrap());
        assert!(!access.delete(b"key").unwrap());
        assert_eq!(access.get(b"key").unwrap(), None);
        transaction.commit().unwrap();
    }
}

#[test]
fn namespaces_isolate_identical_keys() {
    for placement in PLACEMENTS {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let left = store.create_data("left", placement).unwrap();
        let right = store.create_data("right", placement).unwrap();
        let mut transactions = store.into_transactions();

        {
            let transaction = transactions.begin().unwrap();
            let mut left = left.access(&transaction).unwrap();
            let mut right = right.access(&transaction).unwrap();
            left.put(b"", b"left-empty").unwrap();
            left.put(b"key", b"left").unwrap();
            right.put(b"", b"right-empty").unwrap();
            right.put(b"key", b"right").unwrap();
            transaction.commit().unwrap();
        }

        let transaction = transactions.begin().unwrap();
        let left = left.access(&transaction).unwrap();
        let right = right.access(&transaction).unwrap();
        assert_eq!(left.get(b"").unwrap(), Some(b"left-empty".to_vec()));
        assert_eq!(left.get(b"key").unwrap(), Some(b"left".to_vec()));
        assert_eq!(right.get(b"").unwrap(), Some(b"right-empty".to_vec()));
        assert_eq!(right.get(b"key").unwrap(), Some(b"right".to_vec()));

        let limit = ScanLimit::new(10, 1_024).unwrap();
        let left = left
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap();
        let right = right
            .scan(.., ScanDirection::Ascending, None, limit)
            .unwrap();
        assert_eq!(
            left.items,
            vec![
                (Vec::new(), b"left-empty".to_vec()),
                (b"key".to_vec(), b"left".to_vec()),
            ]
        );
        assert_eq!(left.continuation, None);
        assert_eq!(
            right.items,
            vec![
                (Vec::new(), b"right-empty".to_vec()),
                (b"key".to_vec(), b"right".to_vec()),
            ]
        );
        assert_eq!(right.continuation, None);
    }
}

#[test]
fn raw_writes_are_visible_inside_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = store.create_data("data", DataPlacement::Shared).unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(&transaction).unwrap();
    access.put(b"a", b"one").unwrap();
    access.put(b"b", b"two").unwrap();
    assert_eq!(access.get(b"a").unwrap(), Some(b"one".to_vec()));
    let batch = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
        )
        .unwrap();
    assert_eq!(
        batch.items,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    );
}

#[test]
fn raw_binary_keys_page_in_both_directions() {
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

    for placement in PLACEMENTS {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let data = store.create_data("data", placement).unwrap();
        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin().unwrap();
            let mut access = data.access(&transaction).unwrap();
            for key in &keys {
                access.put(key, key).unwrap();
            }
            transaction.commit().unwrap();
        }

        let transaction = transactions.begin().unwrap();
        let access = data.access(&transaction).unwrap();
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
                let batch = access
                    .scan(
                        ..,
                        direction,
                        continuation.as_deref(),
                        ScanLimit::new(1, 1_024).unwrap(),
                    )
                    .unwrap();
                assert!(batch.items.len() <= 1);
                assert_eq!(
                    batch.items,
                    expected[actual.len()..actual.len() + batch.items.len()]
                );
                let has_more = actual.len() + batch.items.len() < expected.len();
                assert_eq!(batch.continuation.is_some(), has_more);
                actual.extend(batch.items);
                if let Some(next) = batch.continuation {
                    assert!(!actual.is_empty());
                    continuation = Some(next);
                } else {
                    break;
                }
            }
            assert_eq!(actual, expected);
        }
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
