use crate::support::{create_byte_map, create_map, store_path};
use dogpaddle_store::{ScanDirection, ScanLimit, Store, StoreError};

#[test]
fn owned_page_survives_writes_commit_and_store_close() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, Vec<u8>>(&mut store, "map").unwrap();
    let mut writes = store.into_transactions();
    let page = {
        let transaction = writes.begin();
        let mut access = map.access(transaction.access()).unwrap();
        for key in 1..=3 {
            access
                .put(&key, &vec![u8::try_from(key).unwrap(); 8192])
                .unwrap();
        }
        let page = access
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(3, 32_768).unwrap(),
            )
            .unwrap();
        access.remove(&2).unwrap();
        access.put(&3, &vec![9]).unwrap();
        transaction.commit().unwrap();
        page
    };
    drop(writes);
    assert_eq!(
        page.entries,
        (1..=3)
            .map(|key| (key, vec![u8::try_from(key).unwrap(); 8192]))
            .collect::<Vec<_>>()
    );
    assert_eq!(page.continuation, None);
}

#[test]
fn later_pages_observe_source_updates_after_an_owned_page() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, u64>(&mut store, "map").unwrap();
    let mut writes = store.into_transactions();
    let transaction = writes.begin();
    let mut access = map.access(transaction.access()).unwrap();
    for key in 1..=3 {
        access.put(&key, &key).unwrap();
    }
    let limit = ScanLimit::new(1, 1024).unwrap();
    let first = access
        .scan(.., ScanDirection::Ascending, None, limit)
        .unwrap();
    assert_eq!(first.entries, vec![(1, 1)]);
    assert_eq!(first.continuation, Some(1));
    access.remove(&2).unwrap();
    access.put(&4, &4).unwrap();
    let mut continuation = first.continuation;
    let mut remaining = Vec::new();
    loop {
        let page = access
            .scan(.., ScanDirection::Ascending, continuation.as_ref(), limit)
            .unwrap();
        remaining.extend(page.entries);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(remaining, vec![(3, 3), (4, 4)]);
    transaction.commit().unwrap();
}

#[test]
fn owned_read_page_survives_snapshot_and_store_close() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, String>(&mut store, "map").unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    let transaction = writes.begin();
    map.access(transaction.access())
        .unwrap()
        .put(&1, &"owned".to_owned())
        .unwrap();
    transaction.commit().unwrap();
    let page = {
        let snapshot = reads.begin();
        map.read(snapshot.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(1, 1024).unwrap(),
            )
            .unwrap()
    };
    drop((reads, writes));
    assert_eq!(page.entries, vec![(1, "owned".to_owned())]);
}

#[test]
fn byte_map_binary_keys_page_in_both_directions() {
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
    let data = create_byte_map(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        let mut access = data.access(transaction.access()).unwrap();
        for key in &keys {
            access.put(key, key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
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
            let result = access
                .scan(
                    ..,
                    direction,
                    continuation.as_ref(),
                    ScanLimit::new(1, 1_024).unwrap(),
                )
                .unwrap();
            let page = result.entries;
            let next = result.continuation;
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
