use dogpaddle_store::{
    MultisetEntry, OrderedMultiset, PartitionedMultiset, ScanDirection, ScanLimit, Store,
    StoreError,
};

use crate::support::store_path;

#[test]
fn ordered_multiset_adjusts_exactly_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let multiset = store
        .create_data::<OrderedMultiset<Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let absent = b"absent".to_vec();
    let retained = b"retained".to_vec();
    let removed = b"removed".to_vec();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();

        assert_eq!(values.multiplicity(&absent).unwrap(), 0);
        let change = values.adjust(&absent, 0).unwrap();
        assert_eq!((change.before(), change.after()), (0, 0));
        assert_eq!(values.multiplicity(&absent).unwrap(), 0);

        let change = values.adjust(&retained, 3).unwrap();
        assert_eq!((change.before(), change.after()), (0, 3));
        assert_eq!(values.multiplicity(&retained).unwrap(), 3);

        values.adjust(&removed, 2).unwrap();
        let change = values.adjust(&removed, -2).unwrap();
        assert_eq!((change.before(), change.after()), (2, 0));
        assert_eq!(values.multiplicity(&removed).unwrap(), 0);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<OrderedMultiset<Vec<u8>>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(values.multiplicity(&absent).unwrap(), 0);
    assert_eq!(values.multiplicity(&retained).unwrap(), 3);
    assert_eq!(values.multiplicity(&removed).unwrap(), 0);
}

#[test]
fn invalid_multiplicity_adjustments_poison_and_roll_back_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let multiset = store
        .create_data::<OrderedMultiset<Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let maximum = b"maximum".to_vec();
    let prior = b"prior".to_vec();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values.adjust(&maximum, i64::MAX).unwrap();
        values.adjust(&maximum, i64::MAX).unwrap();
        values.adjust(&maximum, 1).unwrap();
        assert_eq!(values.multiplicity(&maximum).unwrap(), u64::MAX);
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values.adjust(&prior, 1).unwrap();
        assert!(matches!(
            values.adjust(&b"missing".to_vec(), -1),
            Err(StoreError::MultiplicityUnderflow)
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
    {
        let transaction = transactions.begin();
        let values = multiset.access(transaction.access()).unwrap();
        assert_eq!(values.multiplicity(&prior).unwrap(), 0);
        assert_eq!(values.multiplicity(&maximum).unwrap(), u64::MAX);
    }

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values.adjust(&prior, 1).unwrap();
        assert!(matches!(
            values.adjust(&maximum, 1),
            Err(StoreError::MultiplicityOverflow)
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
    let transaction = transactions.begin();
    let values = multiset.access(transaction.access()).unwrap();
    assert_eq!(values.multiplicity(&prior).unwrap(), 0);
    assert_eq!(values.multiplicity(&maximum).unwrap(), u64::MAX);
}

#[test]
fn partitioned_multiset_orders_binary_keys_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let multiset = store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let partition_key = b"a".to_vec();
    let keys = [
        Vec::new(),
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![0xff],
        vec![0xff, 0],
    ];
    let two = ScanLimit::new(2, usize::MAX).unwrap();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        {
            let mut partition = values.partition(&partition_key).unwrap();
            for (key, difference) in keys.iter().zip(1_i64..) {
                partition.adjust(key, difference).unwrap();
            }
            assert_eq!(partition.multiplicity(&keys[3]).unwrap(), 4);
            assert_eq!(
                partition.first().unwrap().map(|entry| entry.key),
                Some(keys[0].clone())
            );
            assert_eq!(
                partition.last().unwrap().map(|entry| entry.key),
                Some(keys[5].clone())
            );
            assert_eq!(
                partition
                    .scan(ScanDirection::Ascending, None, two)
                    .unwrap()
                    .entries,
                vec![
                    MultisetEntry {
                        key: keys[0].clone(),
                        multiplicity: 1,
                    },
                    MultisetEntry {
                        key: keys[1].clone(),
                        multiplicity: 2,
                    },
                ]
            );
            assert_eq!(
                partition
                    .scan(ScanDirection::Descending, None, two)
                    .unwrap()
                    .entries,
                vec![
                    MultisetEntry {
                        key: keys[5].clone(),
                        multiplicity: 6,
                    },
                    MultisetEntry {
                        key: keys[4].clone(),
                        multiplicity: 5,
                    },
                ]
            );
        }
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(
        values
            .partition(&partition_key)
            .unwrap()
            .multiplicity(&keys[3])
            .unwrap(),
        4
    );
}

#[test]
fn empty_partition_has_no_bounds_or_scan_entries_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let multiset = store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let empty_partition = Vec::new();

    {
        let transaction = store.read_transaction();
        let values = multiset.read(transaction.access()).unwrap();
        let empty = values.partition(&empty_partition).unwrap();
        assert_eq!(empty.first().unwrap(), None);
        assert_eq!(empty.last().unwrap(), None);
        assert!(
            empty
                .scan(
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(1, 1).unwrap(),
                )
                .unwrap()
                .entries
                .is_empty()
        );
    }
    drop(store);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(
        values.partition(&empty_partition).unwrap().first().unwrap(),
        None
    );
}

#[test]
fn partitioned_multiset_pages_resume_in_both_directions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let multiset = store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let partition_key = b"partition".to_vec();
    let keys = (0_u8..5).map(|key| vec![key]).collect::<Vec<_>>();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        let mut partition = values.partition(&partition_key).unwrap();
        for key in &keys {
            partition.adjust(key, i64::from(key[0]) + 1).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
    let mut values = multiset.access(transaction.access()).unwrap();
    let partition = values.partition(&partition_key).unwrap();
    let limit = ScanLimit::new(2, usize::MAX).unwrap();
    for direction in [ScanDirection::Ascending, ScanDirection::Descending] {
        let mut expected = keys.clone();
        if direction == ScanDirection::Descending {
            expected.reverse();
        }
        let mut actual = Vec::new();
        let mut continuation = None;
        loop {
            let page = partition
                .scan(direction, continuation.as_ref(), limit)
                .unwrap();
            actual.extend(page.entries.into_iter().map(|entry| entry.key));
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert_eq!(actual, expected);
    }
}

#[test]
fn partitioned_multiset_byte_limit_can_be_retried() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let multiset = store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let partition_key = b"partition".to_vec();
    let key = vec![7; 128];

    let transaction = transactions.begin();
    let mut values = multiset.access(transaction.access()).unwrap();
    let mut partition = values.partition(&partition_key).unwrap();
    partition.adjust(&key, 1).unwrap();
    let size = match partition.scan(
        ScanDirection::Ascending,
        None,
        ScanLimit::new(1, 1).unwrap(),
    ) {
        Err(StoreError::ItemTooLarge { size, limit: 1 }) => size,
        result => panic!("unexpected scan result: {result:?}"),
    };
    let page = partition
        .scan(
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, size).unwrap(),
        )
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, key);
    assert_eq!(page.continuation, None);
    transaction.commit().unwrap();
}

#[test]
fn partitioned_multiset_isolates_framed_partition_keys_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let multiset = store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let first_partition = b"a".to_vec();
    let adjacent_partition = b"a\0".to_vec();
    let prefix_partition = b"ab".to_vec();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values
            .partition(&first_partition)
            .unwrap()
            .adjust(&b"bc".to_vec(), 5)
            .unwrap();
        values
            .partition(&adjacent_partition)
            .unwrap()
            .adjust(&b"bc".to_vec(), 7)
            .unwrap();
        values
            .partition(&prefix_partition)
            .unwrap()
            .adjust(&b"c".to_vec(), 9)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(
        values
            .partition(&first_partition)
            .unwrap()
            .multiplicity(&b"bc".to_vec())
            .unwrap(),
        5
    );
    assert_eq!(
        values
            .partition(&adjacent_partition)
            .unwrap()
            .multiplicity(&b"bc".to_vec())
            .unwrap(),
        7
    );
    assert_eq!(
        values
            .partition(&prefix_partition)
            .unwrap()
            .multiplicity(&b"c".to_vec())
            .unwrap(),
        9
    );
}

#[test]
fn multiset_collection_kinds_are_distinct() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    store
        .create_data::<OrderedMultiset<Vec<u8>>>("ordered")
        .unwrap();
    store
        .create_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("partitioned")
        .unwrap();
    drop(store);

    let store = Store::open(path).unwrap();
    assert!(matches!(
        store.open_data::<PartitionedMultiset<Vec<u8>, Vec<u8>>>("ordered"),
        Err(StoreError::DataKindMismatch {
            name,
            expected: "partitioned multiset",
            actual: "ordered multiset",
        }) if name == "ordered"
    ));
    assert!(matches!(
        store.open_data::<OrderedMultiset<Vec<u8>>>("partitioned"),
        Err(StoreError::DataKindMismatch {
            name,
            expected: "ordered multiset",
            actual: "partitioned multiset",
        }) if name == "partitioned"
    ));
}
