use dogpaddle_store::{
    OrderedMap, PartitionKey, ScanDirection, ScanLimit, Store, StoreError, StoreSetup,
};
use std::num::NonZeroU64;

use crate::support::store_path;

#[test]
fn ordered_multiset_adjusts_exactly_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
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
        .open_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(values.multiplicity(&absent).unwrap(), 0);
    assert_eq!(values.multiplicity(&retained).unwrap(), 3);
    assert_eq!(values.multiplicity(&removed).unwrap(), 0);
}

#[test]
fn ordered_multiset_replaces_a_full_u64_weight_and_removes_zero() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
    let key = b"row".to_vec();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values.set_multiplicity(&key, u64::MAX).unwrap();
        assert_eq!(values.multiplicity(&key).unwrap(), u64::MAX);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let multiset = store
        .open_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        assert_eq!(values.multiplicity(&key).unwrap(), u64::MAX);
        values.set_multiplicity(&key, 0).unwrap();
        assert_eq!(values.multiplicity(&key).unwrap(), 0);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        multiset
            .read(transaction.access())
            .unwrap()
            .multiplicity(&key)
            .unwrap(),
        0
    );
}

#[test]
fn invalid_multiplicity_adjustments_poison_and_roll_back_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<Vec<u8>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();
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
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
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
                partition
                    .first_bounded(usize::MAX)
                    .unwrap()
                    .map(|entry| entry.0),
                Some(keys[0].clone())
            );
            assert_eq!(
                partition
                    .last_bounded(usize::MAX)
                    .unwrap()
                    .map(|entry| entry.0),
                Some(keys[5].clone())
            );
            assert_eq!(
                partition
                    .scan(ScanDirection::Ascending, None, two)
                    .unwrap()
                    .entries,
                vec![
                    (keys[0].clone(), NonZeroU64::new(1).unwrap()),
                    (keys[1].clone(), NonZeroU64::new(2).unwrap()),
                ]
            );
            assert_eq!(
                partition
                    .scan(ScanDirection::Descending, None, two)
                    .unwrap()
                    .entries,
                vec![
                    (keys[5].clone(), NonZeroU64::new(6).unwrap()),
                    (keys[4].clone(), NonZeroU64::new(5).unwrap()),
                ]
            );
        }
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
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
fn partitioned_multiset_replaces_weight_without_changing_other_partitions() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
    let first = b"first".to_vec();
    let second = b"second".to_vec();
    let key = b"key".to_vec();

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values
            .partition(&first)
            .unwrap()
            .set_multiplicity(&key, u64::MAX)
            .unwrap();
        values
            .partition(&second)
            .unwrap()
            .set_multiplicity(&key, 7)
            .unwrap();
        assert_eq!(
            values
                .partition(&first)
                .unwrap()
                .multiplicity(&key)
                .unwrap(),
            u64::MAX
        );
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        values
            .partition(&first)
            .unwrap()
            .set_multiplicity(&key, 0)
            .unwrap();
        assert_eq!(
            values
                .partition(&first)
                .unwrap()
                .multiplicity(&key)
                .unwrap(),
            0
        );
        assert_eq!(
            values
                .partition(&second)
                .unwrap()
                .multiplicity(&key)
                .unwrap(),
            7
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(
        values
            .partition(&first)
            .unwrap()
            .multiplicity(&key)
            .unwrap(),
        0
    );
    assert_eq!(
        values
            .partition(&second)
            .unwrap()
            .multiplicity(&key)
            .unwrap(),
        7
    );
}

#[test]
fn empty_partition_has_no_bounds_or_scan_entries_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let empty_partition = Vec::new();
    let (_, reads) = store.commit(&path, |_| Ok(())).unwrap().split();

    {
        let transaction = reads.begin();
        let values = multiset.read(transaction.access()).unwrap();
        let empty = values.partition(&empty_partition).unwrap();
        assert_eq!(empty.first_bounded(usize::MAX).unwrap(), None);
        assert_eq!(empty.last_bounded(usize::MAX).unwrap(), None);
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
    drop(reads);

    let store = Store::open(path).unwrap();
    let multiset = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let transaction = store.read_transaction();
    let values = multiset.read(transaction.access()).unwrap();
    assert_eq!(
        values
            .partition(&empty_partition)
            .unwrap()
            .first_bounded(usize::MAX)
            .unwrap(),
        None
    );
}

#[test]
fn partitioned_multiset_pages_resume_in_both_directions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();
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
            actual.extend(page.entries.into_iter().map(|entry| entry.0));
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
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();
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
    assert_eq!(page.entries[0].0, key);
    assert_eq!(page.continuation, None);
    transaction.commit().unwrap();
}

#[test]
fn wide_partition_scan_charges_framed_keys_and_resumes_on_owned_row_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();
    let partition_key = vec![0x11; 256];
    let adjacent_partition = vec![0x12; 256];
    let keys = [vec![0x31; 8 * 1024], vec![0x32; 8 * 1024]];
    let item_bytes = 2
        + partition_key.len()
        + partition_key
            .iter()
            .fold(0, |count, byte| count + usize::from(*byte == 0))
        + keys[0].len()
        + 8;

    {
        let transaction = transactions.begin();
        let mut values = multiset.access(transaction.access()).unwrap();
        let mut partition = values.partition(&partition_key).unwrap();
        partition.adjust(&keys[0], 2).unwrap();
        partition.adjust(&keys[1], 3).unwrap();
        values
            .partition(&adjacent_partition)
            .unwrap()
            .adjust(&vec![0xff; 9 * 1024], 4)
            .unwrap();
        transaction.commit().unwrap();
    }

    let (_, reads) = transactions.split();
    let snapshot = reads.begin();
    let values = multiset.read(snapshot.access()).unwrap();
    let partition = values.partition(&partition_key).unwrap();
    assert!(matches!(
        partition.scan(
            ScanDirection::Ascending,
            None,
            ScanLimit::new(2, item_bytes - 1).unwrap(),
        ),
        Err(StoreError::ItemTooLarge { size, limit })
            if size == item_bytes && limit == item_bytes - 1
    ));
    for direction in [ScanDirection::Ascending, ScanDirection::Descending] {
        let (first, second) = if direction == ScanDirection::Ascending {
            (0, 1)
        } else {
            (1, 0)
        };
        let page = partition
            .scan(direction, None, ScanLimit::new(2, item_bytes).unwrap())
            .unwrap();
        assert_eq!(
            page.entries,
            vec![(
                keys[first].clone(),
                NonZeroU64::new(first as u64 + 2).unwrap()
            )]
        );
        assert_eq!(page.continuation, Some(keys[first].clone()));
        let page = partition
            .scan(
                direction,
                page.continuation.as_ref(),
                ScanLimit::new(2, item_bytes).unwrap(),
            )
            .unwrap();
        assert_eq!(
            page.entries,
            vec![(
                keys[second].clone(),
                NonZeroU64::new(second as u64 + 2).unwrap()
            )]
        );
        assert_eq!(page.continuation, None);
    }
}

#[test]
fn partitioned_multiset_isolates_framed_partition_keys_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let multiset = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
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
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
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
fn weights_and_partitions_use_the_ordered_map_catalog_kind() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    store
        .create_data::<OrderedMap<Vec<u8>, NonZeroU64>>("ordered")
        .unwrap();
    store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("partitioned")
        .unwrap();
    drop(store.commit(&path, |_| Ok(())).unwrap());
    let store = Store::open(path).unwrap();
    assert!(
        store
            .open_data::<OrderedMap<Vec<u8>, NonZeroU64>>("ordered")
            .is_ok()
    );
    assert!(
        store
            .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("partitioned")
            .is_ok()
    );
}

#[test]
fn bounded_partition_endpoints_admit_framed_bytes_and_retry_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let values = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
    {
        let transaction = transactions.begin();
        let mut access = values.access(transaction.access()).unwrap();
        let mut partition = access.partition(&vec![1]).unwrap();
        partition.adjust(&vec![3; 64], 2).unwrap();
        partition.adjust(&vec![4; 64], 1).unwrap();
        // One-byte partition + two-byte terminator + key + u64 weight.
        assert!(matches!(
            partition.first_bounded(74),
            Err(StoreError::ItemTooLarge {
                size: 75,
                limit: 74
            })
        ));
        assert_eq!(partition.first_bounded(75).unwrap().unwrap().0, vec![3; 64]);
        assert_eq!(partition.last_bounded(75).unwrap().unwrap().0, vec![4; 64]);
        transaction.commit().unwrap();
    }
    drop(transactions);
    let store = Store::open(&path).unwrap();
    let values = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let snapshot = store.read_transaction();
    let access = values.read(snapshot.access()).unwrap();
    let partition = access.partition(&vec![1]).unwrap();
    assert!(matches!(
        partition.last_bounded(74),
        Err(StoreError::ItemTooLarge { .. })
    ));
    assert_eq!(partition.last_bounded(75).unwrap().unwrap().1.get(), 1);
    assert!(matches!(
        partition.first_bounded(0),
        Err(StoreError::InvalidScanLimit)
    ));
    assert_eq!(partition.first_bounded(75).unwrap().unwrap().1.get(), 2);
}

#[test]
fn malformed_weight_poisoning_rolls_back_prior_writes_without_copying_oversized_value() {
    for bytes in [vec![0; 8], vec![1; 9], vec![1; 64 * 1024]] {
        let root = tempfile::tempdir().unwrap();
        let path = store_path(&root);
        let mut store = StoreSetup::new();
        let raw = store
            .create_data::<OrderedMap<Vec<u8>, Vec<u8>>>("weights")
            .unwrap();
        let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
        let transaction = transactions.begin();
        raw.access(transaction.access())
            .unwrap()
            .put(&vec![1], &bytes)
            .unwrap();
        transaction.commit().unwrap();
        drop(transactions);
        let store = Store::open(&path).unwrap();
        let weights = store
            .open_data::<OrderedMap<Vec<u8>, NonZeroU64>>("weights")
            .unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        let mut access = weights.access(transaction.access()).unwrap();
        access.adjust(&vec![2], 1).unwrap();
        assert!(matches!(
            access.multiplicity(&vec![1]),
            Err(StoreError::Codec(_))
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
        let transaction = transactions.begin();
        assert_eq!(
            weights
                .access(transaction.access())
                .unwrap()
                .multiplicity(&vec![2])
                .unwrap(),
            0
        );
        transaction.commit().unwrap();
    }
}

#[test]
fn partition_view_reads_and_writes_generic_values_using_the_same_map() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let map = store
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, u64>, String>>("map")
        .unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();
    let transaction = transactions.begin();
    let mut access = map.access(transaction.access()).unwrap();
    access
        .partition(&vec![0])
        .unwrap()
        .put(&1, &"first".to_owned())
        .unwrap();
    access
        .partition(&vec![0, 0])
        .unwrap()
        .put(&1, &"other".to_owned())
        .unwrap();
    let mut partition = access.partition(&vec![0]).unwrap();
    assert!(matches!(
        partition.get_bounded(&1, 4),
        Err(StoreError::ItemTooLarge { size: 5, limit: 4 })
    ));
    assert_eq!(
        partition.first_bounded(32).unwrap(),
        Some((1, "first".to_owned()))
    );
    partition.erase(&1).unwrap();
    assert_eq!(partition.get(&1).unwrap(), None);
    assert_eq!(
        access.get(&PartitionKey(vec![0, 0], 1)).unwrap(),
        Some("other".to_owned())
    );
    transaction.commit().unwrap();
}

#[test]
fn partition_presence_observes_transaction_writes_and_stable_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut setup = StoreSetup::new();
    let values = setup
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let (mut writes, reads) = setup.commit(&path, |_| Ok(())).unwrap().split();
    let old = reads.begin();
    let empty = Vec::new();
    let zero = vec![0];
    let neighbor = vec![0, 0];
    {
        let transaction = writes.begin();
        let mut access = values.access(transaction.access()).unwrap();
        access
            .partition(&empty)
            .unwrap()
            .set_multiplicity(&empty, 1)
            .unwrap();
        access
            .partition(&neighbor)
            .unwrap()
            .set_multiplicity(&empty, 1)
            .unwrap();
        let mut partition = access.partition(&zero).unwrap();
        assert!(partition.is_empty().unwrap());
        assert!(!partition.has_other_key(&empty).unwrap());
        partition.set_multiplicity(&empty, 1).unwrap();
        assert!(!partition.is_empty().unwrap());
        assert!(!partition.has_other_key(&empty).unwrap());
        assert!(partition.has_other_key(&zero).unwrap());
        partition.set_multiplicity(&zero, 2).unwrap();
        assert!(partition.has_other_key(&empty).unwrap());
        assert!(partition.has_other_key(&zero).unwrap());
        partition.set_multiplicity(&empty, 0).unwrap();
        assert!(!partition.has_other_key(&zero).unwrap());
        transaction.commit().unwrap();
    }
    let old_access = values.read(old.access()).unwrap();
    assert!(old_access.partition(&zero).unwrap().is_empty().unwrap());
    assert!(
        !old_access
            .partition(&zero)
            .unwrap()
            .has_other_key(&empty)
            .unwrap()
    );
    drop(old);
    {
        let current = reads.begin();
        let access = values.read(current.access()).unwrap();
        let partition = access.partition(&zero).unwrap();
        assert!(!partition.is_empty().unwrap());
        assert!(!partition.has_other_key(&zero).unwrap());
        assert!(partition.has_other_key(&empty).unwrap());
    }
    {
        let transaction = writes.begin();
        let mut access = values.access(transaction.access()).unwrap();
        let mut partition = access.partition(&zero).unwrap();
        partition.set_multiplicity(&zero, 0).unwrap();
        assert!(partition.is_empty().unwrap());
        // Roll back this deletion; the adjacent partition must never count.
    }
    drop((writes, reads));
    let store = Store::open(&path).unwrap();
    let values = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let snapshot = store.read_transaction();
    let access = values.read(snapshot.access()).unwrap();
    assert!(!access.partition(&zero).unwrap().is_empty().unwrap());
    assert!(
        !access
            .partition(&zero)
            .unwrap()
            .has_other_key(&zero)
            .unwrap()
    );
    assert!(access.partition(&vec![0, 1]).unwrap().is_empty().unwrap());
}

#[test]
fn partition_presence_does_not_decode_neighbor_values_and_preserves_poison() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut setup = StoreSetup::new();
    let raw = setup
        .create_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, Vec<u8>>>("values")
        .unwrap();
    let partition_key = vec![0];
    let local_key = Vec::new();
    let transactions = setup
        .commit(&path, |access| {
            raw.access(access)?
                .partition(&partition_key)?
                .put(&local_key, &vec![0; 8])
        })
        .unwrap();
    drop(transactions);
    let store = Store::open(&path).unwrap();
    let values = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>("values")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut access = values.access(transaction.access()).unwrap();
    let partition = access.partition(&partition_key).unwrap();
    assert!(!partition.is_empty().unwrap());
    assert!(!partition.has_other_key(&local_key).unwrap());
    assert!(partition.has_other_key(&vec![1]).unwrap());
    assert!(matches!(
        partition.multiplicity(&local_key),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        partition.is_empty(),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        partition.has_other_key(&local_key),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
