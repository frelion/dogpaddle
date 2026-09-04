use dogpaddle_store::{
    Cell, Large, ScanDirection, ScanLimit, Small, Store, StoreError, TransactionAccess,
    Transactions,
};

use crate::support::{ByteMap, create_byte_map, open_byte_map, store_path};

fn write_pair(
    access: TransactionAccess<'_>,
    small: &ByteMap<Small>,
    large: &ByteMap<Large>,
) -> Result<(), StoreError> {
    small
        .access(access)?
        .put(&b"key".to_vec(), &b"small".to_vec())?;
    large
        .access(access)?
        .put(&b"key".to_vec(), &b"large".to_vec())
}

#[test]
fn setup_snapshot_reads_an_opened_cell_without_consuming_the_store() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);

    {
        let mut store = Store::create(&path).unwrap();
        let definition = store.create_data::<Cell<u64>>("definition").unwrap();
        let state = store.create_data::<Cell<u64>>("state").unwrap();
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin().unwrap();
        definition
            .access(transaction.access())
            .unwrap()
            .set(&41)
            .unwrap();
        state
            .access(transaction.access())
            .unwrap()
            .set(&42)
            .unwrap();
        transaction.commit().unwrap();
    }

    let store = Store::open(&path).unwrap();
    let definition = store.open_data::<Cell<u64>>("definition").unwrap();
    {
        let transaction = store.read_transaction().unwrap();
        assert_eq!(
            definition
                .read(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(41)
        );
    }

    let state = store.open_data::<Cell<u64>>("state").unwrap();
    let transaction = store.read_transaction().unwrap();
    assert_eq!(
        state.read(transaction.access()).unwrap().get().unwrap(),
        Some(42)
    );
}

#[test]
fn read_snapshot_coexists_with_the_unique_writer_and_remains_stable() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.into_transactions().split();

    {
        let transaction = writes.begin().unwrap();
        cell.access(transaction.access()).unwrap().set(&1).unwrap();
        transaction.commit().unwrap();
    }

    let old_snapshot = reads.begin().unwrap();
    {
        let transaction = writes.begin().unwrap();
        cell.access(transaction.access()).unwrap().set(&2).unwrap();
        transaction.commit().unwrap();
    }
    assert_eq!(
        cell.read(old_snapshot.access()).unwrap().get().unwrap(),
        Some(1)
    );
    drop(old_snapshot);

    let current_snapshot = reads.begin().unwrap();
    assert_eq!(
        cell.read(current_snapshot.access()).unwrap().get().unwrap(),
        Some(2)
    );
    drop(current_snapshot);
    drop(writes);

    let snapshot_without_writer = reads.begin().unwrap();
    assert_eq!(
        cell.read(snapshot_without_writer.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(2)
    );
}

#[test]
fn shared_read_capability_begins_snapshots_on_independent_threads() {
    use std::sync::Barrier;

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.into_transactions().split();

    {
        let transaction = writes.begin().unwrap();
        cell.access(transaction.access()).unwrap().set(&42).unwrap();
        transaction.commit().unwrap();
    }

    let barrier = Barrier::new(3);
    std::thread::scope(|scope| {
        let readers = (0..2)
            .map(|_| {
                let reads = &reads;
                let cell = &cell;
                let barrier = &barrier;
                scope.spawn(move || {
                    let transaction = reads.begin().unwrap();
                    barrier.wait();
                    cell.read(transaction.access()).unwrap().get().unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();

        for reader in readers {
            assert_eq!(reader.join().unwrap(), Some(42));
        }
    });
}

#[test]
fn wrong_store_poison_stops_a_read_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut first_store = Store::create(root.path().join("first")).unwrap();
    let first = first_store.create_data::<Cell<u64>>("cell").unwrap();
    let (_, first_reads) = first_store.into_transactions().split();

    let mut second_store = Store::create(root.path().join("second")).unwrap();
    let second = second_store.create_data::<Cell<u64>>("cell").unwrap();

    let transaction = first_reads.begin().unwrap();
    let access = transaction.access();
    assert!(matches!(second.read(access), Err(StoreError::WrongStore)));
    assert!(matches!(
        first.read(access),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn read_scan_visitor_error_poisons_the_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    let key = b"key".to_vec();

    {
        let transaction = writes.begin().unwrap();
        data.access(transaction.access())
            .unwrap()
            .put(&key, &b"value".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = reads.begin().unwrap();
    let access = data.read(transaction.access()).unwrap();
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1_024).unwrap(),
            |_| Err::<(), _>(StoreError::InvalidScanLimit),
        ),
        Err(StoreError::InvalidScanLimit)
    ));
    assert!(matches!(
        access.get(&key),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn read_decode_error_poisons_the_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = store.create_data::<Cell<Vec<u8>>>("cell").unwrap();
    let typed = store.open_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.into_transactions().split();

    {
        let transaction = writes.begin().unwrap();
        raw.access(transaction.access())
            .unwrap()
            .set(&vec![0])
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = reads.begin().unwrap();
    let access = transaction.access();
    assert!(matches!(
        typed.read(access).unwrap().get(),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        raw.read(access),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn commit_and_drop_are_atomic_across_small_and_large_data() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let small = create_byte_map::<Small>(&mut store, "small").unwrap();
    let large = create_byte_map::<Large>(&mut store, "large").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        write_pair(transaction.access(), &small, &large).unwrap();
        let small = small.access(transaction.access()).unwrap();
        let large = large.access(transaction.access()).unwrap();
        assert_eq!(
            small.get(&b"key".to_vec()).unwrap(),
            Some(b"small".to_vec())
        );
        assert_eq!(
            large.get(&b"key".to_vec()).unwrap(),
            Some(b"large".to_vec())
        );
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin().unwrap();
        small
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"dirty small".to_vec())
            .unwrap();
        large
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"dirty large".to_vec())
            .unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let small = open_byte_map::<Small>(&store, "small").unwrap();
    let large = open_byte_map::<Large>(&store, "large").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        small
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"small".to_vec())
    );
    assert_eq!(
        large
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"large".to_vec())
    );
}

#[test]
fn wrong_store_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut first_store = Store::create(root.path().join("first")).unwrap();
    let first = create_byte_map::<Small>(&mut first_store, "data").unwrap();
    let mut first_transactions = first_store.into_transactions();

    let mut second_store = Store::create(root.path().join("second")).unwrap();
    let second = create_byte_map::<Small>(&mut second_store, "data").unwrap();

    let transaction = first_transactions.begin().unwrap();
    let access = transaction.access();
    let mut first_access = first.access(access).unwrap();
    first_access
        .put(&b"key".to_vec(), &b"value".to_vec())
        .unwrap();
    assert!(matches!(second.access(access), Err(StoreError::WrongStore)));
    assert!(matches!(
        first_access.get(&b"key".to_vec()),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = first_transactions.begin().unwrap();
    assert_eq!(
        first
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn data_objects_from_a_previous_open_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let stale = create_byte_map::<Small>(&mut store, "data").unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let current = open_byte_map::<Small>(&store, "data").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert!(matches!(
        stale.access(transaction.access()),
        Err(StoreError::WrongStore)
    ));
    assert!(matches!(
        current.access(transaction.access()),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn scan_admission_errors_are_soft() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    data.access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"wide".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    let mut visited = false;
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
            |_| {
                visited = true;
                Ok::<(), StoreError>(())
            },
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    assert!(!visited);
    access
        .put(&b"second".to_vec(), &b"still writable".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"second".to_vec())
            .unwrap(),
        Some(b"still writable".to_vec())
    );
}

#[test]
fn unique_transaction_capability_can_move_to_another_thread() {
    fn require_send<T: Send>() {}
    require_send::<Transactions>();

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let transactions = store.into_transactions();

    let (mut transactions, data) = std::thread::spawn(move || {
        let mut transactions = transactions;
        let transaction = transactions.begin().unwrap();
        data.access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"value".to_vec())
            .unwrap();
        transaction.commit().unwrap();
        (transactions, data)
    })
    .join()
    .unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"value".to_vec())
    );
}
