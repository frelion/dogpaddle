use dogpaddle_store::{
    Cell, ScanDirection, ScanLimit, Store, StoreError, StoreSetup, TransactionAccess, Transactions,
};

use crate::support::{ByteMap, create_byte_map, open_byte_map, store_path};

fn write_pair(
    access: TransactionAccess<'_>,
    first: &ByteMap,
    second: &ByteMap,
) -> Result<(), StoreError> {
    first
        .access(access)?
        .put(&b"key".to_vec(), &b"first".to_vec())?;
    second
        .access(access)?
        .put(&b"key".to_vec(), &b"second".to_vec())
}

#[test]
fn setup_snapshot_reads_an_opened_cell_without_consuming_the_store() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);

    {
        let mut store = StoreSetup::new();
        let definition = store.create_data::<Cell<u64>>("definition").unwrap();
        let state = store.create_data::<Cell<u64>>("state").unwrap();
        let mut transactions = store.commit(&path, |_| Ok(())).unwrap();
        let transaction = transactions.begin();
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
        let transaction = store.read_transaction();
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
    let transaction = store.read_transaction();
    assert_eq!(
        state.read(transaction.access()).unwrap().get().unwrap(),
        Some(42)
    );
}

#[test]
fn read_snapshot_coexists_with_the_unique_writer_and_remains_stable() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.commit(store_path(&root), |_| Ok(())).unwrap().split();

    {
        let transaction = writes.begin();
        cell.access(transaction.access()).unwrap().set(&1).unwrap();
        transaction.commit().unwrap();
    }

    let old_snapshot = reads.begin();
    {
        let transaction = writes.begin();
        cell.access(transaction.access()).unwrap().set(&2).unwrap();
        transaction.commit().unwrap();
    }
    assert_eq!(
        cell.read(old_snapshot.access()).unwrap().get().unwrap(),
        Some(1)
    );
    drop(old_snapshot);

    let current_snapshot = reads.begin();
    assert_eq!(
        cell.read(current_snapshot.access()).unwrap().get().unwrap(),
        Some(2)
    );
    drop(current_snapshot);
    drop(writes);

    let snapshot_without_writer = reads.begin();
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
    let mut store = StoreSetup::new();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.commit(store_path(&root), |_| Ok(())).unwrap().split();

    {
        let transaction = writes.begin();
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
                    let transaction = reads.begin();
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
    let mut first_store = StoreSetup::new();
    let first = first_store.create_data::<Cell<u64>>("cell").unwrap();
    let (_, first_reads) = first_store
        .commit(root.path().join("first"), |_| Ok(()))
        .unwrap()
        .split();

    let mut second_store = StoreSetup::new();
    let second = second_store.create_data::<Cell<u64>>("cell").unwrap();
    let _second_transactions = second_store
        .commit(root.path().join("second"), |_| Ok(()))
        .unwrap();

    let transaction = first_reads.begin();
    let access = transaction.access();
    assert!(matches!(second.read(access), Err(StoreError::WrongStore)));
    assert!(matches!(
        first.read(access),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn read_scan_decode_error_poisons_the_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    create_byte_map(&mut store, "data").unwrap();

    drop(store.commit(store_path(&root), |_| Ok(())).unwrap());
    let store = Store::open(store_path(&root)).unwrap();
    let raw = store
        .open_data::<dogpaddle_store::OrderedMap<Vec<u8>, Vec<u8>>>("data")
        .unwrap();
    let typed = store
        .open_data::<dogpaddle_store::OrderedMap<Vec<u8>, u64>>("data")
        .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    let transaction = writes.begin();
    raw.access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &vec![1])
        .unwrap();
    transaction.commit().unwrap();
    let snapshot = reads.begin();
    let access = typed.read(snapshot.access()).unwrap();
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1024).unwrap()
        ),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        access.get(&b"key".to_vec()),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn read_decode_error_poisons_the_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    store.create_data::<Cell<Vec<u8>>>("cell").unwrap();

    drop(store.commit(store_path(&root), |_| Ok(())).unwrap());
    let store = Store::open(store_path(&root)).unwrap();
    let raw = store.open_data::<Cell<Vec<u8>>>("cell").unwrap();
    let typed = store.open_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.into_transactions().split();

    {
        let transaction = writes.begin();
        raw.access(transaction.access())
            .unwrap()
            .set(&vec![0])
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = reads.begin();
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
fn commit_and_drop_are_atomic_across_collections() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let first = create_byte_map(&mut store, "first").unwrap();
    let second = create_byte_map(&mut store, "second").unwrap();
    let mut transactions = store.commit(&path, |_| Ok(())).unwrap();

    {
        let transaction = transactions.begin();
        write_pair(transaction.access(), &first, &second).unwrap();
        let first = first.access(transaction.access()).unwrap();
        let second = second.access(transaction.access()).unwrap();
        assert_eq!(
            first.get(&b"key".to_vec()).unwrap(),
            Some(b"first".to_vec())
        );
        assert_eq!(
            second.get(&b"key".to_vec()).unwrap(),
            Some(b"second".to_vec())
        );
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin();
        first
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"pending first".to_vec())
            .unwrap();
        second
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"pending second".to_vec())
            .unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let first = open_byte_map(&store, "first").unwrap();
    let second = open_byte_map(&store, "second").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        first
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        second
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"second".to_vec())
    );
}

#[test]
fn durability_batch_tracks_write_commits_and_shares_one_explicit_barrier() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = StoreSetup::new();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let (mut writes, reads) = store.commit(&path, |_| Ok(())).unwrap().split();

    let mut batch = writes.durability_batch();
    batch.begin().commit().unwrap();
    assert!(!batch.has_pending());

    let transaction = batch.begin();
    cell.access(transaction.access()).unwrap().set(&41).unwrap();
    transaction.commit().unwrap();
    assert!(batch.has_pending());

    let snapshot = reads.begin();
    assert_eq!(
        cell.read(snapshot.access()).unwrap().get().unwrap(),
        Some(41)
    );
    drop(snapshot);

    let transaction = batch.begin();
    cell.access(transaction.access()).unwrap().set(&42).unwrap();
    transaction.commit().unwrap();
    assert!(batch.has_pending());
    batch.sync().unwrap();
    assert!(!batch.has_pending());
    batch.finish().unwrap();

    let snapshot = reads.begin();
    assert_eq!(
        cell.read(snapshot.access()).unwrap().get().unwrap(),
        Some(42)
    );
    drop(snapshot);
    drop(reads);
    drop(writes);

    let store = Store::open(path).unwrap();
    let cell = store.open_data::<Cell<u64>>("cell").unwrap();
    let snapshot = store.read_transaction();
    assert_eq!(
        cell.read(snapshot.access()).unwrap().get().unwrap(),
        Some(42)
    );
}

#[test]
fn wrong_store_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut first_store = StoreSetup::new();
    let first = create_byte_map(&mut first_store, "data").unwrap();
    let mut first_transactions = first_store
        .commit(root.path().join("first"), |_| Ok(()))
        .unwrap();

    let mut second_store = StoreSetup::new();
    let second = create_byte_map(&mut second_store, "data").unwrap();
    let _second_transactions = second_store
        .commit(root.path().join("second"), |_| Ok(()))
        .unwrap();

    let transaction = first_transactions.begin();
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

    let transaction = first_transactions.begin();
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
    let mut store = StoreSetup::new();
    let stale = create_byte_map(&mut store, "data").unwrap();
    drop(store.commit(&path, |_| Ok(())).unwrap());

    let store = Store::open(&path).unwrap();
    let current = open_byte_map(&store, "data").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
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
    let mut store = StoreSetup::new();
    let data = create_byte_map(&mut store, "data").unwrap();
    let mut transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();

    let transaction = transactions.begin();
    data.access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"wide".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    let mut access = data.access(transaction.access()).unwrap();
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    access
        .put(&b"second".to_vec(), &b"still writable".to_vec())
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin();
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
    let mut store = StoreSetup::new();
    let data = create_byte_map(&mut store, "data").unwrap();
    let transactions = store.commit(store_path(&root), |_| Ok(())).unwrap();

    let (mut transactions, data) = std::thread::spawn(move || {
        let mut transactions = transactions;
        let transaction = transactions.begin();
        data.access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"value".to_vec())
            .unwrap();
        transaction.commit().unwrap();
        (transactions, data)
    })
    .join()
    .unwrap();

    let transaction = transactions.begin();
    assert_eq!(
        data.access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"value".to_vec())
    );
}
