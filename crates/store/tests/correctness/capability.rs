use dogpaddle_store::{
    AppendLog, Cell, Large, OrderedMap, ReadOnly, ScanDirection, ScanLimit, Small, Store,
    StoreData, StoreError,
};

use crate::support::store_path;

#[test]
fn read_only_cell_observes_the_same_transaction_and_committed_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let input = ReadOnly::new(cell.clone());
    let cloned_input = input.clone();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    let mut cell_access = cell.access(access).unwrap();
    let input_access = input.access(access).unwrap();
    cell_access.set(&41).unwrap();
    assert_eq!(input_access.get().unwrap(), Some(41));
    transaction.commit().unwrap();

    let (_, reads) = transactions.split();
    let transaction = reads.begin().unwrap();
    assert_eq!(
        cloned_input
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(41)
    );
}

#[test]
fn read_only_ordered_maps_support_point_reads_and_scans() {
    assert_read_only_ordered_map::<Small>();
    assert_read_only_ordered_map::<Large>();
}

fn assert_read_only_ordered_map<SIZE>()
where
    OrderedMap<u64, String, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = store
        .create_data::<OrderedMap<u64, String, SIZE>>("map")
        .unwrap();
    let input = ReadOnly::new(map.clone());
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    let mut map_access = map.access(access).unwrap();
    map_access.put(&1, &"one".to_owned()).unwrap();
    map_access.put(&2, &"two".to_owned()).unwrap();
    map_access.put(&3, &"three".to_owned()).unwrap();

    let input_access = input.access(access).unwrap();
    assert_eq!(input_access.get(&2).unwrap().as_deref(), Some("two"));

    let mut first_page = Vec::new();
    let continuation = input_access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(2, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                first_page.push(entry.decode_owned()?);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        first_page,
        vec![(1, "one".to_owned()), (2, "two".to_owned())]
    );
    assert_eq!(continuation, Some(2));

    let mut second_page = Vec::new();
    let continuation = input_access
        .scan(
            ..,
            ScanDirection::Ascending,
            continuation.as_ref(),
            ScanLimit::new(2, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                second_page.push(entry.decode_owned()?);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(second_page, vec![(3, "three".to_owned())]);
    assert_eq!(continuation, None);
    transaction.commit().unwrap();

    let (_, reads) = transactions.split();
    let transaction = reads.begin().unwrap();
    let input_access = input.read(transaction.access()).unwrap();
    assert_eq!(input_access.get(&1).unwrap().as_deref(), Some("one"));

    let mut observed = Vec::new();
    input_access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(3, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                observed.push(entry.decode_owned()?);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(observed.len(), 3);
}

#[test]
fn read_only_append_logs_support_independent_scans_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = store.create_data::<AppendLog<Vec<u8>>>("changes").unwrap();
    let first_input = ReadOnly::new(log.clone());
    let second_input = first_input.clone();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    log.access(transaction.access())
        .unwrap()
        .append_batch(&[b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let first_access = first_input.access(transaction.access()).unwrap();
    let second_access = second_input.access(transaction.access()).unwrap();
    assert_eq!(first_access.bounds().unwrap(), 0..3);
    assert_eq!(second_access.bounds().unwrap(), 0..3);

    let mut first_observed = Vec::new();
    let first_scan = first_access
        .scan(
            0,
            ScanLimit::new(2, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                first_observed.push((entry.offset(), entry.decode_owned()?));
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        first_observed,
        vec![(0, b"one".to_vec()), (1, b"two".to_vec())]
    );
    assert_eq!(first_scan.next_offset, 2);
    assert!(!first_scan.caught_up);

    let mut second_observed = Vec::new();
    let second_scan = second_access
        .scan(
            1,
            ScanLimit::new(10, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                second_observed.push((entry.offset(), entry.decode_owned()?));
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        second_observed,
        vec![(1, b"two".to_vec()), (2, b"three".to_vec())]
    );
    assert_eq!(second_scan.next_offset, 3);
    assert!(second_scan.caught_up);
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let reopened = store.open_data::<AppendLog<Vec<u8>>>("changes").unwrap();
    let reopened_input = ReadOnly::new(reopened);
    let (_, reads) = store.into_transactions().split();
    let transaction = reads.begin().unwrap();
    let mut observed_after_reopen = Vec::new();
    let scan = reopened_input
        .read(transaction.access())
        .unwrap()
        .scan(
            0,
            ScanLimit::new(10, 1024).unwrap(),
            |entry| -> Result<(), StoreError> {
                observed_after_reopen.push((entry.offset(), entry.decode_owned()?));
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        observed_after_reopen,
        vec![
            (0, b"one".to_vec()),
            (1, b"two".to_vec()),
            (2, b"three".to_vec()),
        ]
    );
    assert!(scan.caught_up);
}

#[test]
fn read_only_append_log_rw_view_preserves_entry_forwarding_authority() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let source = store.create_data::<AppendLog<Vec<u8>>>("source").unwrap();
    let input = ReadOnly::new(source.clone());
    let output = store.create_data::<AppendLog<Vec<u8>>>("output").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    source
        .access(access)
        .unwrap()
        .append(&b"change".to_vec())
        .unwrap();
    let input_access = input.access(access).unwrap();
    let mut output_access = output.access(access).unwrap();
    input_access
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            output_access.append_entry(&entry)?;
            Ok::<(), StoreError>(())
        })
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    let mut observed = Vec::new();
    output
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            observed.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(observed, vec![b"change".to_vec()]);
}
