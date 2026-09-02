use dogpaddle_store::{
    AppendLog, Cell, OrderedMap, ReadOnly, ScanDirection, ScanLimit, Small, Store, StoreError,
};

use crate::support::store_path;

#[test]
fn read_only_capabilities_bind_every_collection_to_write_and_read_transactions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let map = store
        .create_data::<OrderedMap<u64, String, Small>>("map")
        .unwrap();
    let source = store.create_data::<AppendLog<Vec<u8>>>("source").unwrap();
    let output = store.create_data::<AppendLog<Vec<u8>>>("output").unwrap();
    let cell_input = ReadOnly::new(cell.clone());
    let map_input = ReadOnly::new(map.clone());
    let log_input = ReadOnly::new(source.clone());
    let (mut writes, reads) = store.into_transactions().split();

    {
        let transaction = writes.begin().unwrap();
        let access = transaction.access();
        cell.access(access).unwrap().set(&41).unwrap();
        map.access(access)
            .unwrap()
            .put(&1, &"one".to_owned())
            .unwrap();
        source
            .access(access)
            .unwrap()
            .append(&b"change".to_vec())
            .unwrap();

        assert_eq!(cell_input.access(access).unwrap().get().unwrap(), Some(41));
        assert_eq!(
            map_input
                .access(access)
                .unwrap()
                .get(&1)
                .unwrap()
                .as_deref(),
            Some("one")
        );
        let input = log_input.access(access).unwrap();
        let mut output = output.access(access).unwrap();
        input
            .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
                output.append_entry(&entry)?;
                Ok::<(), StoreError>(())
            })
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = reads.begin().unwrap();
    let access = transaction.access();
    assert_eq!(cell_input.read(access).unwrap().get().unwrap(), Some(41));
    let map = map_input.read(access).unwrap();
    assert!(matches!(
        map.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1).unwrap(),
            |_| unreachable!("oversize entry must not be visited"),
        ),
        Err(StoreError::ItemTooLarge { .. })
    ));
    let mut map_values = Vec::new();
    map.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(1, 1_024).unwrap(),
        |entry| {
            map_values.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        },
    )
    .unwrap();
    assert_eq!(map_values, [(1, "one".to_owned())]);
    assert_eq!(map.get(&1).unwrap().as_deref(), Some("one"));
    let mut log_values = Vec::new();
    log_input
        .read(access)
        .unwrap()
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            log_values.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(log_values, [b"change".to_vec()]);

    drop(transaction);
    drop(reads);
    let transaction = writes.begin().unwrap();
    let mut forwarded = Vec::new();
    output
        .access(transaction.access())
        .unwrap()
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            forwarded.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(forwarded, [b"change".to_vec()]);
}
