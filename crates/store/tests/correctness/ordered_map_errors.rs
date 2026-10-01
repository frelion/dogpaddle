use std::borrow::Cow;

use dogpaddle_store::{
    CodecError, ScanDirection, ScanLimit, Store, StoreError, StoreKey, StoreValue,
};

use crate::support::{create_map, store_path};

use super::open_map;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BrokenKey;

impl StoreKey for BrokenKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional key failure"))
    }

    fn decode_key(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional key failure"))
    }
}

impl StoreValue for BrokenKey {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional value failure"))
    }

    fn decode_value(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional value failure"))
    }
}

#[test]
fn key_codec_errors_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_map::<u64, u64>(&mut store, "safe").unwrap();
    let broken = create_map::<BrokenKey, BrokenKey>(&mut store, "broken").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    safe.access(transaction.access())
        .unwrap()
        .put(&1, &1)
        .unwrap();
    assert!(matches!(
        broken.access(transaction.access()).unwrap().get(&BrokenKey),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get(&1).unwrap(),
        None
    );
}

#[test]
fn failed_page_decode_poisons_and_rolls_back_prior_writes() {
    for write_in_scan_transaction in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let raw = create_map::<u64, Vec<u8>>(&mut store, "map").unwrap();
        let typed = open_map::<u64, u64>(&store, "map").unwrap();
        let marker = create_map::<u64, u64>(&mut store, "marker").unwrap();
        let mut writes = store.into_transactions();
        if !write_in_scan_transaction {
            let transaction = writes.begin();
            let mut raw = raw.access(transaction.access()).unwrap();
            raw.put(&1, &10_u64.to_be_bytes().to_vec()).unwrap();
            raw.put(&2, &vec![0]).unwrap();
            transaction.commit().unwrap();
        }
        let transaction = writes.begin();
        marker
            .access(transaction.access())
            .unwrap()
            .put(&1, &42)
            .unwrap();
        if write_in_scan_transaction {
            let mut raw = raw.access(transaction.access()).unwrap();
            raw.put(&1, &10_u64.to_be_bytes().to_vec()).unwrap();
            raw.put(&2, &vec![0]).unwrap();
        }
        let access = typed.access(transaction.access()).unwrap();
        assert!(matches!(
            access.scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(10, 1024).unwrap()
            ),
            Err(StoreError::Codec(_))
        ));
        assert!(matches!(
            access.get(&1),
            Err(StoreError::TransactionPoisoned)
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
        let transaction = writes.begin();
        assert_eq!(
            marker
                .access(transaction.access())
                .unwrap()
                .get(&1)
                .unwrap(),
            None
        );
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct UndecodableKey(u64);

impl StoreKey for UndecodableKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        match u64::decode_key(bytes)? {
            1 => Ok(Self(1)),
            _ => Err(CodecError::new("intentional later key failure")),
        }
    }
}

#[test]
fn a_later_malformed_key_discards_the_whole_page_and_poisons() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = create_map::<u64, u64>(&mut store, "map").unwrap();
    let malformed = open_map::<UndecodableKey, u64>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        let mut raw = raw.access(transaction.access()).unwrap();
        raw.put(&1, &1).unwrap();
        raw.put(&2, &2).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
    let access = malformed.access(transaction.access()).unwrap();
    let result = access.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(2, 1_024).unwrap(),
    );
    assert!(matches!(result, Err(StoreError::Codec(_))));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn admission_precedes_value_decode_for_points_and_later_scan_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = create_map::<u64, Vec<u8>>(&mut store, "map").unwrap();
    let typed = open_map::<u64, u64>(&store, "map").unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    {
        let transaction = writes.begin();
        let mut raw = raw.access(transaction.access()).unwrap();
        raw.put(&1, &10_u64.to_be_bytes().to_vec()).unwrap();
        raw.put(&2, &vec![0]).unwrap();
        transaction.commit().unwrap();
    }
    let snapshot = reads.begin();
    let access = typed.read(snapshot.access()).unwrap();
    assert!(matches!(
        access.get_bounded(&2, 0),
        Err(StoreError::ItemTooLarge { size: 1, limit: 0 })
    ));
    let page = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(2, 16).unwrap(),
        )
        .unwrap();
    assert_eq!(page.entries, vec![(1, 10)]);
    assert_eq!(page.continuation, Some(1));
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            page.continuation.as_ref(),
            ScanLimit::new(2, 8).unwrap()
        ),
        Err(StoreError::ItemTooLarge { size: 9, limit: 8 })
    ));
    assert_eq!(access.get(&1).unwrap(), Some(10));
    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            page.continuation.as_ref(),
            ScanLimit::new(2, 9).unwrap()
        ),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        access.get(&1),
        Err(StoreError::TransactionPoisoned)
    ));
    let fresh = reads.begin();
    assert_eq!(
        typed.read(fresh.access()).unwrap().get(&1).unwrap(),
        Some(10)
    );
}
