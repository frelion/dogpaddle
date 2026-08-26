use std::borrow::Cow;

use dogpaddle_store::{
    CodecError, ScanDirection, ScanLimit, Small, Store, StoreError, StoreKey, StoreValue,
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
    let safe = create_map::<u64, u64, Small>(&mut store, "safe").unwrap();
    let broken = create_map::<BrokenKey, BrokenKey, Small>(&mut store, "broken").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
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

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get(&1).unwrap(),
        None
    );
}

#[test]
fn scan_decode_errors_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_map::<u64, u64, Small>(&mut store, "safe").unwrap();
    let raw = create_map::<Vec<u8>, Vec<u8>, Small>(&mut store, "raw").unwrap();
    let malformed = open_map::<u64, u64, Small>(&store, "raw").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(transaction.access())
        .unwrap()
        .put(&1, &1)
        .unwrap();
    raw.access(transaction.access())
        .unwrap()
        .put(&vec![0], &0_u64.to_be_bytes().to_vec())
        .unwrap();
    assert!(matches!(
        malformed.access(transaction.access()).unwrap().scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
            |entry| {
                entry.decode_owned()?;
                Ok::<(), StoreError>(())
            },
        ),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get(&1).unwrap(),
        None
    );
    assert_eq!(
        raw.access(transaction.access())
            .unwrap()
            .get(&vec![0])
            .unwrap(),
        None
    );
}

#[derive(Debug, Eq, PartialEq)]
enum VisitError {
    Store,
    Business,
}

impl From<StoreError> for VisitError {
    fn from(_error: StoreError) -> Self {
        Self::Store
    }
}

#[test]
fn visitor_errors_poison_and_roll_back_prior_store_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let source = create_map::<u64, u64, Small>(&mut store, "source").unwrap();
    let output = create_map::<u64, u64, Small>(&mut store, "output").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut source = source.access(transaction.access()).unwrap();
        source.put(&1, &10).unwrap();
        source.put(&2, &20).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let source = source.access(transaction.access()).unwrap();
    let mut output_access = output.access(transaction.access()).unwrap();
    let result = source.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(10, 1_024).unwrap(),
        |entry| {
            let (key, value) = entry.decode_owned()?;
            output_access.put(&key, &value)?;
            if key == 2 {
                return Err(VisitError::Business);
            }
            Ok(())
        },
    );
    assert_eq!(result, Err(VisitError::Business));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .get(&1)
            .unwrap(),
        None
    );
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .get(&2)
            .unwrap(),
        None
    );
}

#[test]
fn swallowed_full_decode_errors_poison_clean_and_dirty_scans() {
    assert_swallowed_full_decode_error_poisons(false);
    assert_swallowed_full_decode_error_poisons(true);
}

fn assert_swallowed_full_decode_error_poisons(dirty: bool) {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = create_map::<u64, Vec<u8>, Small>(&mut store, "map").unwrap();
    let typed = open_map::<u64, u64, Small>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();

    if !dirty {
        let transaction = transactions.begin().unwrap();
        raw.access(transaction.access())
            .unwrap()
            .put(&1, &vec![0])
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    if dirty {
        raw.access(transaction.access())
            .unwrap()
            .put(&1, &vec![0])
            .unwrap();
    }
    let access = typed.access(transaction.access()).unwrap();
    let result = access.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(10, 1_024).unwrap(),
        |entry| {
            assert!(entry.decode_owned().is_err());
            Ok::<(), StoreError>(())
        },
    );
    assert!(matches!(result, Err(StoreError::TransactionPoisoned)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn swallowed_projection_errors_still_stop_the_scan_and_poison() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, u64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        map.access(transaction.access())
            .unwrap()
            .put(&1, &1)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    let result = access.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(10, 1_024).unwrap(),
        |entry| {
            assert!(
                entry
                    .project(|_, _| Err::<(), _>(CodecError::new("projection failure")))
                    .is_err()
            );
            Ok::<(), StoreError>(())
        },
    );
    assert!(matches!(result, Err(StoreError::TransactionPoisoned)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct UndecodableKey(u64);

impl StoreKey for UndecodableKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional continuation failure"))
    }
}

#[test]
fn continuation_is_decoded_before_the_first_callback() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = create_map::<u64, u64, Small>(&mut store, "map").unwrap();
    let malformed = open_map::<UndecodableKey, u64, Small>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut raw = raw.access(transaction.access()).unwrap();
        raw.put(&1, &1).unwrap();
        raw.put(&2, &2).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = malformed.access(transaction.access()).unwrap();
    let mut visits = 0;
    let result = access.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(1, 1_024).unwrap(),
        |_| {
            visits += 1;
            Ok::<(), StoreError>(())
        },
    );
    assert!(matches!(result, Err(StoreError::Codec(_))));
    assert_eq!(visits, 0);
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
