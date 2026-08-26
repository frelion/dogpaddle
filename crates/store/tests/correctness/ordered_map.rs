use std::{
    borrow::Cow,
    sync::atomic::{AtomicUsize, Ordering},
};

use dogpaddle_store::{
    CodecError, Large, OrderedMap, ScanDirection, ScanLimit, Small, Store, StoreData, StoreError,
    StoreKey, StoreValue,
};

use crate::support::{ByteMap, TestValue, create_byte_map, create_map, store_path};

fn open_map<K: StoreKey, V: StoreValue, SIZE>(
    store: &Store,
    name: &str,
) -> Result<OrderedMap<K, V, SIZE>, StoreError>
where
    OrderedMap<K, V, SIZE>: StoreData,
{
    store.open_data(name)
}

#[test]
fn ordered_map_point_operations_are_exact() {
    assert_ordered_map_point_operations::<Small>();
    assert_ordered_map_point_operations::<Large>();
}

fn assert_ordered_map_point_operations<SIZE>()
where
    OrderedMap<u64, String, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, String, SIZE>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = map.access(transaction.access()).unwrap();
    assert_eq!(access.get(&7).unwrap(), None);
    access.put(&7, &"first".to_owned()).unwrap();
    assert_eq!(access.get(&7).unwrap(), Some("first".to_owned()));
    access.put(&7, &"second".to_owned()).unwrap();
    assert_eq!(access.get(&7).unwrap(), Some("second".to_owned()));
    assert!(access.remove(&7).unwrap());
    assert!(!access.remove(&7).unwrap());
    assert_eq!(access.get(&7).unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn ordered_map_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let map = create_map::<u64, TestValue, Large>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    map.access(transaction.access())
        .unwrap()
        .put(&42, &TestValue(9))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let map = open_map::<u64, TestValue, Large>(&store, "map").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(transaction.access()).unwrap().get(&42).unwrap(),
        Some(TestValue(9))
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestKey(u64);

impl StoreKey for TestKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_key()
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        u64::decode_key(bytes).map(Self)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BorrowedKey(Vec<u8>);

impl StoreKey for BorrowedKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.0.as_slice())
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Ok(Self(bytes.into_owned()))
    }
}

#[test]
fn ordered_map_accepts_external_key_and_value_codecs() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<TestKey, TestValue, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    map.access(transaction.access())
        .unwrap()
        .put(&TestKey(3), &TestValue(4))
        .unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        map.access(transaction.access())
            .unwrap()
            .get(&TestKey(3))
            .unwrap(),
        Some(TestValue(4))
    );
}

#[test]
fn borrowed_key_codecs_support_points_ranges_and_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<BorrowedKey, u64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();

    let keys = [
        BorrowedKey(b"a".to_vec()),
        BorrowedKey(b"b".to_vec()),
        BorrowedKey(b"c".to_vec()),
    ];
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(transaction.access()).unwrap();
        for (value, key) in keys.iter().enumerate() {
            access.put(key, &(value as u64)).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    assert_eq!(access.get(&keys[1]).unwrap(), Some(1));
    let limit = ScanLimit::new(1, 1_024).unwrap();
    let mut ascending_items = Vec::new();
    let ascending_continuation = access
        .scan(
            (
                std::ops::Bound::Included(&keys[0]),
                std::ops::Bound::Included(&keys[2]),
            ),
            ScanDirection::Ascending,
            None,
            limit,
            |entry| {
                ascending_items.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(ascending_items, vec![(keys[0].clone(), 0)]);
    assert_eq!(ascending_continuation, Some(keys[0].clone()));
    let mut descending_items = Vec::new();
    let descending_continuation = access
        .scan(.., ScanDirection::Descending, None, limit, |entry| {
            descending_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(descending_items, vec![(keys[2].clone(), 2)]);
    assert_eq!(descending_continuation, Some(keys[2].clone()));
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct WideValue(Vec<u8>);

static FULL_VALUE_DECODES: AtomicUsize = AtomicUsize::new(0);

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

impl StoreValue for WideValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.0.as_slice())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        FULL_VALUE_DECODES.fetch_add(1, Ordering::Relaxed);
        Ok(Self(bytes.into_owned()))
    }
}

#[test]
fn projection_reads_logical_keys_and_fields_without_full_value_decode() {
    assert_projection_reads_logical_keys_and_fields::<Small>();
    assert_projection_reads_logical_keys_and_fields::<Large>();
}

fn assert_projection_reads_logical_keys_and_fields<SIZE>()
where
    OrderedMap<u64, WideValue, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, WideValue, SIZE>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(transaction.access()).unwrap();
        for key in 1_u64..=2 {
            let mut value = vec![0xaa; 8_192];
            value[..8].copy_from_slice(&(key * 10).to_be_bytes());
            access.put(&key, &WideValue(value)).unwrap();
        }
        transaction.commit().unwrap();
    }

    FULL_VALUE_DECODES.store(0, Ordering::Relaxed);
    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    let mut projected = Vec::new();
    let continuation = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 32_768).unwrap(),
            |entry| {
                projected.push(entry.project(|key, value| {
                    let key = u64::from_be_bytes(
                        key.try_into()
                            .map_err(|_| CodecError::new("invalid projected key"))?,
                    );
                    let field = u64::from_be_bytes(
                        value[..8]
                            .try_into()
                            .map_err(|_| CodecError::new("invalid projected field"))?,
                    );
                    Ok((key, field))
                })?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(projected, vec![(1, 10), (2, 20)]);
    assert_eq!(continuation, None);
    assert_eq!(FULL_VALUE_DECODES.load(Ordering::Relaxed), 0);
}

#[test]
fn callbacks_observe_the_admitted_page_while_mutating_the_source_map() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, u64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(transaction.access()).unwrap();
        for key in 1_u64..=3 {
            access.put(&key, &key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let reader = map.access(transaction.access()).unwrap();
    let mut writer = map.access(transaction.access()).unwrap();
    let mut visited = Vec::new();
    let continuation = reader
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
            |entry| {
                let (key, value) = entry.decode_owned()?;
                if key == 1 {
                    assert!(writer.remove(&2)?);
                }
                visited.push((key, value));
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(visited, vec![(1, 1), (2, 2), (3, 3)]);
    assert_eq!(continuation, None);
    assert_eq!(writer.get(&2).unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn later_pages_observe_source_updates_made_by_an_earlier_callback() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, u64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(transaction.access()).unwrap();
        for key in 1_u64..=3 {
            access.put(&key, &key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let reader = map.access(transaction.access()).unwrap();
    let mut writer = map.access(transaction.access()).unwrap();
    let limit = ScanLimit::new(1, 1_024).unwrap();
    let mut first_page = Vec::new();
    let continuation = reader
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            first_page.push(entry.decode_owned()?);
            assert!(writer.remove(&2)?);
            writer.put(&4, &4)?;
            Ok::<(), StoreError>(())
        })
        .unwrap();
    let mut remaining = Vec::new();
    let mut continuation = continuation;
    loop {
        let next = reader
            .scan(
                ..,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
                |entry| {
                    remaining.push(entry.decode_owned()?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        if next.is_none() {
            break;
        }
        continuation = next;
    }

    assert_eq!(first_page, vec![(1, 1)]);
    assert_eq!(remaining, vec![(3, 3), (4, 4)]);
    transaction.commit().unwrap();
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

#[test]
fn byte_maps_support_get_put_replace_and_delete() {
    assert_byte_map_operations::<Small>();
    assert_byte_map_operations::<Large>();
}

fn assert_byte_map_operations<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<SIZE>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let key = b"key".to_vec();
    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    assert_eq!(access.get(&key).unwrap(), None);
    access.put(&key, &b"first".to_vec()).unwrap();
    assert_eq!(access.get(&key).unwrap(), Some(b"first".to_vec()));
    access.put(&key, &b"second".to_vec()).unwrap();
    assert_eq!(access.get(&key).unwrap(), Some(b"second".to_vec()));
    assert!(access.remove(&key).unwrap());
    assert!(!access.remove(&key).unwrap());
    assert_eq!(access.get(&key).unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn data_objects_isolate_identical_keys() {
    assert_data_objects_isolate_identical_keys::<Small>();
    assert_data_objects_isolate_identical_keys::<Large>();
}

fn assert_data_objects_isolate_identical_keys<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let left = create_byte_map::<SIZE>(&mut store, "left").unwrap();
    let right = create_byte_map::<SIZE>(&mut store, "right").unwrap();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut left = left.access(transaction.access()).unwrap();
        let mut right = right.access(transaction.access()).unwrap();
        left.put(&Vec::new(), &b"left-empty".to_vec()).unwrap();
        left.put(&b"key".to_vec(), &b"left".to_vec()).unwrap();
        right.put(&Vec::new(), &b"right-empty".to_vec()).unwrap();
        right.put(&b"key".to_vec(), &b"right".to_vec()).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let left = left.access(transaction.access()).unwrap();
    let right = right.access(transaction.access()).unwrap();
    assert_eq!(left.get(&Vec::new()).unwrap(), Some(b"left-empty".to_vec()));
    assert_eq!(left.get(&b"key".to_vec()).unwrap(), Some(b"left".to_vec()));
    assert_eq!(
        right.get(&Vec::new()).unwrap(),
        Some(b"right-empty".to_vec())
    );
    assert_eq!(
        right.get(&b"key".to_vec()).unwrap(),
        Some(b"right".to_vec())
    );

    let limit = ScanLimit::new(10, 1_024).unwrap();
    let mut left_items = Vec::new();
    let left_continuation = left
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            left_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    let mut right_items = Vec::new();
    let right_continuation = right
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            right_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(
        left_items,
        vec![
            (Vec::new(), b"left-empty".to_vec()),
            (b"key".to_vec(), b"left".to_vec()),
        ]
    );
    assert_eq!(left_continuation, None);
    assert_eq!(
        right_items,
        vec![
            (Vec::new(), b"right-empty".to_vec()),
            (b"key".to_vec(), b"right".to_vec()),
        ]
    );
    assert_eq!(right_continuation, None);
}

#[test]
fn byte_map_writes_are_visible_inside_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let data = create_byte_map::<Small>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = data.access(transaction.access()).unwrap();
    access.put(&b"a".to_vec(), &b"one".to_vec()).unwrap();
    access.put(&b"b".to_vec(), &b"two".to_vec()).unwrap();
    assert_eq!(access.get(&b"a".to_vec()).unwrap(), Some(b"one".to_vec()));
    let mut items = Vec::new();
    let continuation = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 1_024).unwrap(),
            |entry| {
                items.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(
        items,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    );
    assert_eq!(continuation, None);
}

#[test]
fn byte_map_binary_keys_page_in_both_directions() {
    assert_binary_key_pages::<Small>();
    assert_binary_key_pages::<Large>();
}

fn assert_binary_key_pages<SIZE>()
where
    ByteMap<SIZE>: StoreData,
{
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
    let data = create_byte_map::<SIZE>(&mut store, "data").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = data.access(transaction.access()).unwrap();
        for key in &keys {
            access.put(key, key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
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
            let mut page = Vec::new();
            let next = access
                .scan(
                    ..,
                    direction,
                    continuation.as_ref(),
                    ScanLimit::new(1, 1_024).unwrap(),
                    |entry| {
                        page.push(entry.decode_owned()?);
                        Ok::<(), StoreError>(())
                    },
                )
                .unwrap();
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
