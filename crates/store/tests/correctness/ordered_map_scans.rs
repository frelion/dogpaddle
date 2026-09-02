use std::{
    borrow::Cow,
    sync::atomic::{AtomicUsize, Ordering},
};

use dogpaddle_store::{
    CodecError, ScanDirection, ScanLimit, Small, Store, StoreData, StoreError, StoreValue,
};

use crate::support::{ByteMap, create_byte_map, create_map, store_path};

#[derive(Clone, Debug, Eq, PartialEq)]
struct WideValue(Vec<u8>);

static FULL_VALUE_DECODES: AtomicUsize = AtomicUsize::new(0);

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
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<u64, WideValue, Small>(&mut store, "map").unwrap();
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
fn byte_map_binary_keys_page_in_both_directions() {
    assert_binary_key_pages::<Small>();
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
