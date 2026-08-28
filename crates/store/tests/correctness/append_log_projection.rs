use std::borrow::Cow;

use dogpaddle_store::{CodecError, ScanLimit, Store, StoreError, StoreValue};

use crate::support::store_path;

use super::{create_log, scan_values};

#[derive(Debug, Eq, PartialEq)]
struct WideRecord {
    diff: i64,
    payload: Vec<u8>,
}

impl StoreValue for WideRecord {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = Vec::with_capacity(8 + self.payload.len());
        encoded.extend_from_slice(&self.diff.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Ok(encoded)
    }

    fn decode_value(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        panic!("projection must not fully decode WideRecord")
    }
}

fn decode_diff(encoded: &[u8]) -> Result<i64, CodecError> {
    let diff = encoded
        .get(..8)
        .ok_or_else(|| CodecError::new("missing diff"))?;
    Ok(i64::from_be_bytes(
        diff.try_into()
            .map_err(|_| CodecError::new("invalid diff"))?,
    ))
}

#[test]
fn projection_reads_only_needed_fields_and_filter_forwards_encoded_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let input = create_log::<WideRecord>(&mut store, "input");
    let output = create_log::<WideRecord>(&mut store, "output");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut input = input.access(transaction.access()).unwrap();
        input
            .append(&WideRecord {
                diff: -1,
                payload: vec![7; 4_096],
            })
            .unwrap();
        input
            .append(&WideRecord {
                diff: 1,
                payload: vec![9; 4_096],
            })
            .unwrap();
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin().unwrap();
        let input = input.access(transaction.access()).unwrap();
        let mut output = output.access(transaction.access()).unwrap();
        let mut diffs = Vec::new();
        let scan = input
            .scan(0, ScanLimit::new(10, 16_384).unwrap(), |entry| {
                let diff = entry.project(decode_diff)?;
                diffs.push(diff);
                if diff > 0 {
                    output.append_entry(&entry)?;
                }
                Ok::<(), StoreError>(())
            })
            .unwrap();
        assert_eq!(diffs, vec![-1, 1]);
        assert!(scan.caught_up);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    assert_eq!(output.retained_bytes().unwrap(), 4_112);
    let mut forwarded = Vec::new();
    output
        .scan(0, ScanLimit::new(10, 8_192).unwrap(), |entry| {
            forwarded.push((entry.offset(), entry.project(decode_diff)?));
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert_eq!(forwarded, vec![(0, 1)]);
}

#[test]
fn full_decode_can_precede_unchanged_forwarding() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let input = create_log::<u64>(&mut store, "input");
    let output = create_log::<u64>(&mut store, "output");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append(&7)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let input = input.access(transaction.access()).unwrap();
    let mut output = output.access(transaction.access()).unwrap();
    input
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            if entry.decode_owned()? == 7 {
                output.append_entry(&entry)?;
            }
            Ok::<(), StoreError>(())
        })
        .unwrap();
    let (values, _) = scan_values(&output, 0, ScanLimit::new(1, 1_024).unwrap());
    assert_eq!(values, vec![(0, 7)]);
    assert_eq!(output.retained_bytes().unwrap(), 16);
    transaction.commit().unwrap();
}
