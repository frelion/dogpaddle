use std::{borrow::Cow, num::NonZeroUsize};

use dogpaddle_store::{AppendLog, CodecError, ScanLimit, Store, StoreError, StoreValue};

use crate::support::store_path;

fn create_log<T: StoreValue>(store: &mut Store, name: &str) -> AppendLog<T> {
    store.create_data(name).unwrap()
}

fn scan_values<T>(
    access: &dogpaddle_store::AppendLogAccess<'_, T>,
    offset: u64,
    limit: ScanLimit,
) -> (Vec<(u64, T)>, dogpaddle_store::AppendLogScan)
where
    T: StoreValue,
{
    let mut values = Vec::new();
    let scan = access
        .scan(offset, limit, |entry| -> Result<(), StoreError> {
            values.push((entry.offset(), entry.decode_owned()?));
            Ok(())
        })
        .unwrap();
    (values, scan)
}

#[test]
fn fresh_log_is_empty_and_append_offsets_are_stable() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<String>(&mut store, "log");
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.bounds().unwrap(), 0..0);
        let (_, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
        assert_eq!(scan.next_offset, 0);
        assert!(scan.caught_up);

        assert_eq!(access.append(&"a".to_owned()).unwrap(), 0);
        assert_eq!(access.append(&"bb".to_owned()).unwrap(), 1);
        assert_eq!(access.bounds().unwrap(), 0..2);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, "a".to_owned()), (1, "bb".to_owned())]);
    assert_eq!(scan.next_offset, 2);
    assert!(scan.caught_up);
}

#[test]
fn multiple_accesses_see_same_transaction_appends() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut first = log.access(transaction.access()).unwrap();
    let mut second = log.access(transaction.access()).unwrap();
    assert_eq!(first.append(&10).unwrap(), 0);
    assert_eq!(second.append(&20).unwrap(), 1);
    let (values, scan) = scan_values(&first, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 10), (1, 20)]);
    assert!(scan.caught_up);
    transaction.commit().unwrap();
}

#[test]
fn batch_append_is_ordered_and_visible_across_same_transaction_accesses() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut first = log.access(transaction.access()).unwrap();
        let mut second = log.access(transaction.access()).unwrap();
        assert_eq!(first.append_batch(&[]).unwrap(), 0..0);
        assert_eq!(first.append_batch(&[10, 20, 30]).unwrap(), 0..3);
        assert_eq!(second.bounds().unwrap(), 0..3);
        assert_eq!(second.append_batch(&[40, 50]).unwrap(), 3..5);
        assert_eq!(first.bounds().unwrap(), 0..5);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, scan) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 10), (1, 20), (2, 30), (3, 40), (4, 50)]);
    assert!(scan.caught_up);
}

#[test]
fn item_and_byte_limits_produce_exact_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access.append(&b"aaa".to_vec()).unwrap();
        access.append(&b"bbbb".to_vec()).unwrap();
        access.append(&b"ccccc".to_vec()).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (first, first_scan) = scan_values(&access, 0, ScanLimit::new(2, 1_024).unwrap());
    assert_eq!(first, vec![(0, b"aaa".to_vec()), (1, b"bbbb".to_vec())]);
    assert_eq!(first_scan.next_offset, 2);
    assert!(!first_scan.caught_up);

    // Each item is charged for its eight-byte offset plus encoded value.
    let (second, second_scan) = scan_values(
        &access,
        first_scan.next_offset,
        ScanLimit::new(10, 13).unwrap(),
    );
    assert_eq!(second, vec![(2, b"ccccc".to_vec())]);
    assert!(second_scan.caught_up);
}

#[test]
fn oversized_first_entry_is_retryable_in_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&b"abc".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let error = access
        .scan::<StoreError>(0, ScanLimit::new(1, 10).unwrap(), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::ItemTooLarge {
            size: 11,
            limit: 10
        }
    ));
    let (_, scan) = scan_values(&access, 0, ScanLimit::new(1, 11).unwrap());
    assert!(scan.caught_up);
    transaction.commit().unwrap();
}

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
    transaction.commit().unwrap();
}

#[test]
fn truncation_is_bounded_and_never_reuses_offsets() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.append(&10).unwrap(), 0);
        assert_eq!(access.append(&20).unwrap(), 1);
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            1
        );
        assert_eq!(access.bounds().unwrap(), 1..2);
        assert_eq!(
            access
                .truncate_before(2, NonZeroUsize::new(1).unwrap())
                .unwrap(),
            2
        );
        assert_eq!(access.bounds().unwrap(), 2..2);
        assert_eq!(access.append(&30).unwrap(), 2);
        assert_eq!(access.bounds().unwrap(), 2..3);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let (values, _) = scan_values(&access, 2, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(2, 30)]);
}

#[test]
fn cursor_truncation_deletes_each_exact_prefix_entry_without_skipping() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert_eq!(access.append_batch(&[0, 1, 2, 3, 4]).unwrap(), 0..5);
    assert_eq!(
        access
            .truncate_before(4, NonZeroUsize::new(3).unwrap())
            .unwrap(),
        3
    );
    assert_eq!(access.bounds().unwrap(), 3..5);
    let (values, _) = scan_values(&access, 3, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(3, 3), (4, 4)]);
    assert_eq!(
        access
            .truncate_before(4, NonZeroUsize::new(2).unwrap())
            .unwrap(),
        4
    );
    let (values, _) = scan_values(&access, 4, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(4, 4)]);
    transaction.commit().unwrap();
}

#[test]
fn invalid_offsets_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&1)
            .unwrap();
        transaction.commit().unwrap();
    }

    for offset in [2, u64::MAX] {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        assert!(matches!(
            access.scan::<StoreError>(
                offset,
                ScanLimit::new(1, 1_024).unwrap(),
                |_| Ok(())
            ),
            Err(StoreError::LogOffsetOutOfRange {
                offset: actual,
                head: 0,
                tail: 1
            }) if actual == offset
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn stale_cursors_and_future_gc_targets_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access.append(&1).unwrap();
        access
            .truncate_before(1, NonZeroUsize::new(1).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        assert!(matches!(
            access.scan::<StoreError>(0, ScanLimit::new(1, 1_024).unwrap(), |_| Ok(())),
            Err(StoreError::LogOffsetOutOfRange {
                offset: 0,
                head: 1,
                tail: 1
            })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }

    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    assert!(matches!(
        access.truncate_before(2, NonZeroUsize::new(1).unwrap()),
        Err(StoreError::LogOffsetOutOfRange {
            offset: 2,
            head: 1,
            tail: 1
        })
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn dropped_append_and_truncation_transactions_roll_back() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = create_log::<u64>(&mut store, "log");
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&1)
            .unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_eq!(access.bounds().unwrap(), 0..0);
        access.append(&1).unwrap();
        access.append(&2).unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .truncate_before(2, NonZeroUsize::new(2).unwrap())
            .unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<u64>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), 0..2);
    let (values, _) = scan_values(&access, 0, ScanLimit::new(10, 1_024).unwrap());
    assert_eq!(values, vec![(0, 1), (1, 2)]);
}

#[derive(Debug)]
enum VisitError {
    Stop,
}

impl From<StoreError> for VisitError {
    fn from(_: StoreError) -> Self {
        Self::Stop
    }
}

#[test]
fn visitor_business_errors_poison_and_roll_back_prior_writes() {
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
    let input_access = input.access(transaction.access()).unwrap();
    let mut output_access = output.access(transaction.access()).unwrap();
    let error = input_access
        .scan(
            0,
            ScanLimit::new(10, 1_024).unwrap(),
            |entry| -> Result<(), VisitError> {
                output_access.append_entry(&entry)?;
                Err(VisitError::Stop)
            },
        )
        .unwrap_err();
    assert!(matches!(error, VisitError::Stop));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..0
    );
}

#[test]
fn projection_codec_errors_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append(&b"bad".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let error = access
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            entry.project::<()>(|_| Err(CodecError::new("bad projection")))
        })
        .unwrap_err();
    assert!(matches!(error, StoreError::Codec(_)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

enum BatchValue {
    Good(u64),
    Reject,
}

impl StoreValue for BatchValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        match self {
            Self::Good(value) => Ok(value.to_be_bytes()),
            Self::Reject => Err(CodecError::new("rejected batch encode")),
        }
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let bytes: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid batch value"))?;
        Ok(Self::Good(u64::from_be_bytes(bytes)))
    }
}

#[test]
fn batch_encoding_failure_rolls_back_entries_written_before_the_error() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<BatchValue>(&mut store, "log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        let error = access
            .append_batch(&[BatchValue::Good(1), BatchValue::Reject, BatchValue::Good(3)])
            .unwrap_err();
        assert!(matches!(error, StoreError::Codec(_)));
        assert!(matches!(
            access.bounds(),
            Err(StoreError::TransactionPoisoned)
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }

    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), 0..0);
}

struct RejectedDecode;

impl StoreValue for RejectedDecode {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok([])
    }

    fn decode_value(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("rejected decode"))
    }
}

#[test]
fn swallowed_full_decode_errors_poison_clean_and_dirty_reads() {
    for committed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let log = create_log::<RejectedDecode>(&mut store, "log");
        let mut transactions = store.into_transactions();
        if committed {
            let transaction = transactions.begin().unwrap();
            log.access(transaction.access())
                .unwrap()
                .append(&RejectedDecode)
                .unwrap();
            transaction.commit().unwrap();
        }

        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        if !committed {
            access.append(&RejectedDecode).unwrap();
        }
        let error = access
            .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
                let _ = entry.decode_owned();
                Ok::<(), StoreError>(())
            })
            .unwrap_err();
        assert!(matches!(error, StoreError::TransactionPoisoned));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));
    }
}

#[test]
fn swallowed_projection_errors_still_poison_the_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let log = create_log::<Vec<u8>>(&mut store, "log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    access.append(&b"bad".to_vec()).unwrap();
    let error = access
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            let _ = entry.project::<()>(|_| Err(CodecError::new("ignored")));
            Ok::<(), StoreError>(())
        })
        .unwrap_err();
    assert!(matches!(error, StoreError::TransactionPoisoned));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn append_entry_rejects_another_transaction() {
    let left_root = tempfile::tempdir().unwrap();
    let right_root = tempfile::tempdir().unwrap();
    let mut left_store = Store::create(store_path(&left_root)).unwrap();
    let mut right_store = Store::create(store_path(&right_root)).unwrap();
    let input = create_log::<u64>(&mut left_store, "input");
    let output = create_log::<u64>(&mut right_store, "output");
    let mut left_transactions = left_store.into_transactions();
    let mut right_transactions = right_store.into_transactions();
    {
        let transaction = left_transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append(&7)
            .unwrap();
        transaction.commit().unwrap();
    }

    let left_transaction = left_transactions.begin().unwrap();
    let right_transaction = right_transactions.begin().unwrap();
    let input = input.access(left_transaction.access()).unwrap();
    let mut output = output.access(right_transaction.access()).unwrap();
    let error = input
        .scan(0, ScanLimit::new(1, 1_024).unwrap(), |entry| {
            assert!(matches!(
                output.append_entry(&entry),
                Err(StoreError::WrongTransaction)
            ));
            Ok::<(), StoreError>(())
        })
        .unwrap_err();
    assert!(matches!(error, StoreError::TransactionPoisoned));
    assert!(matches!(
        left_transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
    assert!(matches!(
        right_transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
