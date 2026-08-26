use std::{borrow::Cow, num::NonZeroUsize};

use dogpaddle_store::{CodecError, ScanLimit, Store, StoreError, StoreValue};

use crate::support::store_path;

use super::create_log;

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
