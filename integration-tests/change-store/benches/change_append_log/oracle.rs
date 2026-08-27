use std::path::Path;

use dogpaddle_change::Change;
use dogpaddle_change_store_integration::assert_change_eq;
use dogpaddle_store::{AppendLog, Cell, ScanLimit, Store, StoreError, Transactions};

use crate::{case::BenchmarkCase, regular_support::decode_projected_entry, support::decode_entry};

#[derive(Clone, Copy)]
pub(crate) enum DecodeOracle {
    Full,
    Selected,
}

pub(crate) fn validate_raw(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    expected: &[Vec<u8>],
    limit: ScanLimit,
) -> usize {
    let transaction = transactions.begin().expect("begin raw oracle transaction");
    let access = log
        .access(transaction.access())
        .expect("access raw oracle log");
    let mut next = 0_u64;
    let mut index = 0_usize;
    let mut pages = 0_usize;
    loop {
        let progress = access
            .scan(next, limit, |entry| {
                assert_eq!(
                    entry.offset(),
                    u64::try_from(index).expect("offset fits u64")
                );
                entry.project(|encoded| {
                    assert_eq!(encoded, expected[index]);
                    Ok(())
                })?;
                index += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan raw oracle entries");
        pages += 1;
        assert!(
            progress.next_offset > next || progress.caught_up,
            "raw oracle scan must make progress"
        );
        next = progress.next_offset;
        if progress.caught_up {
            break;
        }
    }
    assert_eq!(index, expected.len());
    pages
}

pub(crate) fn validate_decoded(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    case: &BenchmarkCase,
    mode: DecodeOracle,
    limit: ScanLimit,
) -> usize {
    let expected: Vec<&Change> = match mode {
        DecodeOracle::Full => case
            .workload
            .changes
            .iter()
            .map(|generated| &generated.change)
            .collect(),
        DecodeOracle::Selected => case.selected_expected.iter().collect(),
    };
    let transaction = transactions
        .begin()
        .expect("begin decoded oracle transaction");
    let access = log
        .access(transaction.access())
        .expect("access decoded oracle log");
    let mut next = 0_u64;
    let mut index = 0_usize;
    let mut pages = 0_usize;
    loop {
        let progress = access
            .scan(next, limit, |entry| {
                assert_eq!(
                    entry.offset(),
                    u64::try_from(index).expect("offset fits u64")
                );
                let actual = match mode {
                    DecodeOracle::Full => entry.project(decode_entry)?,
                    DecodeOracle::Selected => entry.project(|encoded| {
                        decode_projected_entry(encoded, &case.selected_projections[index])
                    })?,
                };
                assert_change_eq(&actual, expected[index]);
                index += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan decoded oracle entries");
        pages += 1;
        assert!(
            progress.next_offset > next || progress.caught_up,
            "decoded oracle scan must make progress"
        );
        next = progress.next_offset;
        if progress.caught_up {
            break;
        }
    }
    assert_eq!(index, expected.len());
    pages
}

pub(crate) fn validate_reopened_raw(
    path: &Path,
    data_name: &str,
    expected: &[Vec<u8>],
    limit: ScanLimit,
) -> usize {
    let store = Store::open(path).expect("reopen raw oracle Store");
    let log = store
        .open_data::<AppendLog<Vec<u8>>>(data_name)
        .expect("open raw oracle log");
    let mut transactions = store.into_transactions();
    validate_raw(&log, &mut transactions, expected, limit)
}

pub(crate) fn validate_reopened_decoded(
    path: &Path,
    data_name: &str,
    case: &BenchmarkCase,
    mode: DecodeOracle,
    limit: ScanLimit,
) -> usize {
    let store = Store::open(path).expect("reopen decoded oracle Store");
    let log = store
        .open_data::<AppendLog<Vec<u8>>>(data_name)
        .expect("open decoded oracle log");
    let mut transactions = store.into_transactions();
    validate_decoded(&log, &mut transactions, case, mode, limit)
}

pub(crate) fn validate_reopened_pipeline(
    path: &Path,
    case: &BenchmarkCase,
    limit: ScanLimit,
) -> usize {
    let store = Store::open(path).expect("reopen pipeline oracle Store");
    let output = store
        .open_data::<AppendLog<Vec<u8>>>("output")
        .expect("open pipeline output log");
    let cursor = store
        .open_data::<Cell<u64>>("cursor")
        .expect("open pipeline cursor");
    let mut transactions = store.into_transactions();
    let pages = validate_raw(&output, &mut transactions, &case.selected_encoded, limit);
    let transaction = transactions
        .begin()
        .expect("begin pipeline cursor oracle transaction");
    assert_eq!(
        cursor
            .access(transaction.access())
            .expect("access pipeline cursor oracle")
            .get()
            .expect("read pipeline cursor oracle"),
        Some(u64::try_from(case.workload.encoded.len()).expect("tail fits u64"))
    );
    pages
}
