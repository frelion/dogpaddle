use dogpaddle_store::{AppendLog, CodecError, ScanLimit, StoreValue, Transactions};

use crate::{
    CURSOR_KEY, RECORD_HEADER_BYTES,
    fixture::{CdcRecord, LogFixture, decode_header},
};

pub(super) fn assert_stage_cursor(fixture: &mut LogFixture, expected: usize) {
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage validation transaction");
    let cursor = fixture
        .stage_state
        .access(transaction.access())
        .expect("access stage validation state")
        .get(&CURSOR_KEY.to_vec())
        .expect("read stage validation cursor")
        .map(decode_cursor)
        .expect("seeded stage validation cursor");
    assert_eq!(cursor, to_u64(expected));
    transaction
        .commit()
        .expect("finish stage validation transaction");
}

pub(super) fn assert_count_stage(
    fixture: &mut LogFixture,
    expected_cursor: usize,
    expected_count: i64,
) {
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin count stage validation transaction");
    let cursor = fixture
        .stage_state
        .access(transaction.access())
        .expect("access count stage validation state")
        .get(&CURSOR_KEY.to_vec())
        .expect("read count stage validation cursor")
        .map(decode_cursor)
        .expect("seeded count stage validation cursor");
    let count = fixture
        .count
        .access(transaction.access())
        .expect("access count stage validation count")
        .get()
        .expect("read count stage validation count")
        .expect("seeded count stage validation count");
    assert_eq!(cursor, to_u64(expected_cursor));
    assert_eq!(count, expected_count);
    transaction
        .commit()
        .expect("finish count stage validation transaction");
}

pub(super) fn assert_bounds<T: StoreValue>(
    transactions: &mut Transactions,
    log: &AppendLog<T>,
    head: usize,
    tail: usize,
) {
    let transaction = transactions
        .begin()
        .expect("begin log validation transaction");
    let bounds = log
        .access(transaction.access())
        .expect("access validation log")
        .bounds()
        .expect("read validation bounds");
    assert_eq!(bounds, to_u64(head)..to_u64(tail));
    transaction
        .commit()
        .expect("finish log validation transaction");
}

pub(super) fn decode_diff(encoded: &[u8]) -> Result<i64, CodecError> {
    decode_header(encoded).map(|header| header.diff)
}

pub(super) fn decode_key(encoded: &[u8]) -> Result<u64, CodecError> {
    decode_header(encoded).map(|header| header.key)
}

pub(super) fn decode_cursor(encoded: Vec<u8>) -> u64 {
    u64::decode_value(encoded.into()).expect("valid benchmark cursor encoding")
}

pub(super) fn expected_diff_checksum(entries: usize) -> i64 {
    i64::from(!entries.is_multiple_of(2))
}

pub(super) fn expected_full_scan_checksum(entries: usize, record_bytes: usize) -> u64 {
    (0..entries).fold(0_u64, |checksum, index| {
        let key = to_u64(index);
        let fill = if record_bytes == RECORD_HEADER_BYTES {
            0
        } else {
            u8::try_from(key & 0xff).expect("masked payload byte fits in u8")
        };
        checksum.wrapping_add(key).wrapping_add(u64::from(fill))
    })
}

pub(super) fn make_records(entries: usize, record_bytes: usize) -> Vec<CdcRecord> {
    make_records_from(0, entries, record_bytes)
}

pub(super) fn make_records_from(
    start: usize,
    entries: usize,
    record_bytes: usize,
) -> Vec<CdcRecord> {
    let end = start
        .checked_add(entries)
        .expect("benchmark record range fits in usize");
    (start..end)
        .map(|index| CdcRecord::new(index, record_bytes))
        .collect()
}

pub(super) fn chunked_gc_transactions(
    entries: usize,
    batch_items: usize,
    gc_items: usize,
) -> usize {
    (0..entries)
        .step_by(batch_items)
        .map(|start| entries.min(start.saturating_add(batch_items)) - start)
        .map(|batch| batch.div_ceil(gc_items))
        .sum()
}

pub(super) fn scan_limit(record_bytes: usize, batch_items: usize) -> ScanLimit {
    let item_bytes = record_bytes
        .checked_add(size_of::<u64>())
        .expect("benchmark item byte count fits in usize");
    let max_bytes = item_bytes
        .checked_mul(batch_items)
        .expect("benchmark batch byte count fits in usize");
    ScanLimit::new(batch_items, max_bytes).expect("non-zero benchmark scan limit")
}
pub(super) fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark value fits in u64")
}
