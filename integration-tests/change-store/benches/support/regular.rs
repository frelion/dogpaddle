use std::time::Duration;

use dogpaddle_bench_protocol::{
    ConfigurationRecord, DurationSummary, Fields, SampleRecord, SummaryRecord,
};
use dogpaddle_change::{Change, ChangeProjection, decode_change_projected};
use dogpaddle_store::CodecError as StoreCodecError;

use crate::support::{BenchStoreRoot, emit_host_environment, emit_record};

#[derive(Clone, Copy)]
pub(crate) struct SampleWork {
    pub(crate) transactions: usize,
    pub(crate) rows: usize,
    pub(crate) changes: usize,
    pub(crate) encoded_bytes: usize,
    pub(crate) logical_bytes: usize,
}

pub(crate) fn checked_product(label: &str, left: usize, right: usize) -> usize {
    left.checked_mul(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

pub(crate) fn checked_sum(label: &str, left: usize, right: usize) -> usize {
    left.checked_add(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn emit_environment(
    root: &BenchStoreRoot,
    rows_per_change: usize,
    changes_per_transaction: usize,
    payload_bytes: usize,
    samples: usize,
    warmups: usize,
    max_working_set_bytes: usize,
) {
    emit_host_environment(root, "change_append_log");
    let mut fields = Fields::new();
    fields
        .insert("rows_per_change", rows_per_change)
        .expect("encode rows per Change");
    fields
        .insert("changes_per_transaction", changes_per_transaction)
        .expect("encode Changes per transaction");
    fields
        .insert("payload_bytes", payload_bytes)
        .expect("encode payload bytes");
    fields
        .insert("samples", samples)
        .expect("encode sample count");
    fields
        .insert("warmups", warmups)
        .expect("encode warmup count");
    fields
        .insert("max_working_set_bytes", max_working_set_bytes)
        .expect("encode working-set limit");
    emit_record(
        &ConfigurationRecord::new("change_append_log", fields)
            .expect("build Change + Store configuration record"),
    );
}

pub(crate) fn emit_sample(scenario: &str, sample: usize, elapsed: Duration, work: SampleWork) {
    assert!(
        work.transactions > 0,
        "sample transaction count must be non-zero"
    );
    let mut fields = work_fields(work);
    fields
        .insert("rows_per_transaction", work.rows / work.transactions)
        .expect("encode rows per transaction");
    fields
        .insert("changes_per_transaction", work.changes / work.transactions)
        .expect("encode Changes per transaction");
    fields
        .insert(
            "encoded_bytes_per_transaction",
            work.encoded_bytes / work.transactions,
        )
        .expect("encode encoded bytes per transaction");
    fields
        .insert(
            "bytes_per_transaction",
            work.logical_bytes / work.transactions,
        )
        .expect("encode logical bytes per transaction");
    emit_record(
        &SampleRecord::new("change_append_log", scenario, sample, elapsed, fields)
            .expect("build Change + Store sample record"),
    );
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn report(label: &str, samples: &[Duration], work: SampleWork) {
    assert!(
        work.transactions > 0,
        "sample transaction count must be non-zero"
    );
    let summary = DurationSummary::from_samples(samples).expect("summarize benchmark durations");
    assert!(
        !summary.median().is_zero(),
        "benchmark median must be non-zero"
    );
    let seconds = summary.median().as_secs_f64();
    let rows_per_second = work.rows as f64 / seconds;
    let changes_per_second = work.changes as f64 / seconds;
    let encoded_mebibytes_per_second = work.encoded_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let mebibytes_per_second = work.logical_bytes as f64 / (1024.0 * 1024.0) / seconds;
    println!(
        "{label}: min={:?} median={:?} max={:?} rows/s={rows_per_second:.0} changes/s={changes_per_second:.0} encoded_MiB/s={encoded_mebibytes_per_second:.2} logical_MiB/s={mebibytes_per_second:.2}",
        summary.min(),
        summary.median(),
        summary.max(),
    );
    emit_record(
        &SummaryRecord::new("change_append_log", label, summary, work_fields(work))
            .expect("build Change + Store summary record"),
    );
}

fn work_fields(work: SampleWork) -> Fields {
    let mut fields = Fields::new();
    fields
        .insert("transactions", work.transactions)
        .expect("encode transaction count");
    fields.insert("rows", work.rows).expect("encode row count");
    fields
        .insert("changes", work.changes)
        .expect("encode Change count");
    fields
        .insert("encoded_bytes", work.encoded_bytes)
        .expect("encode encoded bytes");
    fields
        .insert("logical_bytes", work.logical_bytes)
        .expect("encode logical bytes");
    fields
}
