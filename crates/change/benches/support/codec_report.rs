use crate::support::runner::{
    BenchmarkCase, MachineRecords, Measurement, duration_summary, per_operation,
};

const MEBIBYTE_BYTES: u128 = 1_048_576;

pub(crate) fn print_header(description: &str) {
    println!();
    println!("=== {description} ===");
    println!(
        "{:<40} {:>10} {:>10} {:>12} {:>12} {:>12} {:>12} {:>13} {:>14}",
        "workload",
        "rows/chg",
        "changes",
        "encoded B",
        "min/chg",
        "median/chg",
        "max/chg",
        "rows/s",
        "encoded MiB/s"
    );
}

pub(crate) fn measurements(
    case: BenchmarkCase,
    measurements: &[Measurement],
    records: &mut MachineRecords,
) {
    summarize(case, measurements);
    records.record(case, measurements);
}

fn summarize(case: BenchmarkCase, measurements: &[Measurement]) {
    let summary = duration_summary(measurements);
    let metric = case.metric;
    let elapsed_nanos = summary.median().as_nanos().max(1);
    let iterations = u128::try_from(metric.operations).expect("iteration count fits in u128");
    let rows = u128::try_from(metric.rows_per_change).expect("row count fits in u128");
    let encoded_bytes =
        u128::try_from(metric.encoded_bytes_per_change).expect("encoded byte count fits in u128");
    let rows_per_second = rows
        .checked_mul(iterations)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .expect("benchmark row throughput numerator fits in u128")
        / elapsed_nanos;
    let encoded_mib_tenths_per_second = encoded_bytes
        .checked_mul(iterations)
        .and_then(|value| value.checked_mul(10_000_000_000))
        .expect("benchmark byte throughput numerator fits in u128")
        / elapsed_nanos
        / MEBIBYTE_BYTES;
    let label = case.label();
    println!(
        "{label:<40} {:>10} {:>10} {:>12} {:>12} {:>12} {:>12} {rows_per_second:>13} {:>11}.{:01}",
        metric.rows_per_change,
        metric.operations,
        metric.encoded_bytes_per_change,
        per_operation(summary.min(), metric.operations),
        per_operation(summary.median(), metric.operations),
        per_operation(summary.max(), metric.operations),
        encoded_mib_tenths_per_second / 10,
        encoded_mib_tenths_per_second % 10,
    );
}
