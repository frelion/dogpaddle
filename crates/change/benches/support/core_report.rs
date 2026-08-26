use std::hint::black_box;

use crate::support::runner::{
    BenchmarkCase, MachineRecords, Measurement, duration_summary, per_operation,
};

pub(crate) fn print_header(description: &str) {
    println!();
    println!("=== {description} ===");
    println!(
        "{:<40} {:>10} {:>10} {:>12} {:>12} {:>12} {:>13}",
        "workload", "rows/chg", "operations", "min/op", "median/op", "max/op", "rows/s"
    );
}

pub(crate) fn rows(
    case: BenchmarkCase,
    samples: usize,
    records: &mut MachineRecords,
    measure: impl FnMut() -> Measurement,
) {
    report(case, samples, records, measure, true);
}

pub(crate) fn latency(
    case: BenchmarkCase,
    samples: usize,
    records: &mut MachineRecords,
    measure: impl FnMut() -> Measurement,
) {
    report(case, samples, records, measure, false);
}

fn report(
    case: BenchmarkCase,
    samples: usize,
    records: &mut MachineRecords,
    measure: impl FnMut() -> Measurement,
    report_rows_per_second: bool,
) {
    let measurements = collect_samples(samples, measure);
    summarize(case, &measurements, report_rows_per_second);
    records.record(case, &measurements);
}

fn collect_samples(samples: usize, mut measure: impl FnMut() -> Measurement) -> Vec<Measurement> {
    let warm = measure();
    black_box(warm.checksum);
    let mut measurements = Vec::with_capacity(samples);
    for _ in 0..samples {
        let measurement = measure();
        assert_eq!(measurement.checksum, warm.checksum);
        measurements.push(measurement);
    }
    measurements
}

fn summarize(case: BenchmarkCase, measurements: &[Measurement], report_rows_per_second: bool) {
    let summary = duration_summary(measurements);
    let metric = case.metric;
    let rows_per_second = report_rows_per_second.then(|| {
        u128::try_from(metric.rows_per_change)
            .expect("row count fits in u128")
            .checked_mul(u128::try_from(metric.operations).expect("iteration count fits in u128"))
            .and_then(|value| value.checked_mul(1_000_000_000))
            .expect("benchmark row throughput numerator fits in u128")
            / summary.median().as_nanos().max(1)
    });
    let rows_per_second = rows_per_second.map_or_else(|| "-".to_owned(), |value| value.to_string());
    let label = case.label();
    println!(
        "{label:<40} {:>10} {:>10} {:>12} {:>12} {:>12} {rows_per_second:>13}",
        metric.rows_per_change,
        metric.operations,
        per_operation(summary.min(), metric.operations),
        per_operation(summary.median(), metric.operations),
        per_operation(summary.max(), metric.operations),
    );
}
