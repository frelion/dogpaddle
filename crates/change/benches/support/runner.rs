use std::{hint::black_box, process::Command, time::Duration};

use super::fixture::{DEFAULT_WORKLOADS, validate_dimensions};

const DEFAULT_ROWS: &[usize] = &[1, 64, 1_024, 16_384];
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_TARGET_ROWS: usize = 65_536;
const DEFAULT_MAX_CHANGES: usize = 1_024;
#[allow(dead_code)]
const MEBIBYTE_BYTES: u128 = 1_048_576;

pub(crate) struct Config {
    pub(crate) rows: Vec<usize>,
    pub(crate) payload_bytes: usize,
    pub(crate) samples: usize,
    pub(crate) target_rows: usize,
    pub(crate) max_changes: usize,
    pub(crate) workloads: Vec<String>,
}

#[derive(Clone, Copy)]
pub(crate) struct Measurement {
    pub(crate) elapsed: Duration,
    pub(crate) checksum: u64,
}

pub(crate) struct SampleRecord {
    workload: &'static str,
    scenario: &'static str,
    sample: usize,
    elapsed: Duration,
    operations: usize,
    rows_per_change: usize,
    encoded_bytes_per_change: usize,
}

impl Config {
    pub(crate) fn load() -> Self {
        let rows = setting_list("DOGPADDLE_BENCH_CHANGE_ROWS", DEFAULT_ROWS);
        let payload_bytes = setting(
            "DOGPADDLE_BENCH_CHANGE_PAYLOAD_BYTES",
            DEFAULT_PAYLOAD_BYTES,
        );
        let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
        let target_rows = setting("DOGPADDLE_BENCH_CHANGE_TARGET_ROWS", DEFAULT_TARGET_ROWS);
        let max_changes = setting("DOGPADDLE_BENCH_CHANGE_MAX_CHANGES", DEFAULT_MAX_CHANGES);
        let workloads = string_list("DOGPADDLE_BENCH_CHANGE_WORKLOADS", DEFAULT_WORKLOADS);
        assert!(rows.iter().all(|rows| *rows > 0));
        assert!(payload_bytes > 0);
        assert!(samples > 0 && target_rows > 0 && max_changes > 0);
        assert!(!workloads.is_empty());
        for workload in &workloads {
            assert!(
                DEFAULT_WORKLOADS.contains(&workload.as_str()),
                "unknown Change benchmark workload {workload:?}"
            );
        }
        for &rows in &rows {
            validate_dimensions(rows, payload_bytes, &workloads);
        }
        rows.len()
            .checked_mul(workloads.len())
            .and_then(|value| value.checked_mul(samples))
            .expect("configured benchmark sample count fits usize");
        Self {
            rows,
            payload_bytes,
            samples,
            target_rows,
            max_changes,
            workloads,
        }
    }

    pub(crate) fn iterations(&self, rows: usize) -> usize {
        self.target_rows.div_ceil(rows).clamp(1, self.max_changes)
    }

    pub(crate) fn print(&self, title: &str) {
        println!("{title}");
        println!(
            "rows/change={:?} target_rows/sample={} max_changes/sample={} samples={} payload_bytes={} workloads={:?}",
            self.rows,
            self.target_rows,
            self.max_changes,
            self.samples,
            self.payload_bytes,
            self.workloads
        );
        println!(
            "execution=single-thread cache=warm setup=outside-timing validation=outside-timing"
        );
        print_environment();
    }
}

pub(crate) fn timed(iterations: usize, mut operation: impl FnMut() -> u64) -> Measurement {
    let mut checksum = 0_u64;
    let started = std::time::Instant::now();
    for _ in 0..iterations {
        checksum = checksum.wrapping_add(operation());
    }
    black_box(checksum);
    Measurement {
        elapsed: started.elapsed(),
        checksum,
    }
}

#[allow(dead_code)]
pub(crate) fn print_core_header(description: &str) {
    println!();
    println!("=== {description} ===");
    println!(
        "{:<40} {:>10} {:>10} {:>12} {:>12} {:>12} {:>13}",
        "workload", "rows/chg", "operations", "min/op", "median/op", "max/op", "rows/s"
    );
}

#[allow(dead_code)]
pub(crate) fn print_codec_header(description: &str) {
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

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) fn report_rows(
    workload: &'static str,
    scenario: &'static str,
    rows: usize,
    encoded_bytes: usize,
    iterations: usize,
    samples: usize,
    records: &mut Vec<SampleRecord>,
    measure: impl FnMut() -> Measurement,
) {
    let measurements = collect_samples(iterations, samples, measure);
    summarize_core(workload, scenario, rows, iterations, &measurements, true);
    record_samples(
        records,
        workload,
        scenario,
        rows,
        encoded_bytes,
        iterations,
        &measurements,
    );
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) fn report_latency(
    workload: &'static str,
    scenario: &'static str,
    rows: usize,
    encoded_bytes: usize,
    iterations: usize,
    samples: usize,
    records: &mut Vec<SampleRecord>,
    measure: impl FnMut() -> Measurement,
) {
    let measurements = collect_samples(iterations, samples, measure);
    summarize_core(workload, scenario, rows, iterations, &measurements, false);
    record_samples(
        records,
        workload,
        scenario,
        rows,
        encoded_bytes,
        iterations,
        &measurements,
    );
}

#[allow(dead_code)]
pub(crate) fn summarize_codec(
    workload: &str,
    scenario: &str,
    rows: usize,
    encoded_bytes: usize,
    iterations: usize,
    measurements: &[Measurement],
) {
    let sorted = sorted_measurements(measurements);
    let min = sorted[0].elapsed;
    let median = sorted[sorted.len() / 2].elapsed;
    let max = sorted[sorted.len() - 1].elapsed;
    let elapsed_nanos = median.as_nanos().max(1);
    let iterations_u128 = u128::try_from(iterations).expect("iteration count fits in u128");
    let rows_u128 = u128::try_from(rows).expect("row count fits in u128");
    let bytes_u128 = u128::try_from(encoded_bytes).expect("encoded byte count fits in u128");
    let rows_per_second = rows_u128
        .checked_mul(iterations_u128)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .expect("benchmark row throughput numerator fits in u128")
        / elapsed_nanos;
    let encoded_mib_tenths_per_second = bytes_u128
        .checked_mul(iterations_u128)
        .and_then(|value| value.checked_mul(10_000_000_000))
        .expect("benchmark byte throughput numerator fits in u128")
        / elapsed_nanos
        / MEBIBYTE_BYTES;
    let label = format!("{workload}/{scenario}");
    println!(
        "{label:<40} {rows:>10} {iterations:>10} {encoded_bytes:>12} {:>12} {:>12} {:>12} {rows_per_second:>13} {:>11}.{:01}",
        per_operation(min, iterations),
        per_operation(median, iterations),
        per_operation(max, iterations),
        encoded_mib_tenths_per_second / 10,
        encoded_mib_tenths_per_second % 10,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_samples(
    records: &mut Vec<SampleRecord>,
    workload: &'static str,
    scenario: &'static str,
    rows: usize,
    encoded_bytes: usize,
    iterations: usize,
    measurements: &[Measurement],
) {
    records.extend(
        measurements
            .iter()
            .enumerate()
            .map(|(sample, measurement)| SampleRecord {
                workload,
                scenario,
                sample,
                elapsed: measurement.elapsed,
                operations: iterations,
                rows_per_change: rows,
                encoded_bytes_per_change: encoded_bytes,
            }),
    );
}

pub(crate) fn print_sample_csv(records: &[SampleRecord]) {
    println!();
    println!("=== machine-readable sample CSV ===");
    println!(
        "workload,scenario,sample,elapsed_ns,operations,rows_per_change,encoded_bytes_per_change"
    );
    for record in records {
        println!(
            "{},{},{},{},{},{},{}",
            record.workload,
            record.scenario,
            record.sample,
            record.elapsed.as_nanos(),
            record.operations,
            record.rows_per_change,
            record.encoded_bytes_per_change
        );
    }
    println!("=== end machine-readable sample CSV ===");
}

#[allow(dead_code)]
fn collect_samples(
    iterations: usize,
    samples: usize,
    mut measure: impl FnMut() -> Measurement,
) -> Vec<Measurement> {
    let warm = measure();
    black_box(warm.checksum);
    let mut measurements = Vec::with_capacity(samples);
    for _ in 0..samples {
        let measurement = measure();
        assert_eq!(measurement.checksum, warm.checksum);
        measurements.push(measurement);
    }
    assert!(iterations > 0);
    measurements
}

#[allow(dead_code)]
fn summarize_core(
    workload: &str,
    scenario: &str,
    rows: usize,
    iterations: usize,
    measurements: &[Measurement],
    report_rows_per_second: bool,
) {
    let sorted = sorted_measurements(measurements);
    let min = sorted[0].elapsed;
    let median = sorted[sorted.len() / 2].elapsed;
    let max = sorted[sorted.len() - 1].elapsed;
    let rows_per_second = report_rows_per_second.then(|| {
        u128::try_from(rows)
            .expect("row count fits in u128")
            .checked_mul(u128::try_from(iterations).expect("iteration count fits in u128"))
            .and_then(|value| value.checked_mul(1_000_000_000))
            .expect("benchmark row throughput numerator fits in u128")
            / median.as_nanos().max(1)
    });
    let rows_per_second = rows_per_second.map_or_else(|| "-".to_owned(), |value| value.to_string());
    let label = format!("{workload}/{scenario}");
    println!(
        "{label:<40} {rows:>10} {iterations:>10} {:>12} {:>12} {:>12} {rows_per_second:>13}",
        per_operation(min, iterations),
        per_operation(median, iterations),
        per_operation(max, iterations),
    );
}

fn sorted_measurements(measurements: &[Measurement]) -> Vec<Measurement> {
    assert!(!measurements.is_empty());
    let mut sorted = measurements.to_vec();
    sorted.sort_unstable_by_key(|measurement| measurement.elapsed);
    sorted
}

fn per_operation(total: Duration, operations: usize) -> String {
    let nanos =
        total.as_nanos() / u128::try_from(operations).expect("operation count fits in u128");
    duration(Duration::from_nanos(
        u64::try_from(nanos).expect("average benchmark duration fits in u64 nanoseconds"),
    ))
}

fn duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}

fn print_environment() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let (cargo_profile, cargo_profile_source) = cargo_profile();
    println!(
        "environment rustc={:?} cargo_profile={cargo_profile:?} cargo_profile_source={cargo_profile_source} debug_assertions={} os={} arch={} cpu={:?} kernel={:?} git_revision={:?} git_dirty={}",
        command_output(&rustc, &["--version"]),
        cfg!(debug_assertions),
        std::env::consts::OS,
        std::env::consts::ARCH,
        cpu_description(),
        command_output("uname", &["-sr"]),
        command_output("git", &["rev-parse", "HEAD"]),
        git_dirty()
    );
}

fn cargo_profile() -> (String, &'static str) {
    match std::env::var("DOGPADDLE_CARGO_PROFILE") {
        Ok(profile) => {
            assert!(
                !profile.is_empty() && profile.trim() == profile,
                "DOGPADDLE_CARGO_PROFILE must be a non-empty Cargo profile name without surrounding whitespace"
            );
            (profile, "environment")
        }
        Err(std::env::VarError::NotPresent) => ("bench".to_owned(), "default"),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("DOGPADDLE_CARGO_PROFILE must be valid Unicode")
        }
    }
}

fn cpu_description() -> String {
    if std::env::consts::OS == "macos" {
        let description = command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
        if description != "unavailable" {
            return description;
        }
    }
    if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo")
        && let Some(description) = cpuinfo.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            matches!(key.trim(), "model name" | "Hardware").then(|| value.trim().to_owned())
        })
    {
        return description;
    }
    "unavailable".to_owned()
}

fn git_dirty() -> &'static str {
    Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or("unavailable", |output| {
            if output.stdout.is_empty() {
                "false"
            } else {
                "true"
            }
        })
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|output| output.trim().to_owned())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("benchmark setting must be an integer")
    })
}

fn setting_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            let parsed = value
                .split(',')
                .map(str::trim)
                .map(|item| {
                    item.parse::<usize>()
                        .expect("benchmark list setting must contain integers")
                })
                .collect::<Vec<_>>();
            assert!(!parsed.is_empty(), "benchmark list setting cannot be empty");
            parsed
        },
    )
}

fn string_list(name: &str, default: &[&str]) -> Vec<String> {
    std::env::var(name).map_or_else(
        |_| default.iter().map(ToString::to_string).collect(),
        |value| {
            let parsed = value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            assert!(!parsed.is_empty(), "benchmark list setting cannot be empty");
            parsed
        },
    )
}
