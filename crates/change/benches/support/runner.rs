use std::{hint::black_box, io, time::Duration};

use dogpaddle_bench_protocol::{
    ConfigurationRecord, DurationSummary, EnvironmentRecord, Fields, HostEnvironment, JsonlWriter,
    SampleRecord, SummaryRecord, positive_usize, positive_usize_list, string_list,
};

use super::fixture::{DEFAULT_WORKLOADS, validate_dimensions};

const DEFAULT_ROWS: &[usize] = &[1, 64, 1_024, 16_384];
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_TARGET_ROWS: usize = 65_536;
const DEFAULT_MAX_CHANGES: usize = 1_024;

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

#[derive(Clone, Copy)]
pub(crate) struct Metric {
    pub(crate) rows_per_change: usize,
    pub(crate) encoded_bytes_per_change: usize,
    pub(crate) operations: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct BenchmarkCase {
    pub(crate) workload: &'static str,
    pub(crate) scenario: &'static str,
    pub(crate) metric: Metric,
}

pub(crate) struct MachineRecords {
    benchmark: &'static str,
    samples: Vec<SampleRecord>,
    summaries: Vec<SummaryRecord>,
}

impl Metric {
    pub(crate) const fn new(
        rows_per_change: usize,
        encoded_bytes_per_change: usize,
        operations: usize,
    ) -> Self {
        assert!(operations > 0, "benchmark operation count must be non-zero");
        Self {
            rows_per_change,
            encoded_bytes_per_change,
            operations,
        }
    }
}

impl BenchmarkCase {
    pub(crate) const fn new(
        workload: &'static str,
        scenario: &'static str,
        metric: Metric,
    ) -> Self {
        Self {
            workload,
            scenario,
            metric,
        }
    }

    pub(crate) fn label(self) -> String {
        format!("{}/{}", self.workload, self.scenario)
    }

    fn fields(self) -> Fields {
        Fields::new()
            .with("workload", self.workload)
            .expect("add Change benchmark workload")
            .with("operations", self.metric.operations)
            .expect("add Change benchmark operation count")
            .with("rows_per_change", self.metric.rows_per_change)
            .expect("add Change benchmark rows per Change")
            .with(
                "encoded_bytes_per_change",
                self.metric.encoded_bytes_per_change,
            )
            .expect("add Change benchmark encoded size")
    }
}

impl Config {
    pub(crate) fn load() -> Self {
        let rows = positive_usize_list("DOGPADDLE_BENCH_CHANGE_ROWS", DEFAULT_ROWS)
            .expect("read Change benchmark row counts");
        let payload_bytes = positive_usize(
            "DOGPADDLE_BENCH_CHANGE_PAYLOAD_BYTES",
            DEFAULT_PAYLOAD_BYTES,
        )
        .expect("read Change benchmark payload size");
        let samples = positive_usize("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES)
            .expect("read Change benchmark sample count");
        let target_rows = positive_usize("DOGPADDLE_BENCH_CHANGE_TARGET_ROWS", DEFAULT_TARGET_ROWS)
            .expect("read Change benchmark target row count");
        let max_changes = positive_usize("DOGPADDLE_BENCH_CHANGE_MAX_CHANGES", DEFAULT_MAX_CHANGES)
            .expect("read Change benchmark maximum Change count");
        let workloads = string_list("DOGPADDLE_BENCH_CHANGE_WORKLOADS", DEFAULT_WORKLOADS)
            .expect("read Change benchmark workloads");
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

    pub(crate) fn print(&self, benchmark: &'static str, title: &str) {
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
        let environment = EnvironmentRecord::new(
            benchmark,
            HostEnvironment::collect(None).expect("collect Change benchmark environment"),
            Fields::new(),
        )
        .expect("construct Change benchmark environment record");
        let configuration = ConfigurationRecord::new(
            benchmark,
            Fields::new()
                .with("rows_per_change", &self.rows)
                .expect("add Change benchmark row counts")
                .with("target_rows_per_sample", self.target_rows)
                .expect("add Change benchmark target row count")
                .with("max_changes_per_sample", self.max_changes)
                .expect("add Change benchmark maximum Change count")
                .with("samples", self.samples)
                .expect("add Change benchmark sample count")
                .with("payload_bytes", self.payload_bytes)
                .expect("add Change benchmark payload size")
                .with("workloads", &self.workloads)
                .expect("add Change benchmark workloads")
                .with("execution", "single_thread")
                .expect("add Change benchmark execution policy")
                .with("cache", "warm")
                .expect("add Change benchmark cache policy")
                .with("setup", "outside_timing")
                .expect("add Change benchmark setup policy")
                .with("validation", "outside_timing")
                .expect("add Change benchmark validation policy"),
        )
        .expect("construct Change benchmark configuration record");
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        writer
            .write(&environment)
            .expect("write Change benchmark environment record");
        writer
            .write(&configuration)
            .expect("write Change benchmark configuration record");
        writer
            .flush()
            .expect("flush Change benchmark protocol records");
    }
}

impl MachineRecords {
    pub(crate) const fn new(benchmark: &'static str) -> Self {
        Self {
            benchmark,
            samples: Vec::new(),
            summaries: Vec::new(),
        }
    }

    pub(crate) fn print(&self) {
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        for sample in &self.samples {
            writer
                .write(sample)
                .expect("write Change benchmark sample record");
        }
        for summary in &self.summaries {
            writer
                .write(summary)
                .expect("write Change benchmark summary record");
        }
        writer
            .flush()
            .expect("flush Change benchmark protocol records");
    }

    pub(crate) fn record(&mut self, case: BenchmarkCase, measurements: &[Measurement]) {
        let fields = case.fields();
        for (sample, measurement) in measurements.iter().enumerate() {
            self.samples.push(
                SampleRecord::new(
                    self.benchmark,
                    case.scenario,
                    sample,
                    measurement.elapsed,
                    fields.clone(),
                )
                .expect("construct Change benchmark sample record"),
            );
        }
        self.summaries.push(
            SummaryRecord::new(
                self.benchmark,
                case.scenario,
                duration_summary(measurements),
                fields,
            )
            .expect("construct Change benchmark summary record"),
        );
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

pub(crate) fn duration_summary(measurements: &[Measurement]) -> DurationSummary {
    let durations = measurements
        .iter()
        .map(|measurement| measurement.elapsed)
        .collect::<Vec<_>>();
    DurationSummary::from_samples(&durations).expect("summarize Change benchmark measurements")
}

pub(crate) fn per_operation(total: Duration, operations: usize) -> String {
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
