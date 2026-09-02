use std::{hint::black_box, io, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, CompletionRecord, ConfigurationRecord, DurationSummary, EnvironmentRecord,
    Fields, HostEnvironment, JsonlWriter, SampleRecord,
};

use super::fixture::{DEFAULT_WORKLOADS, validate_dimensions};

const SMOKE_ROWS: &[usize] = &[4];
const REFERENCE_ROWS: &[usize] = &[1, 64, 1_024, 16_384];

pub(crate) struct Config {
    profile: BenchmarkProfile,
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

    fn series(self) -> String {
        format!(
            "{}/{}/rows={}/encoded_bytes={}/operations={}",
            self.workload,
            self.scenario,
            self.metric.rows_per_change,
            self.metric.encoded_bytes_per_change,
            self.metric.operations
        )
    }

    fn fields(self) -> Fields {
        Fields::new()
            .with("workload", self.workload)
            .with("operations", self.metric.operations)
            .with("rows_per_change", self.metric.rows_per_change)
            .with(
                "encoded_bytes_per_change",
                self.metric.encoded_bytes_per_change,
            )
    }
}

impl Config {
    pub(crate) fn load() -> Self {
        let profile = BenchmarkProfile::from_environment();
        Self::for_profile(profile)
    }

    fn for_profile(profile: BenchmarkProfile) -> Self {
        let (rows, payload_bytes, samples, target_rows, max_changes) = match profile {
            BenchmarkProfile::Smoke => (SMOKE_ROWS, 16, 1, 4, 1),
            BenchmarkProfile::Reference => (REFERENCE_ROWS, 1_024, 9, 65_536, 1_024),
        };
        let rows = rows.to_vec();
        let workloads = DEFAULT_WORKLOADS
            .iter()
            .map(|workload| (*workload).to_owned())
            .collect::<Vec<_>>();
        for &rows in &rows {
            validate_dimensions(rows, payload_bytes, &workloads);
        }
        rows.len()
            .checked_mul(workloads.len())
            .and_then(|value| value.checked_mul(samples))
            .expect("configured benchmark sample count fits usize");
        Self {
            profile,
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

    pub(crate) fn print(&self, benchmark: &'static str, title: &str, scenarios_per_fixture: usize) {
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
            self.profile,
            HostEnvironment::collect(None),
            Fields::new(),
        );
        let configuration = ConfigurationRecord::new(
            benchmark,
            self.expected_samples(scenarios_per_fixture),
            Fields::new()
                .with("rows_per_change", &self.rows)
                .with("target_rows_per_sample", self.target_rows)
                .with("max_changes_per_sample", self.max_changes)
                .with("samples", self.samples)
                .with("payload_bytes", self.payload_bytes)
                .with("workloads", &self.workloads)
                .with("execution", "single_thread")
                .with("cache", "warm")
                .with("setup", "outside_timing")
                .with("validation", "outside_timing"),
        );
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        writer.write(&environment);
        writer.write(&configuration);
        writer.flush();
    }

    fn expected_samples(&self, scenarios_per_fixture: usize) -> NonZeroUsize {
        let count = self
            .rows
            .len()
            .checked_mul(self.workloads.len())
            .and_then(|value| value.checked_mul(scenarios_per_fixture))
            .and_then(|value| value.checked_mul(self.samples))
            .expect("Change benchmark sample count fits usize");
        NonZeroUsize::new(count).expect("Change benchmark has at least one data record")
    }
}

impl MachineRecords {
    pub(crate) const fn new(benchmark: &'static str) -> Self {
        Self {
            benchmark,
            samples: Vec::new(),
        }
    }

    pub(crate) fn print(&self) {
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        for sample in &self.samples {
            writer.write(sample);
        }
        writer.write(&CompletionRecord::new(self.benchmark));
        writer.flush();
    }

    pub(crate) fn record(&mut self, case: BenchmarkCase, measurements: &[Measurement]) {
        let fields = case.fields();
        let series = case.series();
        for (sample, measurement) in measurements.iter().enumerate() {
            self.samples.push(SampleRecord::new(
                self.benchmark,
                &series,
                sample,
                measurement.elapsed,
                fields.clone(),
            ));
        }
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
    DurationSummary::from_samples(&durations)
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
