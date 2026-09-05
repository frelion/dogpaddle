//! Criterion measurements for Flow's durable build and warm-open lifecycle.

use std::{
    fs,
    num::NonZeroU64,
    path::Path,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, SamplingMode};
use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "flow_lifecycle";
const SMOKE_STATION_COUNTS: &[usize] = &[2, 3];
const REFERENCE_STATION_COUNTS: &[usize] = &[2, 64, 1_024];
const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();

#[derive(Clone, Copy)]
struct Config {
    station_counts: &'static [usize],
    sample_size: usize,
    warm_up_time: Duration,
    measurement_time: Duration,
}

impl Config {
    const fn for_profile(profile: PerformanceProfile) -> Self {
        match profile {
            PerformanceProfile::Smoke => Self {
                station_counts: SMOKE_STATION_COUNTS,
                sample_size: 10,
                warm_up_time: Duration::from_millis(20),
                measurement_time: Duration::from_secs(1),
            },
            PerformanceProfile::Reference => Self {
                station_counts: REFERENCE_STATION_COUNTS,
                sample_size: 30,
                warm_up_time: Duration::from_secs(2),
                measurement_time: Duration::from_secs(5),
            },
        }
    }

    fn validate(self) {
        assert!(
            self.station_counts.windows(2).all(|pair| pair[0] < pair[1]),
            "Flow lifecycle Station counts must be strictly increasing"
        );
        assert!(
            self.station_counts.iter().all(|count| *count >= 2),
            "Flow lifecycle requires a Source and Sink"
        );
    }
}

struct LifecycleRun {
    root: RunRoot,
}

impl LifecycleRun {
    fn new(profile: PerformanceProfile, config: Config) -> Self {
        if is_cargo_bench() {
            require_release_build(BENCHMARK);
        }
        let root = RunRoot::for_profile(BENCHMARK, profile);
        let context = json!({
            "benchmark": BENCHMARK,
            "runner": "criterion",
            "criterion_version": "0.8.2",
            "profile": profile,
            "result_directory": root.path().display().to_string(),
            "host": HostEnvironment::collect(Some(root.filesystem_root())),
            "configuration": {
                "station_counts": config.station_counts,
                "sample_size": config.sample_size,
                "sampling_mode": "flat",
                "warm_up_time_ns": nanos(config.warm_up_time),
                "measurement_time_ns": nanos(config.measurement_time),
                "output_capacity_bytes": OUTPUT_CAPACITY_BYTES.get(),
                "scenarios": ["fresh_durable_build", "warm_reopen"],
                "timing_scope": "FlowFactory::build_or_FlowFactory::open_only",
                "fixture_and_validation": "outside_timing",
                "fresh_build_oracle": "validate_then_reopen_and_validate_outside_timing",
                "execution": "single_thread",
                "mdbx_sync_mode": "durable",
            },
        });
        let encoded = serde_json::to_vec_pretty(&context)
            .expect("serialize Flow lifecycle Criterion context");
        fs::write(root.path().join("criterion-context.json"), encoded)
            .expect("write Flow lifecycle Criterion context");
        Self { root }
    }

    fn sample(&self, scenario: &str) -> TempDir {
        self.root.sample(scenario)
    }
}

fn criterion_configuration(config: Config, output_directory: &Path) -> Criterion {
    Criterion::default()
        .sample_size(config.sample_size)
        .warm_up_time(config.warm_up_time)
        .measurement_time(config.measurement_time)
        .without_plots()
        .output_directory(output_directory)
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    let config = Config::for_profile(profile);
    config.validate();
    let run = LifecycleRun::new(profile, config);
    let mut criterion = criterion_configuration(config, run.root.path()).configure_from_args();
    lifecycle(&mut criterion, &run, config);
    criterion.final_summary();
}

fn lifecycle(criterion: &mut Criterion, run: &LifecycleRun, config: Config) {
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.sampling_mode(SamplingMode::Flat);

    for &station_count in config.station_counts {
        group.bench_function(
            BenchmarkId::new("fresh_durable_build", station_count),
            |bencher| {
                bencher
                    .iter_custom(|iterations| measure_fresh_build(run, station_count, iterations));
            },
        );

        let fixture = run.sample(&format!("warm-reopen-{station_count}"));
        let path = fixture.path().join("flow");
        let flow = linear_factory(&path, station_count)
            .build()
            .expect("build warm-reopen benchmark fixture");
        validate_flow(&flow, &path, station_count);
        drop(flow);
        group.bench_function(BenchmarkId::new("warm_reopen", station_count), |bencher| {
            bencher.iter_custom(|iterations| measure_reopen(&path, station_count, iterations));
        });
    }

    group.finish();
}

fn measure_fresh_build(run: &LifecycleRun, station_count: usize, iterations: u64) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let fixture = run.sample(&format!("fresh-build-{station_count}"));
        let path = fixture.path().join("flow");
        let factory = linear_factory(&path, station_count);
        let started = Instant::now();
        let flow = factory.build().expect("build benchmark Flow");
        elapsed = checked_add(elapsed, started.elapsed());
        validate_flow(&flow, &path, station_count);
        drop(flow);
        let reopened = FlowFactory::new(&path)
            .open()
            .expect("reopen freshly built benchmark Flow");
        validate_flow(&reopened, &path, station_count);
    }
    elapsed
}

fn measure_reopen(path: &Path, station_count: usize, iterations: u64) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let factory = FlowFactory::new(path);
        let started = Instant::now();
        let flow = factory.open().expect("open benchmark Flow");
        elapsed = checked_add(elapsed, started.elapsed());
        validate_flow(&flow, path, station_count);
    }
    elapsed
}

fn linear_factory(path: &Path, station_count: usize) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let mut previous = factory.station("scan", SequenceScanDefinition::new(0));
    factory.output_capacity_bytes(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..station_count - 1 {
        let current = factory.station(
            format!("count-{index:08x}"),
            RunningEventCountDefinition::new(),
        );
        factory.output_capacity_bytes(current, OUTPUT_CAPACITY_BYTES);
        factory.connect([previous], current);
        previous = current;
    }
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.connect([previous], sink);
    factory
}

fn validate_flow(flow: &Flow, path: &Path, station_count: usize) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), station_count);
    let mut ids = flow.station_ids();
    assert_eq!(ids.next(), Some("scan"));
    for index in 1..station_count - 1 {
        let expected = format!("count-{index:08x}");
        assert_eq!(ids.next(), Some(expected.as_str()));
    }
    assert_eq!(ids.next(), Some("sink"));
    assert_eq!(ids.next(), None);
}

fn is_cargo_bench() -> bool {
    std::env::args_os().any(|argument| argument == "--bench")
}

fn checked_add(total: Duration, elapsed: Duration) -> Duration {
    total
        .checked_add(elapsed)
        .expect("Flow lifecycle measured duration fits Duration")
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("Flow lifecycle duration fits u64 nanoseconds")
}
