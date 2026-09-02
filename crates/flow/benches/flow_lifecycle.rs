mod support;

use std::{
    io,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    time::Duration,
};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, CompletionRecord, ConfigurationRecord, DurationSummary,
    EnvironmentRecord, Fields, HostEnvironment, JsonlWriter, SampleRecord, SummaryRecord,
    require_benchmark_build,
};
use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};

use support::BenchRoot;

const BENCHMARK: &str = "flow_lifecycle";
const SMOKE_STATION_COUNTS: &[usize] = &[2, 3];
const REFERENCE_STATION_COUNTS: &[usize] = &[2, 64, 1_024];
const SMOKE_SAMPLES: usize = 1;
const REFERENCE_SAMPLES: usize = 9;
const SMOKE_WARMUPS: usize = 1;
const REFERENCE_WARMUPS: usize = 2;
const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();

struct Config {
    station_counts: Vec<usize>,
    samples: usize,
    warmups: usize,
}

impl Config {
    fn for_profile(profile: BenchmarkProfile) -> Self {
        let (station_counts, samples, warmups) = match profile {
            BenchmarkProfile::Smoke => (SMOKE_STATION_COUNTS, SMOKE_SAMPLES, SMOKE_WARMUPS),
            BenchmarkProfile::Reference => (
                REFERENCE_STATION_COUNTS,
                REFERENCE_SAMPLES,
                REFERENCE_WARMUPS,
            ),
        };
        assert!(
            station_counts.windows(2).all(|pair| pair[0] < pair[1]),
            "Flow benchmark station counts must be strictly increasing"
        );
        assert!(
            station_counts.iter().all(|count| *count >= 2),
            "Flow benchmark station counts must contain only counts of at least two"
        );
        Self {
            station_counts: station_counts.to_vec(),
            samples,
            warmups,
        }
    }

    fn expected_data_records(&self) -> NonZeroUsize {
        let count = self
            .station_counts
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_mul(self.samples + 1))
            .expect("Flow lifecycle data-record count fits usize");
        NonZeroUsize::new(count).expect("Flow lifecycle has at least one data record")
    }
}

fn main() {
    require_benchmark_build(BENCHMARK);

    let root = BenchRoot::from_environment(BENCHMARK);
    let config = Config::for_profile(root.profile());
    println!("DogPaddle Flow lifecycle benchmark");
    println!(
        "scope=build/open runtime=excluded sync=durable execution=single-thread validation=outside-timing"
    );
    emit_environment(&root);
    emit_configuration(&config);

    for &station_count in &config.station_counts {
        benchmark_fresh_build(&root, &config, station_count);
        benchmark_warm_reopen(&root, &config, station_count);
    }
    emit_record(
        &CompletionRecord::new(BENCHMARK).expect("construct Flow benchmark completion record"),
    );
}

fn benchmark_fresh_build(root: &BenchRoot, config: &Config, station_count: usize) {
    for _ in 0..config.warmups {
        measure_fresh_build(root, station_count);
    }

    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = measure_fresh_build(root, station_count);
        emit_sample("fresh_durable_build", station_count, sample, elapsed);
        durations.push(elapsed);
    }
    report("fresh_durable_build", station_count, &durations);
}

fn measure_fresh_build(root: &BenchRoot, station_count: usize) -> Duration {
    let sample = root.sample("flow-build");
    let factory = linear_factory(sample.path(), station_count);

    let started = std::time::Instant::now();
    let flow = factory.build().expect("build benchmark Flow");
    let elapsed = started.elapsed();

    validate_flow(&flow, sample.path(), station_count);
    drop(flow);
    let reopened = FlowFactory::open(sample.path()).expect("reopen freshly built benchmark Flow");
    validate_flow(&reopened, sample.path(), station_count);
    drop(reopened);
    drop(sample);
    elapsed
}

fn benchmark_warm_reopen(root: &BenchRoot, config: &Config, station_count: usize) {
    let fixture = root.sample("flow-reopen");
    let flow = linear_factory(fixture.path(), station_count)
        .build()
        .expect("build reopen benchmark fixture");
    validate_flow(&flow, fixture.path(), station_count);
    drop(flow);

    let preflight = FlowFactory::open(fixture.path()).expect("preflight reopen benchmark fixture");
    validate_flow(&preflight, fixture.path(), station_count);
    drop(preflight);
    for _ in 0..config.warmups {
        measure_reopen(fixture.path(), station_count);
    }

    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = measure_reopen(fixture.path(), station_count);
        emit_sample("warm_reopen", station_count, sample, elapsed);
        durations.push(elapsed);
    }
    report("warm_reopen", station_count, &durations);
}

fn measure_reopen(path: &Path, station_count: usize) -> Duration {
    let started = std::time::Instant::now();
    let flow = FlowFactory::open(path).expect("open benchmark Flow");
    let elapsed = started.elapsed();

    validate_flow(&flow, path, station_count);
    drop(flow);
    elapsed
}

fn linear_factory(path: &Path, station_count: usize) -> FlowFactory {
    assert!(
        station_count >= 2,
        "benchmark Flow must contain a source and sink"
    );
    let mut factory = FlowFactory::new(path);
    let mut previous = factory.station("source", SequenceSourceDefinition::new(0));
    factory.output_capacity_bytes(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..station_count - 1 {
        let current = factory.station(format!("count-{index:08x}"), CountDefinition::new());
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
    assert_eq!(ids.next(), Some("source"));
    for index in 1..station_count - 1 {
        let expected = format!("count-{index:08x}");
        assert_eq!(ids.next(), Some(expected.as_str()));
    }
    assert_eq!(ids.next(), Some("sink"));
    assert_eq!(ids.next(), None);
}

fn emit_environment(root: &BenchRoot) {
    let fields = Fields::new()
        .with("mdbx_sync_mode", "durable")
        .expect("add Flow benchmark MDBX sync mode");
    let environment = EnvironmentRecord::new(
        BENCHMARK,
        root.profile(),
        HostEnvironment::collect(Some(root.base())).expect("collect Flow benchmark environment"),
        fields,
    )
    .expect("construct Flow benchmark environment record");
    emit_record(&environment);
}

fn emit_configuration(config: &Config) {
    let fields = Fields::new()
        .with("station_counts", &config.station_counts)
        .expect("add Flow benchmark station counts")
        .with("samples", config.samples)
        .expect("add Flow benchmark sample count")
        .with("warmups", config.warmups)
        .expect("add Flow benchmark warmup count")
        .with("fresh_build_path_and_factory", "outside_timing")
        .expect("add Flow fresh-build setup policy")
        .with("fresh_build_store_per_sample", true)
        .expect("add Flow fresh-build store policy")
        .with("reopen_fixture_and_warmup", "outside_timing")
        .expect("add Flow reopen setup policy")
        .with("reopen_cache", "warm_committed")
        .expect("add Flow reopen cache policy")
        .with("validation", "outside_timing")
        .expect("add Flow benchmark validation policy");
    let configuration = ConfigurationRecord::new(BENCHMARK, config.expected_data_records(), fields)
        .expect("construct Flow benchmark configuration record");
    emit_record(&configuration);
}

fn emit_sample(scenario: &'static str, station_count: usize, sample: usize, elapsed: Duration) {
    let sample = SampleRecord::new(
        BENCHMARK,
        scenario,
        sample,
        elapsed,
        station_fields(station_count),
    )
    .expect("construct Flow benchmark sample record");
    emit_record(&sample);
}

fn report(scenario: &'static str, station_count: usize, samples: &[Duration]) {
    let summary = DurationSummary::from_samples(samples).expect("summarize Flow benchmark samples");
    let min = summary.min();
    let median = summary.median();
    let max = summary.max();
    assert!(!median.is_zero(), "benchmark median must be non-zero");
    println!("{scenario} stations={station_count}: min={min:?} median={median:?} max={max:?}");
    let summary = SummaryRecord::new(BENCHMARK, scenario, summary, station_fields(station_count))
        .expect("construct Flow benchmark summary record");
    emit_record(&summary);
}

fn station_fields(station_count: usize) -> Fields {
    Fields::new()
        .with("station_count", station_count)
        .expect("add Flow benchmark station count")
}

fn emit_record(record: &impl BenchmarkRecord) {
    let stdout = io::stdout();
    let mut writer = JsonlWriter::new(stdout.lock());
    writer
        .write(record)
        .expect("write Flow benchmark protocol record");
    writer
        .flush()
        .expect("flush Flow benchmark protocol record");
}
