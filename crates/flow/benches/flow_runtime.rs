mod support;

use std::{
    io,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::Arc,
    time::Duration,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, CompletionRecord, ConfigurationRecord, DurationSummary,
    EnvironmentRecord, Fields, HostEnvironment, JsonlWriter, LatencySummary, SampleRecord,
    SummaryRecord, require_benchmark_build,
};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

use support::BenchRoot;

const BENCHMARK: &str = "flow_runtime";
const SMOKE_CHAIN_STATIONS: &[usize] = &[3];
const REFERENCE_CHAIN_STATIONS: &[usize] = &[3, 16, 64];
const SMOKE_FANOUTS: &[usize] = &[2];
const REFERENCE_FANOUTS: &[usize] = &[4, 16];
const SMOKE_ROUNDS_PER_SAMPLE: usize = 3;
const REFERENCE_ROUNDS_PER_SAMPLE: usize = 1_024;
const SMOKE_SAMPLES: usize = 1;
const REFERENCE_SAMPLES: usize = 9;
const SMOKE_WARMUP_ROUNDS: usize = 4;
const REFERENCE_WARMUP_ROUNDS: usize = 64;
const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();
const TIGHT_OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(1).unwrap();

struct Config {
    chain_stations: Vec<usize>,
    fanouts: Vec<usize>,
    rounds_per_sample: usize,
    samples: usize,
    warmup_rounds: usize,
}

#[derive(Clone, Copy)]
enum Scenario {
    Sink,
    CapacityPressure,
    Chain { station_count: usize },
    Fanout { consumers: usize },
}

struct Measurement {
    round_latencies: Vec<Duration>,
}

#[derive(Clone, Copy)]
struct WorkCounts {
    advances: usize,
    committed_station_turns: usize,
    input_completions: usize,
}

#[derive(Clone, Copy)]
struct RuntimeMetrics {
    elapsed: Duration,
    latency: LatencySummary,
    work: WorkCounts,
    advances_per_second: u128,
    committed_station_turns_per_second: u128,
    input_completions_per_second: u128,
}

impl Config {
    fn for_profile(profile: BenchmarkProfile) -> Self {
        let (chain_stations, fanouts, rounds_per_sample, samples, warmup_rounds) = match profile {
            BenchmarkProfile::Smoke => (
                SMOKE_CHAIN_STATIONS,
                SMOKE_FANOUTS,
                SMOKE_ROUNDS_PER_SAMPLE,
                SMOKE_SAMPLES,
                SMOKE_WARMUP_ROUNDS,
            ),
            BenchmarkProfile::Reference => (
                REFERENCE_CHAIN_STATIONS,
                REFERENCE_FANOUTS,
                REFERENCE_ROUNDS_PER_SAMPLE,
                REFERENCE_SAMPLES,
                REFERENCE_WARMUP_ROUNDS,
            ),
        };
        assert!(
            chain_stations.windows(2).all(|pair| pair[0] < pair[1]),
            "Flow runtime chain station counts must be strictly increasing"
        );
        assert!(
            chain_stations.iter().all(|count| *count >= 3),
            "Flow runtime chain station counts must contain only counts of at least three"
        );
        assert!(
            fanouts.windows(2).all(|pair| pair[0] < pair[1]),
            "Flow runtime fan-outs must be strictly increasing"
        );
        Self {
            chain_stations: chain_stations.to_vec(),
            fanouts: fanouts.to_vec(),
            rounds_per_sample,
            samples,
            warmup_rounds,
        }
    }

    fn expected_data_records(&self) -> NonZeroUsize {
        let scenarios = 2_usize
            .checked_add(self.chain_stations.len())
            .and_then(|value| value.checked_add(self.fanouts.len()))
            .expect("Flow runtime scenario count fits usize");
        let count = scenarios
            .checked_mul(self.samples + 1)
            .expect("Flow runtime data-record count fits usize");
        NonZeroUsize::new(count).expect("Flow runtime has at least one data record")
    }
}

impl Scenario {
    const fn label(self) -> &'static str {
        match self {
            Self::Sink => "sink_steady",
            Self::CapacityPressure => "capacity_pressure_steady",
            Self::Chain { .. } => "chain_steady",
            Self::Fanout { .. } => "fanout_steady",
        }
    }

    const fn station_count(self) -> usize {
        match self {
            Self::Sink | Self::CapacityPressure => 2,
            Self::Chain { station_count } => station_count,
            Self::Fanout { consumers } => consumers + 1,
        }
    }

    const fn fanout(self) -> usize {
        match self {
            Self::Sink | Self::CapacityPressure | Self::Chain { .. } => 1,
            Self::Fanout { consumers } => consumers,
        }
    }

    const fn topology_name(self) -> &'static str {
        match self {
            Self::Sink | Self::CapacityPressure => "source_sink",
            Self::Chain { .. } => "count_chain",
            Self::Fanout { .. } => "source_fanout_sinks",
        }
    }

    const fn output_capacity_bytes(self) -> NonZeroU64 {
        match self {
            Self::CapacityPressure => TIGHT_OUTPUT_CAPACITY_BYTES,
            Self::Sink | Self::Chain { .. } | Self::Fanout { .. } => OUTPUT_CAPACITY_BYTES,
        }
    }

    const fn capacity_mode(self) -> &'static str {
        if self.is_capacity_pressure() {
            "prefilled_backlog"
        } else {
            "roomy"
        }
    }

    const fn is_capacity_pressure(self) -> bool {
        matches!(self, Self::CapacityPressure)
    }

    const fn committed_station_turns_per_advance(self) -> usize {
        if self.is_capacity_pressure() {
            1
        } else {
            self.station_count()
        }
    }

    const fn input_completions_per_advance(self) -> usize {
        if self.is_capacity_pressure() {
            1
        } else {
            self.station_count() - 1
        }
    }

    fn work_counts(self, advances: usize) -> WorkCounts {
        WorkCounts {
            advances,
            committed_station_turns: advances
                .checked_mul(self.committed_station_turns_per_advance())
                .expect("Flow runtime committed Station turn count fits usize"),
            input_completions: advances
                .checked_mul(self.input_completions_per_advance())
                .expect("Flow runtime input completion count fits usize"),
        }
    }
}

impl Measurement {
    fn elapsed(&self) -> Duration {
        sum_durations(&self.round_latencies)
    }

    fn metrics(&self, scenario: Scenario) -> RuntimeMetrics {
        RuntimeMetrics::new(scenario, &self.round_latencies)
    }
}

impl RuntimeMetrics {
    fn new(scenario: Scenario, round_latencies: &[Duration]) -> Self {
        let elapsed = sum_durations(round_latencies);
        let work = scenario.work_counts(round_latencies.len());
        let latency = LatencySummary::from_samples(round_latencies)
            .expect("summarize Flow runtime round latencies");
        Self {
            elapsed,
            latency,
            work,
            advances_per_second: rate(work.advances, elapsed),
            committed_station_turns_per_second: rate(work.committed_station_turns, elapsed),
            input_completions_per_second: rate(work.input_completions, elapsed),
        }
    }
}

fn main() {
    require_benchmark_build(BENCHMARK);

    let root = BenchRoot::from_environment(BENCHMARK);
    let config = Config::for_profile(root.profile());
    println!("DogPaddle Flow steady runtime benchmark");
    println!(
        "scope=advance steady=chain+fanout+sink+tight_capacity input_retaining_commits_per_change=0 unavailable_input_retaining_commit_profiles=1+8 sync=durable execution=single-thread timing=individual_advance validation=outside-timing"
    );
    emit_environment(&root);
    emit_configuration(&config);

    benchmark_scenario(&root, &config, Scenario::Sink);
    benchmark_scenario(&root, &config, Scenario::CapacityPressure);
    for &station_count in &config.chain_stations {
        benchmark_scenario(&root, &config, Scenario::Chain { station_count });
    }
    for &consumers in &config.fanouts {
        benchmark_scenario(&root, &config, Scenario::Fanout { consumers });
    }
    emit_record(
        &CompletionRecord::new(BENCHMARK)
            .expect("construct Flow runtime benchmark completion record"),
    );
}

fn benchmark_scenario(root: &BenchRoot, config: &Config, scenario: Scenario) {
    let fixture = root.sample(scenario.label());
    let mut flow = scenario_factory(fixture.path(), scenario)
        .build()
        .expect("build Flow runtime benchmark fixture");
    validate_flow(&flow, fixture.path(), scenario);
    if scenario.is_capacity_pressure() {
        drop(flow);
        seed_capacity_backlog(fixture.path(), capacity_backlog_entries(config));
        flow = FlowFactory::open(fixture.path())
            .expect("reopen capacity-pressure runtime benchmark fixture");
    }

    run_rounds(&mut flow, config.warmup_rounds);

    let mut measurements = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let measurement = run_rounds(&mut flow, config.rounds_per_sample);
        emit_sample(scenario, sample, &measurement);
        measurements.push(measurement);
    }
    report(scenario, &measurements);

    validate_flow(&flow, fixture.path(), scenario);
    drop(flow);
    validate_durable_work(fixture.path(), scenario, completed_rounds(config));
    drop(fixture);
}

fn completed_rounds(config: &Config) -> usize {
    let sampled = config
        .rounds_per_sample
        .checked_mul(config.samples)
        .expect("Flow runtime benchmark sampled round count fits usize");
    config
        .warmup_rounds
        .checked_add(sampled)
        .expect("Flow runtime benchmark completed round count fits usize")
}

fn capacity_backlog_entries(config: &Config) -> usize {
    completed_rounds(config)
        .checked_add(1)
        .expect("Flow runtime benchmark capacity backlog count fits usize")
}

fn seed_capacity_backlog(path: &Path, entries: usize) {
    let change = encoded_fixture_change();
    let values = std::iter::repeat_n(change, entries).collect::<Vec<_>>();
    let store = Store::open(path).expect("open Flow Store to seed capacity backlog");
    let output: AppendLog<Vec<u8>> = store
        .open_data("station/00000000/output")
        .expect("open source output to seed capacity backlog");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin capacity backlog seed transaction");
    let offsets = output
        .access(transaction.access())
        .expect("access source output to seed capacity backlog")
        .append_batch(&values)
        .expect("seed source output capacity backlog");
    assert_eq!(
        offsets,
        0..u64::try_from(entries).expect("backlog fits u64")
    );
    transaction
        .commit()
        .expect("commit capacity backlog seed transaction");
}

fn encoded_fixture_change() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![0_u64]))])
        .expect("construct capacity backlog records");
    let change = Change::try_new(records, Int64Array::from(vec![1_i64]))
        .expect("construct capacity backlog Change");
    encode_change(&change).expect("encode capacity backlog Change")
}

fn validate_durable_work(path: &Path, scenario: Scenario, completed_rounds: usize) {
    let completed_rounds =
        u64::try_from(completed_rounds).expect("Flow runtime completed round count fits u64");
    let store = Store::open(path).expect("open Flow Store to validate runtime work counts");
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .expect("open source position to validate runtime work counts");
    let input_states = (1..scenario.station_count())
        .map(|index| {
            store
                .open_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>(&format!(
                    "station/{index:08x}/state"
                ))
                .expect("open Station state to validate runtime work counts")
        })
        .collect::<Vec<_>>();
    let count_states = match scenario {
        Scenario::Chain { station_count } => (1..station_count - 1)
            .map(|index| {
                store
                    .open_data::<Cell<u64>>(&format!("station/{index:08x}/operation/count"))
                    .expect("open Count state to validate runtime work counts")
            })
            .collect::<Vec<_>>(),
        Scenario::Sink | Scenario::CapacityPressure | Scenario::Fanout { .. } => Vec::new(),
    };
    let capacity_output = scenario.is_capacity_pressure().then(|| {
        store
            .open_data::<AppendLog<Vec<u8>>>("station/00000000/output")
            .expect("open source output to validate capacity backlog")
    });
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin runtime work-count validation transaction");
    let source_position = position
        .access(transaction.access())
        .expect("access source position to validate runtime work counts")
        .get()
        .expect("read source position to validate runtime work counts");
    let expected_position = if scenario.is_capacity_pressure() {
        None
    } else {
        Some(
            completed_rounds
                .checked_sub(1)
                .expect("Flow runtime executes at least one round"),
        )
    };
    assert_eq!(
        source_position, expected_position,
        "durable source position must match committed source turns"
    );
    let cursor_key = b"input/00000000/cursor".to_vec();
    for state in &input_states {
        let encoded = state
            .access(transaction.access())
            .expect("access Station state to validate runtime input completions")
            .get(&cursor_key)
            .expect("read Station cursor to validate runtime input completions")
            .expect("runtime input Station has a durable cursor");
        let bytes = <[u8; size_of::<u64>()]>::try_from(encoded.as_slice())
            .expect("runtime input cursor is a big-endian u64");
        assert_eq!(
            u64::from_be_bytes(bytes),
            completed_rounds,
            "durable cursor must match input completions"
        );
    }
    for count in &count_states {
        assert_eq!(
            count
                .access(transaction.access())
                .expect("access Count state to validate committed turns")
                .get()
                .expect("read Count state to validate committed turns"),
            Some(completed_rounds),
            "durable Count state must match committed Count turns"
        );
    }
    if let Some(output) = capacity_output {
        let tail = completed_rounds
            .checked_add(1)
            .expect("Flow runtime capacity backlog tail fits u64");
        assert_eq!(
            output
                .access(transaction.access())
                .expect("access source output to validate capacity backlog")
                .bounds()
                .expect("read source output bounds to validate capacity backlog"),
            completed_rounds..tail,
            "capacity-pressure backlog must retain exactly one entry"
        );
    }
}

fn run_rounds(flow: &mut Flow, rounds: usize) -> Measurement {
    let mut round_latencies = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let started = std::time::Instant::now();
        let outcome = flow.advance();
        let elapsed = started.elapsed();
        assert_eq!(
            outcome.expect("advance runtime benchmark Flow"),
            AdvanceOutcome::Progressed,
            "steady runtime round {round} did not progress"
        );
        round_latencies.push(elapsed);
    }
    Measurement { round_latencies }
}

fn scenario_factory(path: &Path, scenario: Scenario) -> FlowFactory {
    let output_capacity_bytes = scenario.output_capacity_bytes();
    match scenario {
        Scenario::Sink | Scenario::CapacityPressure => sink_factory(path, output_capacity_bytes),
        Scenario::Chain { station_count } => {
            chain_factory(path, station_count, output_capacity_bytes)
        }
        Scenario::Fanout { consumers } => fanout_factory(path, consumers, output_capacity_bytes),
    }
}

fn sink_factory(path: &Path, output_capacity_bytes: NonZeroU64) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, output_capacity_bytes);
    factory.connect([source], sink);
    factory
}

fn chain_factory(
    path: &Path,
    station_count: usize,
    output_capacity_bytes: NonZeroU64,
) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let mut previous = factory.station("source", SequenceSourceDefinition::new(0));
    factory.output_capacity_bytes(previous, output_capacity_bytes);
    for index in 1..station_count - 1 {
        let current = factory.station(format!("count-{index:08x}"), CountDefinition::new());
        factory.output_capacity_bytes(current, output_capacity_bytes);
        factory.connect([previous], current);
        previous = current;
    }
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.connect([previous], sink);
    factory
}

fn fanout_factory(path: &Path, consumers: usize, output_capacity_bytes: NonZeroU64) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    factory.output_capacity_bytes(source, output_capacity_bytes);
    for index in 0..consumers {
        let sink = factory.station(format!("sink-{index:08x}"), DiscardDefinition::new());
        factory.connect([source], sink);
    }
    factory
}

fn validate_flow(flow: &Flow, path: &Path, scenario: Scenario) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), scenario.station_count());
}

fn emit_environment(root: &BenchRoot) {
    let fields = Fields::new()
        .with("mdbx_sync_mode", "durable")
        .expect("add Flow runtime benchmark MDBX sync mode");
    let environment = EnvironmentRecord::new(
        BENCHMARK,
        root.profile(),
        HostEnvironment::collect(Some(root.base()))
            .expect("collect Flow runtime benchmark environment"),
        fields,
    )
    .expect("construct Flow runtime benchmark environment record");
    emit_record(&environment);
}

fn emit_configuration(config: &Config) {
    let fields = Fields::new()
        .with("chain_station_counts", &config.chain_stations)
        .expect("add Flow runtime benchmark chain station counts")
        .with("fanouts", &config.fanouts)
        .expect("add Flow runtime benchmark fan-outs")
        .with("rounds_per_sample", config.rounds_per_sample)
        .expect("add Flow runtime benchmark rounds per sample")
        .with("samples", config.samples)
        .expect("add Flow runtime benchmark sample count")
        .with("warmup_rounds", config.warmup_rounds)
        .expect("add Flow runtime benchmark warmup rounds")
        .with("normal_output_capacity_bytes", OUTPUT_CAPACITY_BYTES.get())
        .expect("add Flow runtime normal output capacity")
        .with(
            "tight_output_capacity_bytes",
            TIGHT_OUTPUT_CAPACITY_BYTES.get(),
        )
        .expect("add Flow runtime tight output capacity")
        .with("input_retaining_commits_per_change_covered", [0_usize])
        .expect("add Flow runtime covered input-retaining Commit profile")
        .with(
            "input_retaining_commits_per_change_unavailable",
            [1_usize, 8],
        )
        .expect("add Flow runtime unavailable input-retaining Commit profiles")
        .with(
            "input_retaining_commit_unavailable_reason",
            "sealed_definition_set_has_no_input_operation_that_returns_commit",
        )
        .expect("add Flow runtime input-retaining Commit coverage boundary")
        .with(
            "input_completion_unit",
            "durable_input_cursor_frontier_advance_fanout_counts_each_edge",
        )
        .expect("add Flow runtime input completion unit")
        .with(
            "committed_station_turn_unit",
            "outer_station_transaction_committed_after_action_pin_and_reclaim_are_not_additional_turns",
        )
        .expect("add Flow runtime committed Station turn unit")
        .with("round_latency_scope", "one_complete_flow_advance_call")
        .expect("add Flow runtime round latency scope")
        .with("round_latency_percentile", "nearest_rank")
        .expect("add Flow runtime round latency percentile convention")
        .with("throughput_rate", "integer_floor_from_timed_advance_ns")
        .expect("add Flow runtime throughput convention")
        .with("sample_elapsed", "sum_of_individually_timed_advances")
        .expect("add Flow runtime elapsed convention")
        .with("raw_round_latencies", "sample_field_round_latencies_ns")
        .expect("add Flow runtime raw round latency convention")
        .with("measurement_protocol", "individual_advance_v2")
        .expect("add Flow runtime measurement protocol")
        .with(
            "comparison_boundary",
            "rerun_all_variants_v2_pre_v2_batch_medians_are_not_comparable",
        )
        .expect("add Flow runtime comparison boundary")
        .with("fixtures", "built_once_outside_timing")
        .expect("add Flow runtime fixture policy")
        .with("validation", "outside_timing")
        .expect("add Flow runtime validation policy");
    let configuration = ConfigurationRecord::new(BENCHMARK, config.expected_data_records(), fields)
        .expect("construct Flow runtime benchmark configuration record");
    emit_record(&configuration);
}

fn emit_sample(scenario: Scenario, sample: usize, measurement: &Measurement) {
    let metrics = measurement.metrics(scenario);
    let round_latencies_ns = measurement
        .round_latencies
        .iter()
        .map(Duration::as_nanos)
        .collect::<Vec<_>>();
    let fields = measurement_fields(scenario, metrics)
        .with("round_latencies_ns", round_latencies_ns)
        .expect("add raw Flow runtime round latencies");
    let sample = SampleRecord::new(BENCHMARK, scenario.label(), sample, metrics.elapsed, fields)
        .expect("construct Flow runtime benchmark sample record");
    emit_record(&sample);
}

fn report(scenario: Scenario, measurements: &[Measurement]) {
    let sample_durations = measurements
        .iter()
        .map(Measurement::elapsed)
        .collect::<Vec<_>>();
    let round_latencies = measurements
        .iter()
        .flat_map(|measurement| measurement.round_latencies.iter().copied())
        .collect::<Vec<_>>();
    let metrics = RuntimeMetrics::new(scenario, &round_latencies);
    let summary = DurationSummary::from_samples(&sample_durations)
        .expect("summarize Flow runtime benchmark samples");
    let min = summary.min();
    let median = summary.median();
    let max = summary.max();
    assert!(!median.is_zero(), "benchmark median must be non-zero");
    println!(
        "{} topology={} stations={} fanout={} advances={} batch_min={min:?} batch_median={median:?} batch_max={max:?} advances/s={} committed_station_turns/s={} input_completions/s={} round_p50={:?} round_p95={:?}",
        scenario.label(),
        scenario.topology_name(),
        scenario.station_count(),
        scenario.fanout(),
        metrics.work.advances,
        metrics.advances_per_second,
        metrics.committed_station_turns_per_second,
        metrics.input_completions_per_second,
        metrics.latency.p50(),
        metrics.latency.p95(),
    );
    let summary = SummaryRecord::new(
        BENCHMARK,
        scenario.label(),
        summary,
        measurement_fields(scenario, metrics),
    )
    .expect("construct Flow runtime benchmark summary record");
    emit_record(&summary);
}

fn scenario_fields(scenario: Scenario) -> Fields {
    Fields::new()
        .with("topology", scenario.topology_name())
        .expect("add Flow runtime topology")
        .with("station_count", scenario.station_count())
        .expect("add Flow runtime station count")
        .with("fanout", scenario.fanout())
        .expect("add Flow runtime fan-out")
        .with(
            "output_capacity_bytes",
            scenario.output_capacity_bytes().get(),
        )
        .expect("add Flow runtime output capacity")
        .with("capacity_mode", scenario.capacity_mode())
        .expect("add Flow runtime capacity mode")
        .with(
            "producer_expected_backpressured",
            scenario.is_capacity_pressure(),
        )
        .expect("add Flow runtime producer pressure expectation")
        .with("expected_outcome", "progressed")
        .expect("add Flow runtime expected outcome")
        .with(
            "input_disposition",
            "complete_only_input_retaining_commit_0",
        )
        .expect("add Flow runtime input disposition")
        .with("input_retaining_commits_per_change", 0)
        .expect("add Flow runtime input-retaining Commit count")
        .with(
            "committed_station_turns_per_advance",
            scenario.committed_station_turns_per_advance(),
        )
        .expect("add Flow runtime committed Station turns per advance")
        .with(
            "input_completions_per_advance",
            scenario.input_completions_per_advance(),
        )
        .expect("add Flow runtime input completions per advance")
}

fn measurement_fields(scenario: Scenario, metrics: RuntimeMetrics) -> Fields {
    scenario_fields(scenario)
        .with("advances", metrics.work.advances)
        .expect("add Flow runtime advance count")
        .with(
            "committed_station_turns",
            metrics.work.committed_station_turns,
        )
        .expect("add Flow runtime committed Station turn count")
        .with("input_completions", metrics.work.input_completions)
        .expect("add Flow runtime input completion count")
        .with("timed_advance_ns_total", metrics.elapsed.as_nanos())
        .expect("add Flow runtime timed advance duration")
        .with("advances_per_second", metrics.advances_per_second)
        .expect("add Flow runtime advance throughput")
        .with(
            "committed_station_turns_per_second",
            metrics.committed_station_turns_per_second,
        )
        .expect("add Flow runtime committed Station turn throughput")
        .with(
            "input_completions_per_second",
            metrics.input_completions_per_second,
        )
        .expect("add Flow runtime input completion throughput")
        .with("round_latency_p50_ns", metrics.latency.p50().as_nanos())
        .expect("add Flow runtime round latency p50")
        .with("round_latency_p95_ns", metrics.latency.p95().as_nanos())
        .expect("add Flow runtime round latency p95")
}

fn sum_durations(durations: &[Duration]) -> Duration {
    durations
        .iter()
        .copied()
        .fold(Duration::ZERO, |total, duration| {
            total
                .checked_add(duration)
                .expect("Flow runtime timed duration fits Duration")
        })
}

fn rate(count: usize, elapsed: Duration) -> u128 {
    assert!(
        !elapsed.is_zero(),
        "Flow runtime throughput requires nonzero elapsed time"
    );
    u128::try_from(count)
        .expect("Flow runtime work count fits u128")
        .checked_mul(1_000_000_000)
        .expect("Flow runtime rate numerator fits u128")
        / elapsed.as_nanos()
}

fn emit_record(record: &impl BenchmarkRecord) {
    let stdout = io::stdout();
    let mut writer = JsonlWriter::new(stdout.lock());
    writer
        .write(record)
        .expect("write Flow runtime benchmark protocol record");
    writer
        .flush()
        .expect("flush Flow runtime benchmark protocol record");
}
