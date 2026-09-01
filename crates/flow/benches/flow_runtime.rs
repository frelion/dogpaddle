mod support;

use std::{io, num::NonZeroU64, path::Path, sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, ConfigurationRecord, DurationSummary, EnvironmentRecord,
    Fields, HostEnvironment, JsonlWriter, SampleRecord, SummaryRecord, positive_usize,
    positive_usize_list, require_benchmark_build,
};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, Store};

use support::BenchRoot;

const BENCHMARK: &str = "flow_runtime";
const SMOKE_CHAIN_STATIONS: &[usize] = &[3, 8];
const REFERENCE_CHAIN_STATIONS: &[usize] = &[3, 8, 32];
const SMOKE_FANOUTS: &[usize] = &[1, 4];
const REFERENCE_FANOUTS: &[usize] = &[1, 4, 16];
const SMOKE_ROUNDS_PER_SAMPLE: usize = 32;
const REFERENCE_ROUNDS_PER_SAMPLE: usize = 1_024;
const SMOKE_SAMPLES: usize = 3;
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
enum Topology {
    Sink,
    Chain { station_count: usize },
    Fanout { consumers: usize },
}

#[derive(Clone, Copy)]
struct Scenario {
    label: &'static str,
    topology: Topology,
    output_capacity_bytes: NonZeroU64,
    capacity_mode: &'static str,
    prefill_capacity_backlog: bool,
}

#[derive(Clone, Copy, Default)]
struct OutcomeCounts {
    idle: usize,
    backpressured: usize,
    progressed: usize,
}

impl Config {
    fn load(profile: BenchmarkProfile) -> Self {
        let (
            default_chain_stations,
            default_fanouts,
            default_rounds_per_sample,
            default_samples,
            default_warmup_rounds,
        ) = match profile {
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
        let chain_stations = positive_usize_list(
            "DOGPADDLE_FLOW_RUNTIME_BENCH_CHAIN_STATIONS",
            default_chain_stations,
        )
        .expect("read Flow runtime benchmark chain station counts");
        let fanouts = positive_usize_list("DOGPADDLE_FLOW_RUNTIME_BENCH_FANOUTS", default_fanouts)
            .expect("read Flow runtime benchmark fan-outs");
        let rounds_per_sample = positive_usize(
            "DOGPADDLE_FLOW_RUNTIME_BENCH_ROUNDS_PER_SAMPLE",
            default_rounds_per_sample,
        )
        .expect("read Flow runtime benchmark rounds per sample");
        let samples = positive_usize("DOGPADDLE_FLOW_RUNTIME_BENCH_SAMPLES", default_samples)
            .expect("read Flow runtime benchmark sample count");
        let warmup_rounds = positive_usize(
            "DOGPADDLE_FLOW_RUNTIME_BENCH_WARMUP_ROUNDS",
            default_warmup_rounds,
        )
        .expect("read Flow runtime benchmark warmup rounds");
        assert!(
            chain_stations.windows(2).all(|pair| pair[0] < pair[1]),
            "DOGPADDLE_FLOW_RUNTIME_BENCH_CHAIN_STATIONS must be strictly increasing"
        );
        assert!(
            chain_stations.iter().all(|count| *count >= 3),
            "DOGPADDLE_FLOW_RUNTIME_BENCH_CHAIN_STATIONS must contain only counts of at least three"
        );
        assert!(
            fanouts.windows(2).all(|pair| pair[0] < pair[1]),
            "DOGPADDLE_FLOW_RUNTIME_BENCH_FANOUTS must be strictly increasing"
        );
        Self {
            chain_stations,
            fanouts,
            rounds_per_sample,
            samples,
            warmup_rounds,
        }
    }
}

impl Topology {
    const fn station_count(self) -> usize {
        match self {
            Self::Sink => 2,
            Self::Chain { station_count } => station_count,
            Self::Fanout { consumers } => consumers + 1,
        }
    }

    const fn fanout(self) -> usize {
        match self {
            Self::Sink | Self::Chain { .. } => 1,
            Self::Fanout { consumers } => consumers,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Sink => "source_sink",
            Self::Chain { .. } => "count_chain",
            Self::Fanout { .. } => "source_fanout_sinks",
        }
    }
}

impl OutcomeCounts {
    fn observe(&mut self, outcome: AdvanceOutcome) {
        match outcome {
            AdvanceOutcome::Idle => self.idle += 1,
            AdvanceOutcome::Backpressured => self.backpressured += 1,
            AdvanceOutcome::Progressed => self.progressed += 1,
        }
    }

    fn validate_all_progressed(self, rounds: usize) {
        assert_eq!(self.idle, 0, "steady runtime unexpectedly became idle");
        assert_eq!(
            self.backpressured, 0,
            "steady runtime unexpectedly reported backpressure"
        );
        assert_eq!(self.progressed, rounds);
    }
}

fn main() {
    require_benchmark_build(BENCHMARK);

    let root = BenchRoot::from_environment();
    let config = Config::load(root.profile());
    println!("DogPaddle Flow steady runtime benchmark");
    println!(
        "scope=advance steady=chain+fanout+sink+tight_capacity keep_heavy=unavailable_public_builtin sync=durable execution=single-thread validation=outside-timing"
    );
    emit_environment(&root);
    emit_configuration(&config);

    benchmark_scenario(
        &root,
        &config,
        Scenario {
            label: "sink_steady",
            topology: Topology::Sink,
            output_capacity_bytes: OUTPUT_CAPACITY_BYTES,
            capacity_mode: "roomy",
            prefill_capacity_backlog: false,
        },
    );
    benchmark_scenario(
        &root,
        &config,
        Scenario {
            label: "capacity_pressure_steady",
            topology: Topology::Sink,
            output_capacity_bytes: TIGHT_OUTPUT_CAPACITY_BYTES,
            capacity_mode: "prefilled_backlog",
            prefill_capacity_backlog: true,
        },
    );
    for &station_count in &config.chain_stations {
        benchmark_scenario(
            &root,
            &config,
            Scenario {
                label: "chain_steady",
                topology: Topology::Chain { station_count },
                output_capacity_bytes: OUTPUT_CAPACITY_BYTES,
                capacity_mode: "roomy",
                prefill_capacity_backlog: false,
            },
        );
    }
    for &consumers in &config.fanouts {
        benchmark_scenario(
            &root,
            &config,
            Scenario {
                label: "fanout_steady",
                topology: Topology::Fanout { consumers },
                output_capacity_bytes: OUTPUT_CAPACITY_BYTES,
                capacity_mode: "roomy",
                prefill_capacity_backlog: false,
            },
        );
    }
}

fn benchmark_scenario(root: &BenchRoot, config: &Config, scenario: Scenario) {
    let fixture = root.sample(scenario.label);
    let mut flow = scenario_factory(fixture.path(), scenario)
        .build()
        .expect("build Flow runtime benchmark fixture");
    validate_flow(&flow, fixture.path(), scenario);
    if scenario.prefill_capacity_backlog {
        drop(flow);
        seed_capacity_backlog(fixture.path(), measured_rounds(config));
        flow = FlowFactory::open(fixture.path())
            .expect("reopen capacity-pressure runtime benchmark fixture");
    }

    let (_, warmup_outcomes) = run_rounds(&mut flow, config.warmup_rounds);
    warmup_outcomes.validate_all_progressed(config.warmup_rounds);

    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let (elapsed, outcomes) = run_rounds(&mut flow, config.rounds_per_sample);
        outcomes.validate_all_progressed(config.rounds_per_sample);
        emit_sample(scenario, config.rounds_per_sample, sample, elapsed);
        durations.push(elapsed);
    }
    report(scenario, config.rounds_per_sample, &durations);

    validate_flow(&flow, fixture.path(), scenario);
    drop(flow);
    if scenario.prefill_capacity_backlog {
        validate_capacity_source_rolled_back(fixture.path());
    }
    drop(fixture);
}

fn measured_rounds(config: &Config) -> usize {
    let sampled = config
        .rounds_per_sample
        .checked_mul(config.samples)
        .expect("Flow runtime benchmark sampled round count fits usize");
    config
        .warmup_rounds
        .checked_add(sampled)
        .and_then(|rounds| rounds.checked_add(1))
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

fn validate_capacity_source_rolled_back(path: &Path) {
    let store = Store::open(path).expect("open Flow Store to validate capacity pressure");
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .expect("open source position to validate capacity pressure");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin capacity-pressure validation transaction");
    assert_eq!(
        position
            .access(transaction.access())
            .expect("access source position to validate capacity pressure")
            .get()
            .expect("read source position to validate capacity pressure"),
        None,
        "capacity-pressure source position must roll back every rejected output"
    );
}

fn run_rounds(flow: &mut Flow, rounds: usize) -> (Duration, OutcomeCounts) {
    let mut outcomes = OutcomeCounts::default();
    let started = std::time::Instant::now();
    for _ in 0..rounds {
        outcomes.observe(flow.advance().expect("advance runtime benchmark Flow"));
    }
    (started.elapsed(), outcomes)
}

fn scenario_factory(path: &Path, scenario: Scenario) -> FlowFactory {
    match scenario.topology {
        Topology::Sink => sink_factory(path, scenario.output_capacity_bytes),
        Topology::Chain { station_count } => chain_factory(path, station_count),
        Topology::Fanout { consumers } => fanout_factory(path, consumers),
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

fn chain_factory(path: &Path, station_count: usize) -> FlowFactory {
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

fn fanout_factory(path: &Path, consumers: usize) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    factory.output_capacity_bytes(source, OUTPUT_CAPACITY_BYTES);
    for index in 0..consumers {
        let sink = factory.station(format!("sink-{index:08x}"), DiscardDefinition::new());
        factory.connect([source], sink);
    }
    factory
}

fn validate_flow(flow: &Flow, path: &Path, scenario: Scenario) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), scenario.topology.station_count());
}

fn emit_environment(root: &BenchRoot) {
    let fields = Fields::new()
        .with("mdbx_sync_mode", "durable")
        .expect("add Flow runtime benchmark MDBX sync mode");
    let environment = EnvironmentRecord::for_profile(
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
        .with("keep_heavy", "unavailable_public_builtin")
        .expect("add Flow runtime Keep coverage boundary")
        .with("fixtures", "built_once_outside_timing")
        .expect("add Flow runtime fixture policy")
        .with("validation", "outside_timing")
        .expect("add Flow runtime validation policy");
    let configuration = ConfigurationRecord::new(BENCHMARK, fields)
        .expect("construct Flow runtime benchmark configuration record");
    emit_record(&configuration);
}

fn emit_sample(scenario: Scenario, rounds: usize, sample: usize, elapsed: Duration) {
    let sample = SampleRecord::new(
        BENCHMARK,
        scenario.label,
        sample,
        elapsed,
        scenario_fields(scenario, rounds),
    )
    .expect("construct Flow runtime benchmark sample record");
    emit_record(&sample);
}

fn report(scenario: Scenario, rounds: usize, samples: &[Duration]) {
    let summary =
        DurationSummary::from_samples(samples).expect("summarize Flow runtime benchmark samples");
    let min = summary.min();
    let median = summary.median();
    let max = summary.max();
    assert!(!median.is_zero(), "benchmark median must be non-zero");
    println!(
        "{} topology={} stations={} fanout={} rounds={rounds}: min={min:?} median={median:?} max={max:?}",
        scenario.label,
        scenario.topology.name(),
        scenario.topology.station_count(),
        scenario.topology.fanout(),
    );
    let summary = SummaryRecord::new(
        BENCHMARK,
        scenario.label,
        summary,
        scenario_fields(scenario, rounds),
    )
    .expect("construct Flow runtime benchmark summary record");
    emit_record(&summary);
}

fn scenario_fields(scenario: Scenario, rounds: usize) -> Fields {
    Fields::new()
        .with("topology", scenario.topology.name())
        .expect("add Flow runtime topology")
        .with("station_count", scenario.topology.station_count())
        .expect("add Flow runtime station count")
        .with("fanout", scenario.topology.fanout())
        .expect("add Flow runtime fan-out")
        .with(
            "output_capacity_bytes",
            scenario.output_capacity_bytes.get(),
        )
        .expect("add Flow runtime output capacity")
        .with("capacity_mode", scenario.capacity_mode)
        .expect("add Flow runtime capacity mode")
        .with(
            "producer_expected_backpressured",
            scenario.prefill_capacity_backlog,
        )
        .expect("add Flow runtime producer pressure expectation")
        .with("rounds", rounds)
        .expect("add Flow runtime rounds")
        .with("expected_outcome", "progressed")
        .expect("add Flow runtime expected outcome")
        .with("input_disposition", "complete_only")
        .expect("add Flow runtime input disposition")
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
