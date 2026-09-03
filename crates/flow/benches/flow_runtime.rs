use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::Arc,
    time::Duration,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_bench_protocol::{
    BenchmarkProfile, CaseId, CaseSpec, Fields, Measurement, Plan, Run,
};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition,
    transform::RunningEventCountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

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

struct RoundMeasurement {
    round_latencies: Vec<Duration>,
}

#[derive(Clone, Copy)]
struct WorkCounts {
    advances: usize,
    committed_station_turns: usize,
    input_completions: usize,
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

    fn series(self) -> String {
        format!(
            "{}/topology={}/stations={}/fanout={}/capacity={}",
            self.label(),
            self.topology_name(),
            self.station_count(),
            self.fanout(),
            self.output_capacity_bytes()
        )
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

impl RoundMeasurement {
    fn elapsed(&self) -> Duration {
        sum_durations(&self.round_latencies)
    }
}

fn main() {
    let profile = BenchmarkProfile::from_environment();
    let config = Config::for_profile(profile);
    let scenarios = std::iter::once(Scenario::Sink)
        .chain(std::iter::once(Scenario::CapacityPressure))
        .chain(
            config
                .chain_stations
                .iter()
                .map(|&station_count| Scenario::Chain { station_count }),
        )
        .chain(
            config
                .fanouts
                .iter()
                .map(|&consumers| Scenario::Fanout { consumers }),
        )
        .collect::<Vec<_>>();
    let mut plan = Plan::new(profile, configuration_fields(&config));
    let cases = scenarios
        .into_iter()
        .map(|scenario| {
            let work = scenario.work_counts(config.rounds_per_sample);
            let case = plan.case(CaseSpec::new(
                scenario.series(),
                NonZeroUsize::new(config.samples).expect("Flow runtime has samples"),
                scenario_fields(scenario)
                    .with("advances", work.advances)
                    .with("committed_station_turns", work.committed_station_turns)
                    .with("input_completions", work.input_completions),
            ));
            (scenario, case)
        })
        .collect::<Vec<_>>();
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }

    for (scenario, case) in cases {
        benchmark_scenario(&mut run, &config, scenario, case);
    }
    run.finish(|| {});
}

fn benchmark_scenario(run: &mut Run, config: &Config, scenario: Scenario, case: CaseId) {
    let fixture = run.sample(scenario.label());
    let path = fixture.path().join("flow");
    let mut flow = scenario_factory(&path, scenario)
        .build()
        .expect("build Flow runtime benchmark fixture");
    validate_flow(&flow, &path, scenario);
    if scenario.is_capacity_pressure() {
        drop(flow);
        seed_capacity_backlog(&path, capacity_backlog_entries(config));
        flow =
            FlowFactory::open(&path).expect("reopen capacity-pressure runtime benchmark fixture");
    }

    run_rounds(&mut flow, config.warmup_rounds);

    for _ in 0..config.samples {
        let measurement = run_rounds(&mut flow, config.rounds_per_sample);
        let round_latencies_ns = measurement
            .round_latencies
            .iter()
            .map(Duration::as_nanos)
            .collect::<Vec<_>>();
        run.push(
            case,
            Measurement::with_fields(
                measurement.elapsed(),
                Fields::new().with("round_latencies_ns", round_latencies_ns),
            ),
        );
    }

    validate_flow(&flow, &path, scenario);
    drop(flow);
    validate_durable_work(&path, scenario, completed_rounds(config));
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
                    .open_data::<Cell<u64>>(&format!(
                        "station/{index:08x}/operation/running_event_count.count"
                    ))
                    .expect("open RunningEventCount state to validate runtime work counts")
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
                .expect("access RunningEventCount state to validate committed turns")
                .get()
                .expect("read RunningEventCount state to validate committed turns"),
            Some(completed_rounds),
            "durable RunningEventCount state must match committed turns"
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

fn run_rounds(flow: &mut Flow, rounds: usize) -> RoundMeasurement {
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
    RoundMeasurement { round_latencies }
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
        let current = factory.station(
            format!("count-{index:08x}"),
            RunningEventCountDefinition::new(),
        );
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

fn configuration_fields(config: &Config) -> Fields {
    Fields::new()
        .with("chain_station_counts", &config.chain_stations)
        .with("fanouts", &config.fanouts)
        .with("rounds_per_sample", config.rounds_per_sample)
        .with("samples", config.samples)
        .with("warmup_rounds", config.warmup_rounds)
        .with("normal_output_capacity_bytes", OUTPUT_CAPACITY_BYTES.get())
        .with(
            "tight_output_capacity_bytes",
            TIGHT_OUTPUT_CAPACITY_BYTES.get(),
        )
        .with("input_retaining_commits_per_change_covered", [0_usize])
        .with(
            "input_retaining_commits_per_change_unavailable",
            [1_usize, 8],
        )
        .with(
            "input_retaining_commit_unavailable_reason",
            "sealed_definition_set_has_no_input_operation_that_returns_commit",
        )
        .with(
            "input_completion_unit",
            "durable_input_cursor_frontier_advance_fanout_counts_each_edge",
        )
        .with(
            "committed_station_turn_unit",
            "outer_station_transaction_committed_after_action_pin_and_reclaim_are_not_additional_turns",
        )
        .with("round_latency_scope", "one_complete_flow_advance_call")
        .with("round_latency_percentile", "nearest_rank")
        .with("throughput_rate", "integer_floor_from_timed_advance_ns")
        .with("sample_elapsed", "sum_of_individually_timed_advances")
        .with("raw_round_latencies", "sample_field_round_latencies_ns")
        .with("measurement_protocol", "individual_advance_v2")
        .with(
            "comparison_boundary",
            "rerun_all_variants_v2_pre_v2_batch_medians_are_not_comparable",
        )
        .with("fixtures", "built_once_outside_timing")
        .with("validation", "outside_timing")
        .with("execution", "single_thread")
        .with("mdbx_sync_mode", "durable")
}

fn scenario_fields(scenario: Scenario) -> Fields {
    Fields::new()
        .with("topology", scenario.topology_name())
        .with("station_count", scenario.station_count())
        .with("fanout", scenario.fanout())
        .with(
            "output_capacity_bytes",
            scenario.output_capacity_bytes().get(),
        )
        .with("capacity_mode", scenario.capacity_mode())
        .with(
            "producer_expected_backpressured",
            scenario.is_capacity_pressure(),
        )
        .with("expected_outcome", "progressed")
        .with("input_retaining_commits_per_change", 0)
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
