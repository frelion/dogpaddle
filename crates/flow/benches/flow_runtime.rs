use std::{
    io::{BufWriter, Write},
    num::NonZeroU64,
    path::Path,
    sync::Arc,
    time::Duration,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};
use serde_json::{Value, json};

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

struct AdvanceTrace {
    sample: usize,
    round: usize,
    advance: usize,
    elapsed: Duration,
    outcome: AdvanceOutcome,
}

#[derive(Clone, Copy)]
struct WorkCounts {
    advances: usize,
    committed_station_turns: usize,
    input_completions: usize,
}

struct DurableOracle {
    completed_advances: u64,
    expected_scan_position: Option<u64>,
    scan_position: Option<u64>,
    expected_input_cursor: u64,
    input_cursors: Vec<u64>,
    expected_count_state: Option<u64>,
    count_states: Vec<Option<u64>>,
    expected_capacity_output_bounds: Option<[u64; 2]>,
    capacity_output_bounds: Option<[u64; 2]>,
}

struct OracleResources {
    position: Cell<u64>,
    input_states: Vec<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
    count_states: Vec<Cell<u64>>,
    capacity_output: Option<AppendLog<Vec<u8>>>,
}

struct TraceRun {
    profile: PerformanceProfile,
    root: RunRoot,
    output: BufWriter<std::io::Stdout>,
}

impl Config {
    fn for_profile(profile: PerformanceProfile) -> Self {
        let (chain_stations, fanouts, rounds_per_sample, samples, warmup_rounds) = match profile {
            PerformanceProfile::Smoke => (
                SMOKE_CHAIN_STATIONS,
                SMOKE_FANOUTS,
                SMOKE_ROUNDS_PER_SAMPLE,
                SMOKE_SAMPLES,
                SMOKE_WARMUP_ROUNDS,
            ),
            PerformanceProfile::Reference => (
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

impl OracleResources {
    fn open(store: &Store, scenario: Scenario) -> Self {
        let position = store
            .open_data("station/00000000/operation/sequence_scan.position")
            .expect("open scan position to validate runtime work counts");
        let input_states = (1..scenario.station_count())
            .map(|index| {
                store
                    .open_data(&format!("station/{index:08x}/state"))
                    .expect("open Station state to validate runtime work counts")
            })
            .collect();
        let count_states = match scenario {
            Scenario::Chain { station_count } => (1..station_count - 1)
                .map(|index| {
                    store
                        .open_data(&format!(
                            "station/{index:08x}/operation/running_event_count.count"
                        ))
                        .expect("open RunningEventCount state to validate runtime work counts")
                })
                .collect(),
            Scenario::Sink | Scenario::CapacityPressure | Scenario::Fanout { .. } => Vec::new(),
        };
        let capacity_output = scenario.is_capacity_pressure().then(|| {
            store
                .open_data("station/00000000/output")
                .expect("open scan output to validate capacity backlog")
        });
        Self {
            position,
            input_states,
            count_states,
            capacity_output,
        }
    }
}

impl DurableOracle {
    fn passed(&self) -> bool {
        let counts_match = self.expected_count_state.map_or_else(
            || self.count_states.is_empty(),
            |expected| {
                self.count_states
                    .iter()
                    .all(|count| *count == Some(expected))
            },
        );
        self.scan_position == self.expected_scan_position
            && self
                .input_cursors
                .iter()
                .all(|cursor| *cursor == self.expected_input_cursor)
            && counts_match
            && self.capacity_output_bounds == self.expected_capacity_output_bounds
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
            Self::Sink | Self::CapacityPressure => "scan_sink",
            Self::Chain { .. } => "count_chain",
            Self::Fanout { .. } => "scan_fanout_sinks",
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

impl TraceRun {
    fn new(profile: PerformanceProfile, config: &Config) -> Self {
        require_release_build(BENCHMARK);
        let root = RunRoot::from_environment(BENCHMARK);
        assert_eq!(root.profile(), profile);
        let host = HostEnvironment::collect(Some(root.filesystem_root()));
        let mut run = Self {
            profile,
            root,
            output: BufWriter::new(std::io::stdout()),
        };
        run.emit(&json!({
            "record": "context",
            "benchmark": BENCHMARK,
            "protocol": "flow_runtime_advance_trace_v1",
            "profile": profile,
            "result_directory": run.root.path().display().to_string(),
            "host": host,
            "configuration": configuration(config),
        }));
        run
    }

    const fn root(&self) -> &RunRoot {
        &self.root
    }

    fn trace(&mut self, scenario: Scenario, trace: &AdvanceTrace) {
        let per_advance = scenario.work_counts(1);
        self.emit(&json!({
            "record": "advance",
            "benchmark": BENCHMARK,
            "profile": self.profile,
            "series": scenario.series(),
            "scenario": scenario_context(scenario),
            "phase": "sample",
            "sample": trace.sample,
            "round": trace.round,
            "advance": trace.advance,
            "elapsed_ns": nanos(trace.elapsed),
            "outcome": outcome_label(trace.outcome),
            "oracle": {
                "expected_outcome": "progressed",
                "outcome_matches": trace.outcome == AdvanceOutcome::Progressed,
                "advances": per_advance.advances,
                "committed_station_turns": per_advance.committed_station_turns,
                "input_completions": per_advance.input_completions,
            },
        }));
    }

    fn oracle(&mut self, scenario: Scenario, oracle: &DurableOracle) {
        let scenario_context = scenario_context(scenario);
        let input_station_indices = (1..scenario.station_count()).collect::<Vec<_>>();
        let count_station_indices = match scenario {
            Scenario::Chain { station_count } => (1..station_count - 1).collect::<Vec<_>>(),
            Scenario::Sink | Scenario::CapacityPressure | Scenario::Fanout { .. } => Vec::new(),
        };
        let work = scenario.work_counts(
            usize::try_from(oracle.completed_advances)
                .expect("Flow runtime completed advance count fits usize"),
        );
        self.emit(&json!({
            "record": "oracle",
            "benchmark": BENCHMARK,
            "profile": self.profile,
            "series": scenario.series(),
            "scenario": scenario_context,
            "passed": oracle.passed(),
            "completed_advances": oracle.completed_advances,
            "expected": {
                "scan_position": oracle.expected_scan_position,
                "input_cursor": oracle.expected_input_cursor,
                "count_state": oracle.expected_count_state,
                "capacity_output_bounds": oracle.expected_capacity_output_bounds,
                "advances": work.advances,
                "committed_station_turns": work.committed_station_turns,
                "input_completions": work.input_completions,
            },
            "actual": {
                "scan_position": oracle.scan_position,
                "input_station_indices": input_station_indices,
                "input_cursors": oracle.input_cursors,
                "count_station_indices": count_station_indices,
                "count_states": oracle.count_states,
                "capacity_output_bounds": oracle.capacity_output_bounds,
            },
        }));
    }

    fn finish(mut self) {
        self.emit(&json!({
            "record": "completion",
            "benchmark": BENCHMARK,
            "profile": self.profile,
        }));
        self.output
            .flush()
            .expect("flush Flow runtime performance trace");
    }

    fn emit(&mut self, value: &Value) {
        serde_json::to_writer(&mut self.output, value)
            .expect("serialize Flow runtime performance record");
        self.output
            .write_all(b"\n")
            .expect("write Flow runtime performance record");
        self.output
            .flush()
            .expect("flush Flow runtime performance record");
    }
}

fn main() {
    if !std::env::args_os().any(|argument| argument == "--bench") {
        return;
    }
    let profile = PerformanceProfile::from_environment();
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
    let mut run = TraceRun::new(profile, &config);

    for scenario in scenarios {
        benchmark_scenario(&mut run, &config, scenario);
    }
    run.finish();
}

fn benchmark_scenario(run: &mut TraceRun, config: &Config, scenario: Scenario) {
    let fixture = run.root().sample(scenario.label());
    let path = fixture.path().join("flow");
    let mut flow = scenario_factory(&path, scenario)
        .build()
        .expect("build Flow runtime benchmark fixture");
    validate_flow(&flow, &path, scenario);
    if scenario.is_capacity_pressure() {
        drop(flow);
        seed_capacity_backlog(&path, capacity_backlog_entries(config));
        flow = FlowFactory::new(&path)
            .open()
            .expect("reopen capacity-pressure runtime benchmark fixture");
    }

    let completed = completed_rounds(config);
    warm_up(&mut flow, config.warmup_rounds);
    for sample in 0..config.samples {
        let advance = config
            .warmup_rounds
            .checked_add(
                sample
                    .checked_mul(config.rounds_per_sample)
                    .expect("Flow runtime sample advance offset fits usize"),
            )
            .expect("Flow runtime sampled advance offset fits usize");
        run_rounds(
            run,
            scenario,
            &mut flow,
            config.rounds_per_sample,
            sample,
            advance,
        );
    }

    validate_flow(&flow, &path, scenario);
    drop(flow);
    let oracle = validate_durable_work(&path, scenario, completed);
    run.oracle(scenario, &oracle);
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
        .expect("open scan output to seed capacity backlog");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin capacity backlog seed transaction");
    let offsets = output
        .access(transaction.access())
        .expect("access scan output to seed capacity backlog")
        .append_batch(&values)
        .expect("seed scan output capacity backlog");
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

fn validate_durable_work(
    path: &Path,
    scenario: Scenario,
    completed_rounds: usize,
) -> DurableOracle {
    let completed_rounds =
        u64::try_from(completed_rounds).expect("Flow runtime completed round count fits u64");
    let store = Store::open(path).expect("open Flow Store to validate runtime work counts");
    let resources = OracleResources::open(&store, scenario);
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin runtime work-count validation transaction");
    let scan_position = resources
        .position
        .access(transaction.access())
        .expect("access scan position to validate runtime work counts")
        .get()
        .expect("read scan position to validate runtime work counts");
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
        scan_position, expected_position,
        "durable scan position must match committed scan turns"
    );
    let cursor_key = b"input/00000000/cursor".to_vec();
    let input_cursors = resources
        .input_states
        .iter()
        .map(|state| {
            let encoded = state
                .access(transaction.access())
                .expect("access Station state to validate runtime input completions")
                .get(&cursor_key)
                .expect("read Station cursor to validate runtime input completions")
                .expect("runtime input Station has a durable cursor");
            let bytes = <[u8; size_of::<u64>()]>::try_from(encoded.as_slice())
                .expect("runtime input cursor is a big-endian u64");
            u64::from_be_bytes(bytes)
        })
        .collect::<Vec<_>>();
    assert!(
        input_cursors
            .iter()
            .all(|cursor| *cursor == completed_rounds),
        "every durable cursor must match input completions"
    );
    let count_values = resources
        .count_states
        .iter()
        .map(|count| {
            count
                .access(transaction.access())
                .expect("access RunningEventCount state to validate committed turns")
                .get()
                .expect("read RunningEventCount state to validate committed turns")
        })
        .collect::<Vec<_>>();
    assert!(
        count_values
            .iter()
            .all(|count| *count == Some(completed_rounds)),
        "every durable RunningEventCount state must match committed turns"
    );
    let capacity_output_bounds = resources.capacity_output.map(|output| {
        let bounds = output
            .access(transaction.access())
            .expect("access scan output to validate capacity backlog")
            .bounds()
            .expect("read scan output bounds to validate capacity backlog");
        [bounds.start, bounds.end]
    });
    let expected_capacity_output_bounds = scenario.is_capacity_pressure().then(|| {
        let tail = completed_rounds
            .checked_add(1)
            .expect("Flow runtime capacity backlog tail fits u64");
        [completed_rounds, tail]
    });
    assert_eq!(
        capacity_output_bounds, expected_capacity_output_bounds,
        "capacity-pressure backlog must retain exactly one entry"
    );
    let oracle = DurableOracle {
        completed_advances: completed_rounds,
        expected_scan_position: expected_position,
        scan_position,
        expected_input_cursor: completed_rounds,
        input_cursors,
        expected_count_state: matches!(scenario, Scenario::Chain { .. })
            .then_some(completed_rounds),
        count_states: count_values,
        expected_capacity_output_bounds,
        capacity_output_bounds,
    };
    assert!(oracle.passed(), "Flow runtime durable oracle must pass");
    oracle
}

fn run_rounds(
    run: &mut TraceRun,
    scenario: Scenario,
    flow: &mut Flow,
    rounds: usize,
    sample: usize,
    first_advance: usize,
) {
    for round in 0..rounds {
        let started = std::time::Instant::now();
        let outcome = flow
            .advance()
            .expect("advance Flow runtime benchmark fixture");
        let elapsed = started.elapsed();
        let trace = AdvanceTrace {
            sample,
            round,
            advance: first_advance
                .checked_add(round)
                .expect("Flow runtime advance index fits usize"),
            elapsed,
            outcome,
        };
        run.trace(scenario, &trace);
        assert_eq!(
            outcome,
            AdvanceOutcome::Progressed,
            "steady runtime round {round} did not progress"
        );
    }
}

fn warm_up(flow: &mut Flow, rounds: usize) {
    for round in 0..rounds {
        let outcome = flow
            .advance()
            .expect("warm up Flow runtime benchmark fixture");
        assert_eq!(
            outcome,
            AdvanceOutcome::Progressed,
            "runtime warmup round {round} did not progress"
        );
    }
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
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, output_capacity_bytes);
    factory.connect([scan], sink);
    factory
}

fn chain_factory(
    path: &Path,
    station_count: usize,
    output_capacity_bytes: NonZeroU64,
) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let mut previous = factory.station("scan", SequenceScanDefinition::new(0));
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
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    factory.output_capacity_bytes(scan, output_capacity_bytes);
    for index in 0..consumers {
        let sink = factory.station(format!("sink-{index:08x}"), DiscardDefinition::new());
        factory.connect([scan], sink);
    }
    factory
}

fn validate_flow(flow: &Flow, path: &Path, scenario: Scenario) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), scenario.station_count());
}

fn configuration(config: &Config) -> Value {
    json!({
        "chain_station_counts": config.chain_stations,
        "fanouts": config.fanouts,
        "rounds_per_sample": config.rounds_per_sample,
        "samples": config.samples,
        "warmup_rounds": config.warmup_rounds,
        "normal_output_capacity_bytes": OUTPUT_CAPACITY_BYTES.get(),
        "tight_output_capacity_bytes": TIGHT_OUTPUT_CAPACITY_BYTES.get(),
        "input_retaining_commits_per_change_covered": [0_usize],
        "input_retaining_commits_per_change_unavailable": [1_usize, 8],
        "input_retaining_commit_unavailable_reason":
            "sealed_definition_set_has_no_input_operation_that_returns_commit",
        "input_completion_unit":
            "durable_input_cursor_frontier_advance_fanout_counts_each_edge",
        "committed_station_turn_unit":
            "outer_station_transaction_committed_after_action_pin_and_reclaim_are_not_additional_turns",
        "round_latency_scope": "one_complete_flow_advance_call",
        "raw_round_latencies": "one_advance_record_per_sampled_call",
        "raw_outcomes": "one_advance_record_per_sampled_call",
        "durable_oracle": "one_full_actual_and_expected_record_per_scenario",
        "measurement_protocol": "owner_local_advance_trace_v1",
        "fixtures": "built_once_outside_timing",
        "validation": "outside_timing",
        "execution": "single_thread",
        "mdbx_sync_mode": "durable",
    })
}

fn scenario_context(scenario: Scenario) -> Value {
    json!({
        "topology": scenario.topology_name(),
        "station_count": scenario.station_count(),
        "fanout": scenario.fanout(),
        "output_capacity_bytes": scenario.output_capacity_bytes().get(),
        "capacity_mode": scenario.capacity_mode(),
        "producer_expected_backpressured": scenario.is_capacity_pressure(),
        "expected_outcome": "progressed",
        "input_retaining_commits_per_change": 0,
    })
}

const fn outcome_label(outcome: AdvanceOutcome) -> &'static str {
    match outcome {
        AdvanceOutcome::Idle => "idle",
        AdvanceOutcome::Backpressured => "backpressured",
        AdvanceOutcome::Progressed => "progressed",
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("Flow runtime duration fits u64 nanoseconds")
}
