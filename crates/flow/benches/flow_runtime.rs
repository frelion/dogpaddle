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
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, RunningEventCountDefinition,
            SchemaAlignDefinition, SchemaAlignField, SelectDefinition,
        },
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Cell, Store, SubscribedLog, SubscribedLogWriter, Subscription};
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
const PURE_CHAIN_LOGICAL_OPERATION_COUNT: usize = 7;
const PURE_CHAIN_TRANSFORM_COUNT: usize = 5;

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
    UnfusedPureChain,
    FusedPureChain,
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
    expected_input_position: u64,
    input_positions: Vec<u64>,
    expected_count_state: Option<u64>,
    count_states: Vec<Option<u64>>,
    expected_output_tails: Vec<u64>,
    output_tails: Vec<u64>,
    output_retained_bytes: Vec<u64>,
    expected_ipc_change_appends: u64,
    ipc_change_appends: u64,
    expected_capacity_output_bounds: Option<[u64; 2]>,
    capacity_output_bounds: Option<[u64; 2]>,
}

struct OracleResources {
    position: Cell<u64>,
    input_subscriptions: Vec<Subscription<Vec<u8>>>,
    count_states: Vec<Cell<u64>>,
    outputs: Vec<SubscribedLogWriter<Vec<u8>>>,
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
        let input_subscriptions = match scenario {
            Scenario::Sink | Scenario::CapacityPressure => {
                vec![open_subscription(store, 0, 0)]
            }
            Scenario::UnfusedPureChain | Scenario::FusedPureChain | Scenario::Chain { .. } => {
                scenario
                    .output_station_indices()
                    .into_iter()
                    .map(|producer| open_subscription(store, producer, 0))
                    .collect()
            }
            Scenario::Fanout { consumers } => (0..consumers)
                .map(|subscriber| {
                    open_subscription(
                        store,
                        0,
                        u64::try_from(subscriber).expect("fan-out subscriber index fits u64"),
                    )
                })
                .collect(),
        };
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
            Scenario::Sink
            | Scenario::CapacityPressure
            | Scenario::UnfusedPureChain
            | Scenario::FusedPureChain
            | Scenario::Fanout { .. } => Vec::new(),
        };
        let outputs = scenario
            .output_station_indices()
            .into_iter()
            .map(|station| open_output(store, station))
            .collect();
        Self {
            position,
            input_subscriptions,
            count_states,
            outputs,
        }
    }
}

fn open_subscription(store: &Store, producer: usize, subscriber: u64) -> Subscription<Vec<u8>> {
    let output: SubscribedLog<Vec<u8>> = store
        .open_data(&format!("station/{producer:08x}/output"))
        .expect("open producer output to validate input progress");
    output.subscription(subscriber)
}

fn open_output(store: &Store, station: usize) -> SubscribedLogWriter<Vec<u8>> {
    let output: SubscribedLog<Vec<u8>> = store
        .open_data(&format!("station/{station:08x}/output"))
        .expect("open output to validate durable IPC append counts");
    output.writer()
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
                .input_positions
                .iter()
                .all(|position| *position == self.expected_input_position)
            && counts_match
            && self.output_tails == self.expected_output_tails
            && (self.expected_capacity_output_bounds.is_some()
                || self.output_retained_bytes.iter().all(|bytes| *bytes == 0))
            && self.ipc_change_appends == self.expected_ipc_change_appends
            && self.capacity_output_bounds == self.expected_capacity_output_bounds
    }
}

impl Scenario {
    const fn label(self) -> &'static str {
        match self {
            Self::Sink => "sink_steady",
            Self::CapacityPressure => "capacity_pressure_steady",
            Self::UnfusedPureChain => "pure_chain_unfused_steady",
            Self::FusedPureChain => "pure_chain_fused_steady",
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
            Self::Sink | Self::CapacityPressure | Self::FusedPureChain => 2,
            Self::UnfusedPureChain => PURE_CHAIN_LOGICAL_OPERATION_COUNT,
            Self::Chain { station_count } => station_count,
            Self::Fanout { consumers } => consumers + 1,
        }
    }

    const fn fanout(self) -> usize {
        match self {
            Self::Sink
            | Self::CapacityPressure
            | Self::UnfusedPureChain
            | Self::FusedPureChain
            | Self::Chain { .. } => 1,
            Self::Fanout { consumers } => consumers,
        }
    }

    const fn topology_name(self) -> &'static str {
        match self {
            Self::Sink | Self::CapacityPressure => "scan_sink",
            Self::UnfusedPureChain | Self::FusedPureChain => {
                "sequence_project_extend_filter_select_schema_align_discard"
            }
            Self::Chain { .. } => "count_chain",
            Self::Fanout { .. } => "scan_fanout_sinks",
        }
    }

    const fn output_capacity_bytes(self) -> NonZeroU64 {
        match self {
            Self::CapacityPressure => TIGHT_OUTPUT_CAPACITY_BYTES,
            Self::Sink
            | Self::UnfusedPureChain
            | Self::FusedPureChain
            | Self::Chain { .. }
            | Self::Fanout { .. } => OUTPUT_CAPACITY_BYTES,
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

    const fn ipc_change_appends_per_advance(self) -> usize {
        if self.is_capacity_pressure() {
            0
        } else {
            self.output_log_count()
        }
    }

    const fn output_log_count(self) -> usize {
        match self {
            Self::Sink | Self::CapacityPressure | Self::FusedPureChain | Self::Fanout { .. } => 1,
            Self::UnfusedPureChain | Self::Chain { .. } => self.station_count() - 1,
        }
    }

    fn output_station_indices(self) -> Vec<usize> {
        match self {
            Self::Sink | Self::CapacityPressure | Self::FusedPureChain | Self::Fanout { .. } => {
                vec![0]
            }
            Self::UnfusedPureChain | Self::Chain { .. } => (0..self.station_count() - 1).collect(),
        }
    }

    const fn fusible_transform_count(self) -> usize {
        if matches!(self, Self::UnfusedPureChain | Self::FusedPureChain) {
            PURE_CHAIN_TRANSFORM_COUNT
        } else {
            0
        }
    }

    const fn inline_stage_count(self) -> usize {
        if matches!(self, Self::FusedPureChain) {
            PURE_CHAIN_TRANSFORM_COUNT
        } else {
            0
        }
    }

    const fn logical_operation_count(self) -> usize {
        if matches!(self, Self::UnfusedPureChain | Self::FusedPureChain) {
            PURE_CHAIN_LOGICAL_OPERATION_COUNT
        } else {
            self.station_count()
        }
    }

    const fn execution_layout(self) -> &'static str {
        match self {
            Self::UnfusedPureChain => "standalone_stations",
            Self::FusedPureChain => "scan_output_pipeline",
            Self::Sink | Self::CapacityPressure | Self::Chain { .. } | Self::Fanout { .. } => {
                "station_cores"
            }
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
        if std::env::args_os().any(|argument| argument == "--bench") {
            require_release_build(BENCHMARK);
        }
        let root = RunRoot::for_profile(BENCHMARK, profile);
        let host = HostEnvironment::collect(Some(root.filesystem_root()));
        let mut run = Self {
            profile,
            root,
            output: BufWriter::new(std::io::stdout()),
        };
        run.emit(&json!({
            "record": "context",
            "benchmark": BENCHMARK,
            "protocol": "flow_runtime_advance_trace_v4",
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

    fn fixture(&mut self, scenario: Scenario, structure: (usize, usize, usize)) {
        let (station_count, output_count, input_edge_count) = structure;
        self.emit(&json!({
            "record": "fixture",
            "benchmark": BENCHMARK,
            "profile": self.profile,
            "series": scenario.series(),
            "scenario": scenario_context(scenario),
            "structure": {
                "physical_station_count": station_count,
                "durable_output_log_count": output_count,
                "durable_input_edge_count": input_edge_count,
            },
        }));
    }

    fn trace(&mut self, scenario: Scenario, trace: &AdvanceTrace) {
        self.emit(&json!({
            "record": "advance",
            "benchmark": BENCHMARK,
            "profile": self.profile,
            "series": scenario.series(),
            "phase": "sample",
            "sample": trace.sample,
            "round": trace.round,
            "advance": trace.advance,
            "elapsed_ns": nanos(trace.elapsed),
            "outcome": outcome_label(trace.outcome),
        }));
    }

    fn oracle(&mut self, scenario: Scenario, oracle: &DurableOracle) {
        let scenario_context = scenario_context(scenario);
        let input_station_indices = (1..scenario.station_count()).collect::<Vec<_>>();
        let count_station_indices = match scenario {
            Scenario::Chain { station_count } => (1..station_count - 1).collect::<Vec<_>>(),
            Scenario::Sink
            | Scenario::CapacityPressure
            | Scenario::UnfusedPureChain
            | Scenario::FusedPureChain
            | Scenario::Fanout { .. } => Vec::new(),
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
                "input_position": oracle.expected_input_position,
                "count_state": oracle.expected_count_state,
                "capacity_output_bounds": oracle.expected_capacity_output_bounds,
                "advances": work.advances,
                "committed_station_turns": work.committed_station_turns,
                "input_completions": work.input_completions,
                "output_tails": oracle.expected_output_tails,
                "ipc_change_appends": oracle.expected_ipc_change_appends,
            },
            "actual": {
                "scan_position": oracle.scan_position,
                "input_station_indices": input_station_indices,
                "input_positions": oracle.input_positions,
                "count_station_indices": count_station_indices,
                "count_states": oracle.count_states,
                "output_tails": oracle.output_tails,
                "output_retained_bytes": oracle.output_retained_bytes,
                "ipc_change_appends": oracle.ipc_change_appends,
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
    let profile = PerformanceProfile::for_benchmark();
    let config = Config::for_profile(profile);
    let scenarios = std::iter::once(Scenario::Sink)
        .chain(std::iter::once(Scenario::CapacityPressure))
        .chain([Scenario::UnfusedPureChain, Scenario::FusedPureChain])
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
    let structure = validate_flow(&flow, &path, scenario);
    run.fixture(scenario, structure);
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

    assert_eq!(validate_flow(&flow, &path, scenario), structure);
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
    let store = Store::open(path).expect("open Flow Store to seed capacity backlog");
    let output: SubscribedLog<Vec<u8>> = store
        .open_data("station/00000000/output")
        .expect("open scan output to seed capacity backlog");
    let writer = output.writer();
    let (mut transactions, reads) = store.into_transactions().split();
    let transaction = transactions.begin();
    for _ in 0..entries {
        assert!(
            writer
                .try_append(&change, NonZeroU64::MAX, transaction.access())
                .expect("seed scan output capacity backlog")
        );
    }
    transaction
        .commit()
        .expect("commit capacity backlog seed transaction");
    let transaction = reads.begin();
    let status = writer
        .status(transaction.access())
        .expect("read seeded scan output backlog");
    assert_eq!(
        (status.head, status.tail),
        (
            0,
            u64::try_from(entries).expect("capacity backlog entry count fits u64")
        )
    );
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
    let transaction = store.read_transaction();
    let access = transaction.access();
    let scan_position = resources
        .position
        .read(access)
        .expect("access scan position to validate runtime work counts")
        .get()
        .expect("read scan position to validate runtime work counts");
    let expected_position = (!scenario.is_capacity_pressure()).then(|| {
        completed_rounds
            .checked_sub(1)
            .expect("Flow runtime executes at least one round")
    });
    let input_positions = resources
        .input_subscriptions
        .iter()
        .map(|subscription| {
            subscription
                .status(access)
                .expect("read durable input subscription position")
                .position
        })
        .collect::<Vec<_>>();
    let count_values = resources
        .count_states
        .iter()
        .map(|count| {
            count
                .read(access)
                .expect("access RunningEventCount state to validate committed turns")
                .get()
                .expect("read RunningEventCount state to validate committed turns")
        })
        .collect::<Vec<_>>();
    let output_statuses = resources
        .outputs
        .iter()
        .map(|output| output.status(access).expect("read durable output status"))
        .collect::<Vec<_>>();
    let (output_tails, output_retained_bytes) = output_statuses
        .iter()
        .map(|status| (status.tail, status.retained_bytes))
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let expected_capacity_output_bounds = scenario.is_capacity_pressure().then(|| {
        let tail = completed_rounds
            .checked_add(1)
            .expect("Flow runtime capacity backlog tail fits u64");
        [completed_rounds, tail]
    });
    let initial_output_entries = expected_capacity_output_bounds.map_or(0, |bounds| bounds[1]);
    let ipc_change_appends = output_tails.iter().try_fold(0_u64, |total, tail| {
        let appended = tail
            .checked_sub(initial_output_entries)
            .expect("durable output tail cannot precede its seeded baseline");
        total.checked_add(appended)
    });
    let ipc_change_appends =
        ipc_change_appends.expect("Flow runtime durable IPC Change append count fits u64");
    let expected_ipc_change_appends = completed_rounds
        .checked_mul(
            u64::try_from(scenario.ipc_change_appends_per_advance())
                .expect("per-advance IPC Change append count fits u64"),
        )
        .expect("Flow runtime expected IPC Change append count fits u64");
    let expected_output_tails = if scenario.is_capacity_pressure() {
        vec![initial_output_entries]
    } else {
        vec![completed_rounds; scenario.output_log_count()]
    };
    let capacity_output_bounds = scenario.is_capacity_pressure().then(|| {
        let status = output_statuses
            .first()
            .expect("capacity-pressure scenario has one output");
        [status.head, status.tail]
    });
    let oracle = DurableOracle {
        completed_advances: completed_rounds,
        expected_scan_position: expected_position,
        scan_position,
        expected_input_position: completed_rounds,
        input_positions,
        expected_count_state: matches!(scenario, Scenario::Chain { .. })
            .then_some(completed_rounds),
        count_states: count_values,
        expected_output_tails,
        output_tails,
        output_retained_bytes,
        expected_ipc_change_appends,
        ipc_change_appends,
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
        Scenario::UnfusedPureChain => unfused_pure_chain_factory(path, output_capacity_bytes),
        Scenario::FusedPureChain => fused_pure_chain_factory(path, output_capacity_bytes),
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

fn unfused_pure_chain_factory(path: &Path, output_capacity_bytes: NonZeroU64) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let project = factory.station("project", pure_chain_project());
    let extend = factory.station("extend", pure_chain_extend());
    let filter = factory.station("filter", pure_chain_filter());
    let select = factory.station("select", pure_chain_select());
    let schema_align = factory.station("schema-align", pure_chain_schema_align());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [scan, project, extend, filter, select, schema_align] {
        factory.output_capacity_bytes(station, output_capacity_bytes);
    }
    for (input, output) in [
        (scan, project),
        (project, extend),
        (extend, filter),
        (filter, select),
        (select, schema_align),
        (schema_align, sink),
    ] {
        factory.connect([input], output);
    }
    factory
}

fn fused_pure_chain_factory(path: &Path, output_capacity_bytes: NonZeroU64) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, output_capacity_bytes);
    factory.connect([scan], sink);
    factory
        .inline_output(scan, pure_chain_project())
        .expect("inline pure-chain Project")
        .inline_output(scan, pure_chain_extend())
        .expect("inline pure-chain Extend")
        .inline_output(scan, pure_chain_filter())
        .expect("inline pure-chain Filter")
        .inline_output(scan, pure_chain_select())
        .expect("inline pure-chain Select")
        .inline_output(scan, pure_chain_schema_align())
        .expect("inline pure-chain SchemaAlign");
    factory
}

fn pure_chain_project() -> ProjectDefinition {
    ProjectDefinition::new([0])
}

fn pure_chain_extend() -> ExtendDefinition {
    ExtendDefinition::try_new("next", col("value") + lit(1_u64))
        .expect("construct pure-chain Extend definition")
}

fn pure_chain_filter() -> FilterDefinition {
    FilterDefinition::try_new(col("next").gt(lit(0_u64)))
        .expect("construct pure-chain Filter definition")
}

fn pure_chain_select() -> SelectDefinition {
    SelectDefinition::try_new([("value", col("value")), ("next", col("next"))])
        .expect("construct pure-chain Select definition")
}

fn pure_chain_schema_align() -> SchemaAlignDefinition {
    SchemaAlignDefinition::try_new([
        SchemaAlignField::try_new("source_value", col("value"), false)
            .expect("construct pure-chain source field alignment"),
        SchemaAlignField::try_new("derived_value", col("next"), false)
            .expect("construct pure-chain derived field alignment"),
    ])
    .expect("construct pure-chain SchemaAlign definition")
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

fn validate_flow(flow: &Flow, path: &Path, scenario: Scenario) -> (usize, usize, usize) {
    assert_eq!(flow.path(), path);
    let statuses = flow
        .status()
        .expect("read Flow status to validate benchmark structure");
    let station_count = statuses.len();
    let output_count = statuses
        .iter()
        .filter(|station| station.output.is_some())
        .count();
    let input_edge_count = statuses.iter().map(|station| station.inputs.len()).sum();
    assert_eq!(flow.station_count(), station_count);
    for status in statuses
        .iter()
        .filter_map(|station| station.output.as_ref())
    {
        assert_eq!(
            status.capacity_bytes,
            scenario.output_capacity_bytes().get()
        );
    }
    (station_count, output_count, input_edge_count)
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
            "durable_subscription_position_advance_fanout_counts_each_edge",
        "committed_station_turn_unit":
            "outer_station_transaction_committed_after_action_and_subscription_acknowledgement",
        "ipc_change_append_unit":
            "committed_station_output_log_tail_advance_excluding_fixture_seed_entries",
        "cumulative_ipc_bytes": null,
        "cumulative_ipc_bytes_unavailable_reason":
            "public_output_status_exposes_current_retained_bytes_not_historical_bytes_written",
        "per_transaction_duration_ns": null,
        "per_transaction_duration_unavailable_reason":
            "public_flow_api_exposes_complete_advance_duration_not_individual_station_transactions",
        "round_latency_scope": "one_complete_flow_advance_call",
        "raw_round_latencies": "one_advance_record_per_sampled_call",
        "raw_outcomes": "one_advance_record_per_sampled_call",
        "durable_oracle": "one_full_actual_and_expected_record_per_scenario",
        "measurement_protocol": "owner_local_advance_trace_v4",
        "fixtures": "built_once_outside_timing",
        "validation": "outside_timing",
        "execution": "single_thread",
        "rocksdb_wal_sync": true,
    })
}

fn scenario_context(scenario: Scenario) -> Value {
    json!({
        "topology": scenario.topology_name(),
        "execution_layout": scenario.execution_layout(),
        "logical_operation_count": scenario.logical_operation_count(),
        "fusible_transform_count": scenario.fusible_transform_count(),
        "inline_stage_count": scenario.inline_stage_count(),
        "semantic_committed_station_turns_per_progressed_advance":
            scenario.committed_station_turns_per_advance(),
        "ipc_change_appends_per_progressed_advance":
            scenario.ipc_change_appends_per_advance(),
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
