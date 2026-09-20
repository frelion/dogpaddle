//! Process-isolated Rust heap and logical-state evidence for residual `EquiJoin`.

use std::{
    ffi::OsString,
    fs::{self, File},
    io::{BufWriter, Write},
    mem::size_of,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_expr::col;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{
        Action, Operation, OperationInput, Turn,
        transform::{EquiJoinDefinition, EquiJoinKind},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{
    OrderedMap, ReadTransactions, ScanDirection, ScanLimit, Store, StoreSetup, Transactions,
};
use serde::Serialize;
use serde_json::{Value, json};

const BENCHMARK: &str = "equi_join_resources";
const CHILD_ARGUMENT: &str = "--resource-child";
const MATCH_COUNTS: &str = "equi_join.match_counts";
const STATE_SCAN_ITEMS: usize = 256;
const STATE_SCAN_BYTES: usize = 4 * 1024 * 1024;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Invocation {
    Test,
    Benchmark,
}

#[derive(Clone, Copy)]
enum Scenario {
    Fanout {
        kind: EquiJoinKind,
        candidates: usize,
        candidate_payload_bytes: usize,
        qualifying: usize,
        observe_state: bool,
    },
    WholeClaim {
        rows: usize,
        payload_bytes: usize,
    },
}

#[derive(Clone, Copy)]
struct CaseSpec {
    name: &'static str,
    scenario: Scenario,
}

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    reads: ReadTransactions,
    match_counts: Option<OrderedMap<Vec<u8>, u64>>,
}

struct Workload {
    fixture: Fixture,
    port: usize,
    input: Change,
    expected_output_rows: usize,
}

#[derive(Default, Serialize)]
struct ClaimMeasurement {
    turns: usize,
    output_rows: usize,
    positive_rows: usize,
    negative_rows: usize,
    output_arrow_bytes: usize,
    max_turn_output_rows: usize,
    max_turn_output_arrow_bytes: usize,
}

#[derive(Clone, Copy, Default, Serialize)]
struct StateSnapshot {
    actual_entries: usize,
    shadow_entries: usize,
    zero_value_entries: usize,
    logical_key_value_bytes: usize,
}

#[derive(Default)]
struct StatePeaks {
    actual_entries: usize,
    shadow_entries: usize,
    total_entries: usize,
    logical_key_value_bytes: usize,
}

#[derive(Serialize)]
struct PersistentStateMeasurement {
    collection: &'static str,
    coverage: &'static str,
    peak_actual_entries: usize,
    peak_shadow_entries: usize,
    peak_total_entries: usize,
    peak_logical_key_value_bytes: usize,
    after_insert: StateSnapshot,
    after_retract: StateSnapshot,
}

#[derive(Serialize)]
struct RustHeapMeasurement {
    coverage: &'static str,
    total_blocks: u64,
    total_bytes: u64,
    current_blocks: usize,
    current_bytes: usize,
    peak_blocks: usize,
    peak_bytes: usize,
}

#[derive(Serialize)]
struct ResourceRecord {
    benchmark: &'static str,
    case: &'static str,
    profile: &'static str,
    invocation: &'static str,
    workload: Value,
    input_arrow_bytes: usize,
    rust_heap: RustHeapMeasurement,
    claim: ClaimMeasurement,
    persistent_state: Option<PersistentStateMeasurement>,
    rss_bytes: Option<u64>,
    rss_status: &'static str,
}

impl Invocation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::Benchmark => "benchmark",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "test" => Self::Test,
            "benchmark" => Self::Benchmark,
            _ => panic!("unknown EquiJoin resource invocation {value:?}"),
        }
    }
}

impl CaseSpec {
    const fn fanout(
        name: &'static str,
        kind: EquiJoinKind,
        candidates: usize,
        candidate_payload_bytes: usize,
        qualifying: usize,
        observe_state: bool,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::Fanout {
                kind,
                candidates,
                candidate_payload_bytes,
                qualifying,
                observe_state,
            },
        }
    }

    const fn whole_claim(name: &'static str, rows: usize, payload_bytes: usize) -> Self {
        Self {
            name,
            scenario: Scenario::WholeClaim {
                rows,
                payload_bytes,
            },
        }
    }

    fn context(self) -> Value {
        match self.scenario {
            Scenario::Fanout {
                kind,
                candidates,
                candidate_payload_bytes,
                qualifying,
                observe_state,
            } => json!({
                "name": self.name,
                "scenario": "fanout",
                "join_kind": format!("{kind:?}"),
                "distinct_candidates": candidates,
                "candidate_payload_bytes": candidate_payload_bytes,
                "qualifying_candidates": qualifying,
                "logical_predicate_evaluations": 2 * candidates,
                "observe_match_counts": observe_state,
            }),
            Scenario::WholeClaim {
                rows,
                payload_bytes,
            } => json!({
                "name": self.name,
                "scenario": "whole_claim",
                "join_kind": "Inner",
                "claim_rows": rows,
                "claim_payload_bytes_per_row": payload_bytes,
                "distinct_candidates": 0,
                "qualifying_candidates": 0,
                "logical_predicate_evaluations": 0,
                "observe_match_counts": false,
            }),
        }
    }
}

impl Fixture {
    fn new(path: &Path, kind: EquiJoinKind, schema: &SchemaRef) -> Self {
        let output_names: &[&str] = match kind {
            EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti => {
                &["left_key", "left_value", "left_payload"]
            }
            EquiJoinKind::Inner | EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => &[
                "left_key",
                "left_value",
                "left_payload",
                "right_key",
                "right_value",
                "right_payload",
            ],
        };
        let definition = EquiJoinDefinition::try_new(
            kind,
            [(col("key"), col("key"))],
            output_names.iter().copied(),
            Some(col("left.value").lt(col("right.value"))),
        )
        .expect("define residual EquiJoin resource workload");
        let mut setup = StoreSetup::new();
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema), Arc::clone(schema)],
                &mut setup.data_scope(),
                "operation",
                RuntimeResource::none(),
            )
            .expect("construct residual EquiJoin resource workload")
            .into_parts();
        let transactions = setup.commit(path, |_| Ok(())).expect("commit setup");
        drop((operation, transactions));
        let store = Store::open(path).expect("reopen observational store");
        let match_counts = (kind != EquiJoinKind::Inner).then(|| {
            store
                .open_data::<OrderedMap<Vec<u8>, u64>>("operation/equi_join.match_counts")
                .expect("open an observational match-count handle")
        });
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema), Arc::clone(schema)],
                &mut store.data_scope(),
                "operation",
                RuntimeResource::none(),
            )
            .expect("open residual EquiJoin resource workload")
            .into_parts();
        let (transactions, reads) = store.into_transactions().split();
        Self {
            operation,
            transactions,
            reads,
            match_counts,
        }
    }

    fn apply(&mut self, port: usize, change: &Change) -> ClaimMeasurement {
        let mut measurement = ClaimMeasurement::default();
        for _ in 0..100_000 {
            let (complete, output) = self.apply_once(port, change);
            measurement.observe(output.as_ref());
            if complete {
                return measurement;
            }
        }
        panic!("residual EquiJoin resource Claim did not complete")
    }

    fn apply_once(&mut self, port: usize, change: &Change) -> (bool, Option<Change>) {
        let Turn::Ready(prepared) = self
            .operation
            .turn(Some(OperationInput { port, change }))
            .expect("prepare residual EquiJoin resource turn")
        else {
            panic!("residual EquiJoin must be ready for a pinned input")
        };
        let transaction = self.transactions.begin();
        let (action, completion) = prepared
            .apply(transaction.access())
            .expect("apply residual EquiJoin resource turn");
        transaction
            .commit()
            .expect("commit residual EquiJoin resource turn");
        completion
            .run()
            .expect("complete residual EquiJoin resource turn");
        match action {
            Action::Commit(output) => (false, output),
            Action::Complete(output) => (true, output),
            Action::Idle => panic!("residual EquiJoin returned Idle for a pinned input"),
        }
    }

    fn state_snapshot(&self) -> StateSnapshot {
        let map = self
            .match_counts
            .as_ref()
            .expect("only a residual presence Join exposes match-count state");
        let transaction = self.reads.begin();
        let map = map
            .read(transaction.access())
            .expect("read match-count state");
        let limit = ScanLimit::new(STATE_SCAN_ITEMS, STATE_SCAN_BYTES)
            .expect("positive match-count scan limits are valid");
        let mut resume_after = None;
        let mut snapshot = StateSnapshot::default();
        loop {
            let page = map
                .scan(.., ScanDirection::Ascending, resume_after.as_ref(), limit)
                .expect("scan match-count state");
            for (key, value) in page.entries {
                match key.first() {
                    Some(0) => snapshot.actual_entries += 1,
                    Some(1) => snapshot.shadow_entries += 1,
                    _ => panic!("match-count state contains an unknown key domain"),
                }
                snapshot.zero_value_entries += usize::from(value == 0);
                snapshot.logical_key_value_bytes = snapshot
                    .logical_key_value_bytes
                    .saturating_add(key.len())
                    .saturating_add(size_of::<u64>());
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            resume_after = Some(continuation);
        }
        snapshot
    }
}

impl ClaimMeasurement {
    fn observe(&mut self, output: Option<&Change>) {
        self.turns += 1;
        let Some(output) = output else {
            return;
        };
        let output_rows = output.num_rows();
        let output_arrow_bytes = change_arrow_bytes(output);
        self.output_rows += output_rows;
        self.output_arrow_bytes = self.output_arrow_bytes.saturating_add(output_arrow_bytes);
        self.max_turn_output_rows = self.max_turn_output_rows.max(output_rows);
        self.max_turn_output_arrow_bytes = self.max_turn_output_arrow_bytes.max(output_arrow_bytes);
        for difference in output.diffs().values() {
            match difference.cmp(&0) {
                std::cmp::Ordering::Less => self.negative_rows += 1,
                std::cmp::Ordering::Greater => self.positive_rows += 1,
                std::cmp::Ordering::Equal => panic!("Change admitted a zero difference"),
            }
        }
    }
}

impl StatePeaks {
    fn observe(&mut self, snapshot: StateSnapshot) {
        self.actual_entries = self.actual_entries.max(snapshot.actual_entries);
        self.shadow_entries = self.shadow_entries.max(snapshot.shadow_entries);
        self.total_entries = self
            .total_entries
            .max(snapshot.actual_entries + snapshot.shadow_entries);
        self.logical_key_value_bytes = self
            .logical_key_value_bytes
            .max(snapshot.logical_key_value_bytes);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn change(
    schema: &SchemaRef,
    keys: Vec<u64>,
    values: Vec<i64>,
    payload_bytes: usize,
    difference: i64,
) -> Change {
    assert_eq!(keys.len(), values.len());
    let rows = keys.len();
    let payload = "x".repeat(payload_bytes);
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(vec![payload.as_str(); rows])),
        ],
    )
    .expect("build EquiJoin resource records");
    Change::try_new(records, Int64Array::from(vec![difference; rows]))
        .expect("build EquiJoin resource Change")
}

fn ordinal_values(rows: usize) -> Vec<i64> {
    (0..rows)
        .map(|value| i64::try_from(value).expect("resource workload ordinal fits i64"))
        .collect()
}

fn repeated_key(rows: usize) -> Vec<u64> {
    vec![7; rows]
}

fn unique_keys(rows: usize) -> Vec<u64> {
    (0..rows)
        .map(|value| u64::try_from(value).expect("resource workload ordinal fits u64"))
        .collect()
}

fn prepare_workload(spec: CaseSpec, path: &Path) -> Workload {
    let schema = schema();
    match spec.scenario {
        Scenario::Fanout {
            kind,
            candidates,
            candidate_payload_bytes,
            qualifying,
            ..
        } => {
            assert!(qualifying <= candidates);
            let mut fixture = Fixture::new(path, kind, &schema);
            let seed = change(
                &schema,
                repeated_key(candidates),
                ordinal_values(candidates),
                candidate_payload_bytes,
                1,
            );
            let seeded = fixture.apply(0, &seed);
            assert!(seeded.turns > 0);
            let threshold =
                i64::try_from(qualifying).expect("resource workload selectivity fits i64");
            let input = change(&schema, vec![7], vec![threshold], 0, 1);
            let expected_output_rows = match kind {
                EquiJoinKind::Inner => qualifying,
                EquiJoinKind::FullOuter if qualifying == 0 => 1,
                EquiJoinKind::FullOuter => 2 * qualifying,
                _ => panic!("resource fanout case uses an unsupported Join kind"),
            };
            Workload {
                fixture,
                port: 1,
                input,
                expected_output_rows,
            }
        }
        Scenario::WholeClaim {
            rows,
            payload_bytes,
        } => {
            let fixture = Fixture::new(path, EquiJoinKind::Inner, &schema);
            let input = change(
                &schema,
                unique_keys(rows),
                ordinal_values(rows),
                payload_bytes,
                1,
            );
            Workload {
                fixture,
                port: 0,
                input,
                expected_output_rows: 0,
            }
        }
    }
}

fn change_arrow_bytes(change: &Change) -> usize {
    change
        .records()
        .get_array_memory_size()
        .saturating_add(change.diffs().get_array_memory_size())
}

fn measure_heap(spec: CaseSpec, path: &Path) -> (usize, ClaimMeasurement, RustHeapMeasurement) {
    let Workload {
        mut fixture,
        port,
        input,
        expected_output_rows,
    } = prepare_workload(spec, path);
    let input_arrow_bytes = change_arrow_bytes(&input);
    let profiler = dhat::Profiler::builder().testing().build();
    let claim = std::hint::black_box(fixture.apply(port, &input));
    let stats = dhat::HeapStats::get();
    drop(profiler);
    assert_eq!(claim.output_rows, expected_output_rows);
    assert!(claim.turns > 0);
    let heap = RustHeapMeasurement {
        coverage: "allocations made through Rust's global allocator during one complete driving Claim; fixture, seed, and input Arrow allocation excluded; RocksDB native heap excluded",
        total_blocks: stats.total_blocks,
        total_bytes: stats.total_bytes,
        current_blocks: stats.curr_blocks,
        current_bytes: stats.curr_bytes,
        peak_blocks: stats.max_blocks,
        peak_bytes: stats.max_bytes,
    };
    (input_arrow_bytes, claim, heap)
}

fn run_observed_claim(
    fixture: &mut Fixture,
    port: usize,
    input: &Change,
    peaks: &mut StatePeaks,
) -> ClaimMeasurement {
    let mut measurement = ClaimMeasurement::default();
    for _ in 0..100_000 {
        let (complete, output) = fixture.apply_once(port, input);
        measurement.observe(output.as_ref());
        peaks.observe(fixture.state_snapshot());
        if complete {
            return measurement;
        }
    }
    panic!("observed residual EquiJoin resource Claim did not complete")
}

fn measure_persistent_state(spec: CaseSpec, path: &Path) -> PersistentStateMeasurement {
    let Scenario::Fanout {
        kind: EquiJoinKind::FullOuter,
        candidates,
        candidate_payload_bytes,
        qualifying,
        observe_state: true,
    } = spec.scenario
    else {
        panic!("persistent-state measurement requires its FullOuter case")
    };
    let schema = schema();
    let mut fixture = Fixture::new(path, EquiJoinKind::FullOuter, &schema);
    let seed = change(
        &schema,
        repeated_key(candidates),
        ordinal_values(candidates),
        candidate_payload_bytes,
        1,
    );
    fixture.apply(0, &seed);
    let threshold = i64::try_from(qualifying).expect("resource workload selectivity fits i64");
    let insert = change(&schema, vec![7], vec![threshold], 0, 1);
    let retract = change(&schema, vec![7], vec![threshold], 0, -1);
    let mut peaks = StatePeaks::default();
    peaks.observe(fixture.state_snapshot());
    let inserted = run_observed_claim(&mut fixture, 1, &insert, &mut peaks);
    assert_eq!(inserted.output_rows, 2 * qualifying);
    let after_insert = fixture.state_snapshot();
    let expected_actual = qualifying + 1;
    assert_eq!(after_insert.actual_entries, expected_actual);
    assert_eq!(after_insert.shadow_entries, 0);
    let retracted = run_observed_claim(&mut fixture, 1, &retract, &mut peaks);
    assert_eq!(retracted.output_rows, 2 * qualifying);
    let after_retract = fixture.state_snapshot();
    assert_eq!(after_retract.actual_entries, 0);
    assert_eq!(after_retract.shadow_entries, 0);
    assert!(peaks.shadow_entries >= expected_actual);
    assert!(peaks.total_entries >= expected_actual.saturating_mul(2));
    PersistentStateMeasurement {
        collection: MATCH_COUNTS,
        coverage: "decoded logical map entries and key-plus-u64 bytes observed after every committed turn in a separate unprofiled pass; excludes RocksDB/WAL/LSM/cache bytes",
        peak_actual_entries: peaks.actual_entries,
        peak_shadow_entries: peaks.shadow_entries,
        peak_total_entries: peaks.total_entries,
        peak_logical_key_value_bytes: peaks.logical_key_value_bytes,
        after_insert,
        after_retract,
    }
}

fn measure_case(
    spec: CaseSpec,
    sample: &Path,
    profile: PerformanceProfile,
    invocation: Invocation,
) -> ResourceRecord {
    let heap_path = sample.join("heap-store");
    let (input_arrow_bytes, claim, rust_heap) = measure_heap(spec, &heap_path);
    let persistent_state = match spec.scenario {
        Scenario::Fanout {
            observe_state: true,
            ..
        } => Some(measure_persistent_state(spec, &sample.join("state-store"))),
        Scenario::Fanout {
            observe_state: false,
            ..
        }
        | Scenario::WholeClaim { .. } => None,
    };
    ResourceRecord {
        benchmark: BENCHMARK,
        case: spec.name,
        profile: profile.as_str(),
        invocation: invocation.as_str(),
        workload: spec.context(),
        input_arrow_bytes,
        rust_heap,
        claim,
        persistent_state,
        rss_bytes: None,
        rss_status: "unavailable: this portable runner does not sample process RSS; allocator bytes and logical Store bytes must not be interpreted as RSS",
    }
}

fn cases(profile: PerformanceProfile, invocation: Invocation) -> Vec<CaseSpec> {
    if invocation == Invocation::Test {
        test_cases()
    } else {
        benchmark_cases(profile)
    }
}

fn test_cases() -> Vec<CaseSpec> {
    vec![
        CaseSpec::fanout("selectivity_zero", EquiJoinKind::Inner, 8, 0, 0, false),
        CaseSpec::fanout("selectivity_full", EquiJoinKind::Inner, 8, 0, 8, false),
        CaseSpec::fanout("wide_full", EquiJoinKind::Inner, 8, 4 * 1024, 8, false),
        CaseSpec::fanout(
            "oversized_candidate",
            EquiJoinKind::Inner,
            1,
            1024 * 1024 + 1,
            1,
            false,
        ),
        CaseSpec::fanout("page_boundary", EquiJoinKind::Inner, 257, 0, 129, false),
        CaseSpec::whole_claim("whole_claim", 33, 256),
        CaseSpec::fanout(
            "full_outer_state",
            EquiJoinKind::FullOuter,
            129,
            0,
            65,
            true,
        ),
        CaseSpec::fanout(
            "full_outer_wide_state",
            EquiJoinKind::FullOuter,
            257,
            4 * 1024,
            129,
            true,
        ),
    ]
}

fn benchmark_cases(profile: PerformanceProfile) -> Vec<CaseSpec> {
    let (selection_fanout, wide_fanout, wide_payload, large_fanout, claim_rows, claim_payload) =
        match profile {
            PerformanceProfile::Smoke => (64, 32, 16 * 1024, 1_024, 257, 1_024),
            PerformanceProfile::Reference => (1_024, 64, 64 * 1024, 4_096, 1_024, 4 * 1024),
        };
    vec![
        CaseSpec::fanout(
            "selectivity_zero",
            EquiJoinKind::Inner,
            selection_fanout,
            0,
            0,
            false,
        ),
        CaseSpec::fanout(
            "selectivity_half",
            EquiJoinKind::Inner,
            selection_fanout,
            0,
            selection_fanout / 2,
            false,
        ),
        CaseSpec::fanout(
            "selectivity_full",
            EquiJoinKind::Inner,
            selection_fanout,
            0,
            selection_fanout,
            false,
        ),
        CaseSpec::fanout(
            "wide_zero",
            EquiJoinKind::Inner,
            wide_fanout,
            wide_payload,
            0,
            false,
        ),
        CaseSpec::fanout(
            "wide_full",
            EquiJoinKind::Inner,
            wide_fanout,
            wide_payload,
            wide_fanout,
            false,
        ),
        CaseSpec::fanout(
            "oversized_candidate",
            EquiJoinKind::Inner,
            1,
            1024 * 1024 + 1,
            1,
            false,
        ),
        CaseSpec::fanout("page_boundary", EquiJoinKind::Inner, 257, 0, 129, false),
        CaseSpec::fanout(
            "large_fanout",
            EquiJoinKind::Inner,
            large_fanout,
            0,
            large_fanout / 2,
            false,
        ),
        CaseSpec::whole_claim("whole_claim", claim_rows, claim_payload),
        CaseSpec::fanout(
            "full_outer_state",
            EquiJoinKind::FullOuter,
            selection_fanout.max(129),
            0,
            selection_fanout.max(129) / 2,
            true,
        ),
        CaseSpec::fanout(
            "full_outer_wide_state",
            EquiJoinKind::FullOuter,
            wide_fanout.max(257),
            wide_payload,
            wide_fanout.max(257) / 2,
            true,
        ),
    ]
}

fn parse_profile(value: &str) -> PerformanceProfile {
    match value {
        "smoke" => PerformanceProfile::Smoke,
        "reference" => PerformanceProfile::Reference,
        _ => panic!("unknown EquiJoin resource profile {value:?}"),
    }
}

fn run_child(arguments: &[OsString]) {
    assert_eq!(arguments.len(), 5, "invalid resource child arguments");
    let case = arguments[1]
        .to_str()
        .expect("resource case name must be Unicode");
    let profile = parse_profile(
        arguments[2]
            .to_str()
            .expect("resource profile must be Unicode"),
    );
    let invocation = Invocation::parse(
        arguments[3]
            .to_str()
            .expect("resource invocation must be Unicode"),
    );
    let sample = PathBuf::from(&arguments[4]);
    let spec = cases(profile, invocation)
        .into_iter()
        .find(|spec| spec.name == case)
        .unwrap_or_else(|| panic!("unknown EquiJoin resource case {case:?}"));
    let record = measure_case(spec, &sample, profile, invocation);
    serde_json::to_writer(std::io::stdout().lock(), &record)
        .expect("write EquiJoin resource child result");
    println!();
}

fn invoke_child(
    executable: &Path,
    profile: PerformanceProfile,
    invocation: Invocation,
    spec: CaseSpec,
    sample: &Path,
) -> Value {
    let output = Command::new(executable)
        .arg(CHILD_ARGUMENT)
        .arg(spec.name)
        .arg(profile.as_str())
        .arg(invocation.as_str())
        .arg(sample)
        .output()
        .expect("start EquiJoin resource child");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "EquiJoin resource child {:?} exited with {}: {stderr}",
            spec.name, output.status
        );
    }
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "decode EquiJoin resource child {:?}: {error}; stdout={}",
            spec.name,
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn write_context(
    root: &RunRoot,
    profile: PerformanceProfile,
    invocation: Invocation,
    specs: &[CaseSpec],
) {
    let context = json!({
        "benchmark": BENCHMARK,
        "runner": "owner-local process-isolated dhat",
        "dhat_version": "0.3.3",
        "profile": profile,
        "invocation": invocation.as_str(),
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "measurement_contracts": {
            "rust_heap": "Each fresh child builds its fixture, seed, and input before starting one dhat Profiler. Stats cover allocations made through Rust's global allocator during one complete driving Claim. They exclude the input Arrow allocation, pre-existing fixture/seed memory, and RocksDB native allocations.",
            "persistent_logical_state": "The FullOuter case runs a second unprofiled fixture and scans equi_join.match_counts after every committed turn. Counts and decoded key-plus-u64 bytes exclude RocksDB cache, WAL, LSM, compression, tombstones, and filesystem allocation.",
            "output_arrow_bytes": "Arrow get_array_memory_size plus the diff array, summed for emitted Changes. Arrow may count shared buffers more than once.",
            "rss": "Unavailable. The portable owner runner intentionally does not treat allocator counters, logical Store bytes, ps samples, or platform-specific high-water units as process RSS.",
            "comparison": "Only benchmark-mode records from the same code, rustc, host, profile, filesystem, workload, and baseline epoch are comparable. Test-mode heap values validate the protocol only."
        },
        "cases": specs.iter().copied().map(CaseSpec::context).collect::<Vec<_>>(),
    });
    fs::write(
        root.path().join("resource-context.json"),
        serde_json::to_vec_pretty(&context).expect("encode EquiJoin resource context"),
    )
    .expect("write EquiJoin resource context");
}

fn run_parent(arguments: &[OsString]) {
    let profile = PerformanceProfile::for_benchmark();
    let invocation = if arguments.iter().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
        Invocation::Benchmark
    } else {
        Invocation::Test
    };
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let specs = cases(profile, invocation);
    write_context(&root, profile, invocation, &specs);
    let executable = std::env::current_exe().expect("locate EquiJoin resource executable");
    let result_file = File::create(root.path().join("resources.jsonl"))
        .expect("create EquiJoin resource result file");
    let mut result_file = BufWriter::new(result_file);
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    for spec in specs {
        eprintln!("{BENCHMARK}: measuring {}", spec.name);
        let sample = root.sample(spec.name);
        let record = invoke_child(&executable, profile, invocation, spec, sample.path());
        serde_json::to_writer(&mut result_file, &record)
            .expect("write EquiJoin resource file record");
        result_file
            .write_all(b"\n")
            .expect("terminate EquiJoin resource file record");
        result_file
            .flush()
            .expect("flush EquiJoin resource file record");
        serde_json::to_writer(&mut stdout, &record).expect("write EquiJoin resource stdout record");
        stdout
            .write_all(b"\n")
            .expect("terminate EquiJoin resource stdout record");
        stdout.flush().expect("flush EquiJoin resource stdout");
    }
}

fn main() {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == CHILD_ARGUMENT)
    {
        run_child(&arguments);
    } else {
        run_parent(&arguments);
    }
}
