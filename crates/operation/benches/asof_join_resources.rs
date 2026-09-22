//! Process-isolated Rust heap, output, and logical-state evidence for `AsOfJoin`.

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
        transform::{
            AsOfDirection, AsOfEqualityKey, AsOfEqualityMode, AsOfJoinDefinition, AsOfJoinKind,
            AsOfOrderKey, AsOfTieBreak, AsOfTieFallback,
        },
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{
    OrderedMap, ReadTransactions, ScanDirection, ScanLimit, Store, StoreSetup, Transactions,
};
use serde::Serialize;
use serde_json::{Value, json};

const BENCHMARK: &str = "asof_join_resources";
const CHILD_ARGUMENT: &str = "--resource-child";
const LEFT_ROWS: &str = "asof_join.left_rows";
const RIGHT_ROWS: &str = "asof_join.right_rows";
const STATE_SCAN_ITEMS: usize = 256;
const STATE_SCAN_BYTES: usize = 8 * 1024 * 1024;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Invocation {
    Test,
    Benchmark,
}

#[derive(Clone, Copy)]
enum Scenario {
    LeftLookup {
        candidates: usize,
        candidate_payload_bytes: usize,
        residual_far_fallback: bool,
    },
    WholeClaim {
        rows: usize,
        payload_bytes: usize,
    },
    RightHistoricalRematch {
        left_rows: usize,
        right_versions: usize,
        payload_bytes: usize,
    },
    RightEmptyLeftPreload {
        rows: usize,
        payload_bytes: usize,
        key_shape: RightKeyShape,
    },
    RightNullLeftHistory {
        left_rows: usize,
        right_events: usize,
        payload_bytes: usize,
    },
    LeftNullRightHistory {
        right_rows: usize,
        payload_bytes: usize,
    },
    RightActiveOverlay {
        rows: usize,
        payload_bytes: usize,
    },
}

#[derive(Clone, Copy)]
enum RightKeyShape {
    Distinct,
    Same,
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
    left_rows: OrderedMap<Vec<u8>, u64>,
    right_rows: OrderedMap<Vec<u8>, u64>,
}

struct Workload {
    fixture: Fixture,
    port: usize,
    inserted: Change,
    retracted: Change,
    expected_insert_output: ExpectedOutput,
    expected_retract_output: ExpectedOutput,
    expected_min_turns: usize,
    expected_left_after_insert: Option<(usize, u64)>,
    expected_right_after_insert: Option<(usize, u64)>,
}

#[derive(Clone, Copy)]
struct ExpectedOutput {
    positive_rows: usize,
    negative_rows: usize,
}

#[derive(Clone, Copy)]
struct GroupwiseOutputOracle {
    groups: usize,
    difference: i64,
}

#[derive(Default, Serialize)]
struct ClaimMeasurement {
    turns: usize,
    output_rows: usize,
    positive_rows: usize,
    negative_rows: usize,
    positive_weight: u64,
    negative_weight: u64,
    output_arrow_bytes: usize,
    max_turn_output_rows: usize,
    max_turn_output_arrow_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
struct MapSnapshot {
    entries: usize,
    zero_value_entries: usize,
    total_weight: u64,
    logical_key_value_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
struct StateSnapshot {
    left: MapSnapshot,
    right: MapSnapshot,
}

#[derive(Default)]
struct StatePeaks {
    left_entries: usize,
    right_entries: usize,
    total_entries: usize,
    logical_key_value_bytes: usize,
}

#[derive(Serialize)]
struct PersistentStateMeasurement {
    collections: [&'static str; 2],
    coverage: &'static str,
    peak_left_entries: usize,
    peak_right_entries: usize,
    peak_total_entries: usize,
    peak_logical_key_value_bytes: usize,
    before: StateSnapshot,
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
    persistent_state: PersistentStateMeasurement,
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
            _ => panic!("unknown ASOF resource invocation {value:?}"),
        }
    }
}

impl CaseSpec {
    const fn left_lookup(
        name: &'static str,
        candidates: usize,
        candidate_payload_bytes: usize,
        residual_far_fallback: bool,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::LeftLookup {
                candidates,
                candidate_payload_bytes,
                residual_far_fallback,
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

    const fn right_historical_rematch(
        name: &'static str,
        left_rows: usize,
        right_versions: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::RightHistoricalRematch {
                left_rows,
                right_versions,
                payload_bytes,
            },
        }
    }

    const fn right_empty_left_distinct(
        name: &'static str,
        rows: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::RightEmptyLeftPreload {
                rows,
                payload_bytes,
                key_shape: RightKeyShape::Distinct,
            },
        }
    }

    const fn right_empty_left_same_key(
        name: &'static str,
        rows: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::RightEmptyLeftPreload {
                rows,
                payload_bytes,
                key_shape: RightKeyShape::Same,
            },
        }
    }

    const fn right_active_overlay(name: &'static str, rows: usize, payload_bytes: usize) -> Self {
        Self {
            name,
            scenario: Scenario::RightActiveOverlay {
                rows,
                payload_bytes,
            },
        }
    }

    const fn right_null_left_history(
        name: &'static str,
        left_rows: usize,
        right_events: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::RightNullLeftHistory {
                left_rows,
                right_events,
                payload_bytes,
            },
        }
    }

    const fn left_null_right_history(
        name: &'static str,
        right_rows: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            scenario: Scenario::LeftNullRightHistory {
                right_rows,
                payload_bytes,
            },
        }
    }

    fn context(self) -> Value {
        match self.scenario {
            Scenario::LeftLookup {
                candidates,
                candidate_payload_bytes,
                residual_far_fallback,
            } => json!({
                "name": self.name,
                "scenario": "left_lookup",
                "right_candidates": candidates,
                "candidate_payload_bytes": candidate_payload_bytes,
                "residual": residual_far_fallback.then_some("right.value > left.value"),
                "qualifying_candidates": if residual_far_fallback { 1 } else { candidates },
                "winner_position": if residual_far_fallback { "oldest/farthest backward" } else { "newest backward" },
            }),
            Scenario::WholeClaim {
                rows,
                payload_bytes,
            } => json!({
                "name": self.name,
                "scenario": "whole_claim",
                "left_rows": rows,
                "payload_bytes_per_row": payload_bytes,
                "right_candidates": 0,
            }),
            Scenario::RightHistoricalRematch {
                left_rows,
                right_versions,
                payload_bytes,
            } => json!({
                "name": self.name,
                "scenario": "right_historical_rematch",
                "left_rows": left_rows,
                "right_versions": right_versions,
                "payload_bytes_per_row": payload_bytes,
                "corrected_left_rows": left_rows,
                "candidate_shape": "one eligible old version plus future ineligible history",
            }),
            Scenario::RightEmptyLeftPreload {
                rows,
                payload_bytes,
                key_shape,
            } => {
                let (scenario, exact_right_keys, purpose) = match key_shape {
                    RightKeyShape::Distinct => (
                        "right_empty_left_distinct_preload",
                        rows,
                        "port-1 whole-Claim turn pagination and heap growth without left rematch work",
                    ),
                    RightKeyShape::Same => (
                        "right_empty_left_same_key_multiplicity",
                        usize::from(rows != 0),
                        "port-1 repeated-key admission, multiplicity growth, and turn pagination without left rematch work",
                    ),
                };
                json!({
                    "name": self.name,
                    "scenario": scenario,
                    "driving_port": 1,
                    "left_rows": 0,
                    "right_claim_events": rows,
                    "exact_right_keys": exact_right_keys,
                    "post_insert_total_weight": rows,
                    "payload_bytes_per_row": payload_bytes,
                    "expected_output_rows": 0,
                    "purpose": purpose,
                })
            }
            Scenario::RightNullLeftHistory {
                left_rows,
                right_events,
                payload_bytes,
            } => right_null_left_history_context(self.name, left_rows, right_events, payload_bytes),
            Scenario::LeftNullRightHistory {
                right_rows,
                payload_bytes,
            } => left_null_right_history_context(self.name, right_rows, payload_bytes),
            Scenario::RightActiveOverlay {
                rows,
                payload_bytes,
            } => json!({
                "name": self.name,
                "scenario": "right_active_overlay_growth",
                "driving_port": 1,
                "seeded_left_rows": rows,
                "equality_partitions": rows,
                "right_claim_events": rows,
                "left_rows_rematched_per_event": 1,
                "candidates_per_rematch": 1,
                "payload_bytes_per_row": payload_bytes,
                "expected_insert_output": { "positive_rows": rows, "negative_rows": 0 },
                "expected_retract_output": { "positive_rows": 0, "negative_rows": rows },
                "purpose": "port-1 batch-prefix active-overlay growth isolated from historical fanout: each event adds one distinct equality partition and rematches exactly one left row against one candidate",
            }),
        }
    }
}

fn right_null_left_history_context(
    name: &'static str,
    left_rows: usize,
    right_events: usize,
    payload_bytes: usize,
) -> Value {
    json!({
        "name": name,
        "scenario": "right_null_left_history",
        "driving_port": 1,
        "seeded_left_rows": left_rows,
        "seeded_left_order": null,
        "right_claim_events": right_events,
        "payload_bytes_per_row": payload_bytes,
        "expected_output_rows": 0,
        "expected_max_turns": 1,
        "purpose": "prove right presence changes seek directly to matchable left-order rows instead of scanning persisted NULL-order history",
    })
}

fn left_null_right_history_context(
    name: &'static str,
    right_rows: usize,
    payload_bytes: usize,
) -> Value {
    json!({
        "name": name,
        "scenario": "left_null_right_history",
        "driving_port": 0,
        "seeded_right_rows": right_rows,
        "seeded_right_order": null,
        "left_claim_events": 1,
        "payload_bytes_per_row": payload_bytes,
        "expected_output_rows": 0,
        "expected_max_turns": 1,
        "purpose": "prove left selection seeks directly to matchable right-order rows instead of scanning persisted NULL-order history",
    })
}

impl Fixture {
    fn new(path: &Path, schema: &SchemaRef, residual: bool) -> Self {
        let definition = AsOfJoinDefinition::try_new(
            AsOfJoinKind::Inner,
            AsOfDirection::Backward { allow_exact: true },
            [AsOfEqualityKey::new(
                AsOfEqualityMode::Equal,
                col("group"),
                col("group"),
            )],
            [AsOfOrderKey::new(col("at"), col("at"))],
            std::iter::empty::<AsOfTieBreak>(),
            AsOfTieFallback::CanonicalAscending,
            None,
            [
                "left_group",
                "left_at",
                "left_value",
                "left_payload",
                "right_group",
                "right_at",
                "right_value",
                "right_payload",
            ],
            residual.then(|| col("right.value").gt(col("left.value"))),
        )
        .expect("define ASOF resource workload");
        let mut setup = StoreSetup::new();
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema), Arc::clone(schema)],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .expect("construct ASOF resource workload")
            .into_parts();
        let transactions = setup.commit(path, |_| Ok(())).expect("commit setup");
        drop((operation, transactions));
        let store = Store::open(path).expect("reopen observational store");
        let left_rows = store
            .open_data::<OrderedMap<Vec<u8>, u64>>("operation/asof_join.left_rows")
            .expect("open observational ASOF left rows");
        let right_rows = store
            .open_data::<OrderedMap<Vec<u8>, u64>>("operation/asof_join.right_rows")
            .expect("open observational ASOF right rows");
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema), Arc::clone(schema)],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .expect("open ASOF resource workload")
            .into_parts();
        let (transactions, reads) = store.into_transactions().split();
        Self {
            operation,
            transactions,
            reads,
            left_rows,
            right_rows,
        }
    }

    fn apply(&mut self, port: usize, change: &Change) -> ClaimMeasurement {
        let mut measurement = ClaimMeasurement::default();
        for _ in 0..1_000_000 {
            let (complete, output) = self.apply_once(port, change);
            measurement.observe(output.as_ref());
            if complete {
                return measurement;
            }
        }
        panic!("ASOF resource Claim did not complete")
    }

    fn apply_once(&mut self, port: usize, change: &Change) -> (bool, Option<Change>) {
        let Turn::Ready(prepared) = self
            .operation
            .turn(Some(OperationInput { port, change }))
            .expect("prepare ASOF resource turn")
        else {
            panic!("ASOF resource workload must be ready for a pinned input")
        };
        let transaction = self.transactions.begin();
        let (action, completion) = prepared
            .apply(transaction.access())
            .expect("apply ASOF resource turn");
        transaction.commit().expect("commit ASOF resource turn");
        completion.run().expect("complete ASOF resource turn");
        match action {
            Action::Commit(output) => (false, output),
            Action::Complete(output) => (true, output),
            Action::Idle => panic!("ASOF resource workload returned Idle for a pinned input"),
        }
    }

    fn state_snapshot(&self) -> StateSnapshot {
        let transaction = self.reads.begin();
        StateSnapshot {
            left: scan_map(
                &self
                    .left_rows
                    .read(transaction.access())
                    .expect("read ASOF left rows"),
            ),
            right: scan_map(
                &self
                    .right_rows
                    .read(transaction.access())
                    .expect("read ASOF right rows"),
            ),
        }
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
                std::cmp::Ordering::Less => {
                    self.negative_rows += 1;
                    self.negative_weight = self
                        .negative_weight
                        .checked_add(difference.unsigned_abs())
                        .expect("observed ASOF negative output weight fits u64");
                }
                std::cmp::Ordering::Greater => {
                    self.positive_rows += 1;
                    self.positive_weight = self
                        .positive_weight
                        .checked_add(difference.unsigned_abs())
                        .expect("observed ASOF positive output weight fits u64");
                }
                std::cmp::Ordering::Equal => panic!("Change admitted a zero difference"),
            }
        }
    }
}

fn assert_claim(claim: &ClaimMeasurement, expected: ExpectedOutput, expected_min_turns: usize) {
    assert_eq!(claim.positive_rows, expected.positive_rows);
    assert_eq!(claim.negative_rows, expected.negative_rows);
    assert_eq!(
        claim.positive_weight,
        u64::try_from(expected.positive_rows).expect("expected ASOF positive weight fits u64")
    );
    assert_eq!(
        claim.negative_weight,
        u64::try_from(expected.negative_rows).expect("expected ASOF negative weight fits u64")
    );
    assert_eq!(
        claim.output_rows,
        expected
            .positive_rows
            .checked_add(expected.negative_rows)
            .expect("expected ASOF output row count fits usize")
    );
    assert!(claim.turns >= expected_min_turns);
}

fn assert_scenario_turn_bound(spec: CaseSpec, claim: &ClaimMeasurement) {
    match spec.scenario {
        Scenario::RightNullLeftHistory { .. } => assert!(
            claim.turns <= 1,
            "persisted NULL-order left history must not add right-Claim turns"
        ),
        Scenario::LeftNullRightHistory { .. } => assert!(
            claim.turns <= 1,
            "persisted NULL-order right history must not add left-Claim turns"
        ),
        _ => {}
    }
}

impl StatePeaks {
    fn observe(&mut self, snapshot: StateSnapshot) {
        self.left_entries = self.left_entries.max(snapshot.left.entries);
        self.right_entries = self.right_entries.max(snapshot.right.entries);
        self.total_entries = self
            .total_entries
            .max(snapshot.left.entries.saturating_add(snapshot.right.entries));
        self.logical_key_value_bytes = self.logical_key_value_bytes.max(
            snapshot
                .left
                .logical_key_value_bytes
                .saturating_add(snapshot.right.logical_key_value_bytes),
        );
    }
}

fn scan_map(map: &dogpaddle_store::OrderedMapReadAccess<'_, Vec<u8>, u64>) -> MapSnapshot {
    let limit = ScanLimit::new(STATE_SCAN_ITEMS, STATE_SCAN_BYTES)
        .expect("positive ASOF state scan limits are valid");
    let mut resume_after = None;
    let mut snapshot = MapSnapshot::default();
    loop {
        let page = map
            .scan(.., ScanDirection::Ascending, resume_after.as_ref(), limit)
            .expect("scan ASOF logical rows");
        for (key, value) in page.entries {
            snapshot.entries += 1;
            snapshot.zero_value_entries += usize::from(value == 0);
            snapshot.total_weight = snapshot
                .total_weight
                .checked_add(value)
                .expect("ASOF observed total row weight fits u64");
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

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::UInt64, false),
        Field::new("at", DataType::Int64, true),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn change(
    schema: &SchemaRef,
    groups: Vec<u64>,
    orders: Vec<i64>,
    values: Vec<i64>,
    payload_bytes: usize,
    difference: i64,
) -> Change {
    change_with_optional_orders(
        schema,
        groups,
        orders.into_iter().map(Some).collect(),
        values,
        payload_bytes,
        difference,
    )
}

fn change_with_optional_orders(
    schema: &SchemaRef,
    groups: Vec<u64>,
    orders: Vec<Option<i64>>,
    values: Vec<i64>,
    payload_bytes: usize,
    difference: i64,
) -> Change {
    assert_eq!(groups.len(), orders.len());
    assert_eq!(groups.len(), values.len());
    let rows = groups.len();
    let payload = "x".repeat(payload_bytes);
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(groups)),
            Arc::new(Int64Array::from(orders)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(vec![payload.as_str(); rows])),
        ],
    )
    .expect("build ASOF resource records");
    Change::try_new(records, Int64Array::from(vec![difference; rows]))
        .expect("build ASOF resource Change")
}

fn ordinals(rows: usize) -> Vec<i64> {
    (0..rows)
        .map(|value| i64::try_from(value).expect("ASOF resource ordinal fits i64"))
        .collect()
}

#[expect(
    clippy::too_many_lines,
    reason = "keeping each resource scenario's complete fixture and driving Claim together makes the measurement boundary auditable"
)]
fn prepare_workload(spec: CaseSpec, path: &Path) -> Workload {
    let schema = schema();
    match spec.scenario {
        Scenario::LeftLookup {
            candidates,
            candidate_payload_bytes,
            residual_far_fallback,
        } => {
            let mut fixture = Fixture::new(path, &schema, residual_far_fallback);
            let orders = ordinals(candidates);
            let values = if residual_far_fallback {
                let mut values = vec![-1; candidates];
                values[0] = 1;
                values
            } else {
                orders.clone()
            };
            fixture.apply(
                1,
                &change(
                    &schema,
                    vec![7; candidates],
                    orders,
                    values,
                    candidate_payload_bytes,
                    1,
                ),
            );
            let probe = i64::try_from(candidates).expect("candidate count fits i64");
            Workload {
                fixture,
                port: 0,
                inserted: change(&schema, vec![7], vec![probe], vec![0], 0, 1),
                retracted: change(&schema, vec![7], vec![probe], vec![0], 0, -1),
                expected_insert_output: ExpectedOutput {
                    positive_rows: 1,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 1,
                },
                expected_min_turns: 1,
                expected_left_after_insert: None,
                expected_right_after_insert: None,
            }
        }
        Scenario::WholeClaim {
            rows,
            payload_bytes,
        } => {
            let fixture = Fixture::new(path, &schema, false);
            let orders = ordinals(rows);
            Workload {
                fixture,
                port: 0,
                inserted: change(
                    &schema,
                    vec![7; rows],
                    orders.clone(),
                    orders.clone(),
                    payload_bytes,
                    1,
                ),
                retracted: change(
                    &schema,
                    vec![7; rows],
                    orders.clone(),
                    orders,
                    payload_bytes,
                    -1,
                ),
                expected_insert_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_min_turns: 1,
                expected_left_after_insert: None,
                expected_right_after_insert: None,
            }
        }
        Scenario::RightHistoricalRematch {
            left_rows,
            right_versions,
            payload_bytes,
        } => {
            let mut fixture = Fixture::new(path, &schema, false);
            let mut right_orders = vec![0];
            while right_orders.len() < right_versions {
                right_orders.push(10_000_i64.saturating_add(
                    i64::try_from(right_orders.len()).expect("right ordinal fits i64"),
                ));
            }
            fixture.apply(
                1,
                &change(
                    &schema,
                    vec![7; right_orders.len()],
                    right_orders.clone(),
                    right_orders,
                    payload_bytes,
                    1,
                ),
            );
            // Seed candidates before probes so fixture construction reaches the same relation
            // without benchmarking every intermediate right-presence transition.
            let left_orders = (2..left_rows.saturating_add(2))
                .map(|value| i64::try_from(value).expect("left ordinal fits i64"))
                .collect::<Vec<_>>();
            fixture.apply(
                0,
                &change(
                    &schema,
                    vec![7; left_rows],
                    left_orders.clone(),
                    left_orders,
                    payload_bytes,
                    1,
                ),
            );
            Workload {
                fixture,
                port: 1,
                inserted: change(&schema, vec![7], vec![1], vec![1], payload_bytes, 1),
                retracted: change(&schema, vec![7], vec![1], vec![1], payload_bytes, -1),
                expected_insert_output: ExpectedOutput {
                    positive_rows: left_rows,
                    negative_rows: left_rows,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: left_rows,
                    negative_rows: left_rows,
                },
                expected_min_turns: 1,
                expected_left_after_insert: None,
                expected_right_after_insert: None,
            }
        }
        Scenario::RightEmptyLeftPreload {
            rows,
            payload_bytes,
            key_shape,
        } => {
            let fixture = Fixture::new(path, &schema, false);
            let (groups, orders, values, expected_entries) = match key_shape {
                RightKeyShape::Distinct => (vec![7; rows], ordinals(rows), ordinals(rows), rows),
                RightKeyShape::Same => (vec![7; rows], vec![0; rows], vec![0; rows], 1),
            };
            Workload {
                fixture,
                port: 1,
                inserted: change(
                    &schema,
                    groups.clone(),
                    orders.clone(),
                    values.clone(),
                    payload_bytes,
                    1,
                ),
                retracted: change(&schema, groups, orders, values, payload_bytes, -1),
                expected_insert_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_min_turns: rows.div_ceil(256),
                expected_left_after_insert: Some((0, 0)),
                expected_right_after_insert: Some((expected_entries, rows as u64)),
            }
        }
        Scenario::RightNullLeftHistory {
            left_rows,
            right_events,
            payload_bytes,
        } => {
            let mut fixture = Fixture::new(path, &schema, false);
            let seeded = fixture.apply(
                0,
                &change_with_optional_orders(
                    &schema,
                    vec![7; left_rows],
                    vec![None; left_rows],
                    ordinals(left_rows),
                    payload_bytes,
                    1,
                ),
            );
            assert_eq!(seeded.output_rows, 0);
            let orders = ordinals(right_events);
            Workload {
                fixture,
                port: 1,
                inserted: change(
                    &schema,
                    vec![7; right_events],
                    orders.clone(),
                    orders.clone(),
                    payload_bytes,
                    1,
                ),
                retracted: change(
                    &schema,
                    vec![7; right_events],
                    orders.clone(),
                    orders,
                    payload_bytes,
                    -1,
                ),
                expected_insert_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_min_turns: 1,
                expected_left_after_insert: Some((left_rows, left_rows as u64)),
                expected_right_after_insert: Some((right_events, right_events as u64)),
            }
        }
        Scenario::LeftNullRightHistory {
            right_rows,
            payload_bytes,
        } => {
            let mut fixture = Fixture::new(path, &schema, false);
            let seeded = fixture.apply(
                1,
                &change_with_optional_orders(
                    &schema,
                    vec![7; right_rows],
                    vec![None; right_rows],
                    ordinals(right_rows),
                    payload_bytes,
                    1,
                ),
            );
            assert_eq!(seeded.output_rows, 0);
            Workload {
                fixture,
                port: 0,
                inserted: change(&schema, vec![7], vec![1], vec![1], payload_bytes, 1),
                retracted: change(&schema, vec![7], vec![1], vec![1], payload_bytes, -1),
                expected_insert_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: 0,
                },
                expected_min_turns: 1,
                expected_left_after_insert: Some((1, 1)),
                expected_right_after_insert: Some((right_rows, right_rows as u64)),
            }
        }
        Scenario::RightActiveOverlay {
            rows,
            payload_bytes,
        } => {
            let mut fixture = Fixture::new(path, &schema, false);
            let groups = (0..rows)
                .map(|value| u64::try_from(value).expect("ASOF resource group fits u64"))
                .collect::<Vec<_>>();
            let seeded = fixture.apply(
                0,
                &change(
                    &schema,
                    groups.clone(),
                    vec![1; rows],
                    vec![1; rows],
                    payload_bytes,
                    1,
                ),
            );
            assert_eq!(seeded.output_rows, 0);
            Workload {
                fixture,
                port: 1,
                inserted: change(
                    &schema,
                    groups.clone(),
                    vec![0; rows],
                    vec![0; rows],
                    payload_bytes,
                    1,
                ),
                retracted: change(
                    &schema,
                    groups,
                    vec![0; rows],
                    vec![0; rows],
                    payload_bytes,
                    -1,
                ),
                expected_insert_output: ExpectedOutput {
                    positive_rows: rows,
                    negative_rows: 0,
                },
                expected_retract_output: ExpectedOutput {
                    positive_rows: 0,
                    negative_rows: rows,
                },
                expected_min_turns: rows.div_ceil(256),
                expected_left_after_insert: Some((rows, rows as u64)),
                expected_right_after_insert: Some((rows, rows as u64)),
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
        inserted,
        expected_insert_output,
        expected_min_turns,
        ..
    } = prepare_workload(spec, path);
    let input_arrow_bytes = change_arrow_bytes(&inserted);
    let profiler = dhat::Profiler::builder().testing().build();
    let claim = std::hint::black_box(fixture.apply(port, &inserted));
    let stats = dhat::HeapStats::get();
    drop(profiler);
    assert_claim(&claim, expected_insert_output, expected_min_turns);
    assert_scenario_turn_bound(spec, &claim);
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

fn assert_groupwise_output(
    output: Option<&Change>,
    oracle: GroupwiseOutputOracle,
    seen_groups: &mut [bool],
) {
    let Some(output) = output else {
        return;
    };
    let records = output.records();
    assert_eq!(records.num_columns(), 8);
    let left_groups = records
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("ASOF left group output is UInt64");
    let left_orders = records
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ASOF left order output is Int64");
    let left_values = records
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ASOF left value output is Int64");
    let right_groups = records
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("ASOF right group output is UInt64");
    let right_orders = records
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ASOF right order output is Int64");
    let right_values = records
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ASOF right value output is Int64");
    for row in 0..output.num_rows() {
        assert!(!left_groups.is_null(row));
        assert!(!right_groups.is_null(row));
        let group = usize::try_from(left_groups.value(row))
            .expect("observed ASOF group ordinal fits usize");
        assert!(group < oracle.groups);
        assert_eq!(right_groups.value(row), left_groups.value(row));
        assert!(!seen_groups[group]);
        seen_groups[group] = true;
        assert_eq!(left_orders.value(row), 1);
        assert_eq!(left_values.value(row), 1);
        assert_eq!(right_orders.value(row), 0);
        assert_eq!(right_values.value(row), 0);
        assert_eq!(output.diffs().value(row), oracle.difference);
    }
}

fn run_observed_claim(
    fixture: &mut Fixture,
    port: usize,
    input: &Change,
    peaks: &mut StatePeaks,
    groupwise_output: Option<GroupwiseOutputOracle>,
) -> ClaimMeasurement {
    let mut measurement = ClaimMeasurement::default();
    let mut seen_groups = groupwise_output.map(|oracle| vec![false; oracle.groups]);
    for _ in 0..1_000_000 {
        let (complete, output) = fixture.apply_once(port, input);
        measurement.observe(output.as_ref());
        if let (Some(oracle), Some(seen_groups)) = (groupwise_output, seen_groups.as_mut()) {
            assert_groupwise_output(output.as_ref(), oracle, seen_groups);
        }
        peaks.observe(fixture.state_snapshot());
        if complete {
            if let Some(seen_groups) = seen_groups {
                assert!(seen_groups.into_iter().all(std::convert::identity));
            }
            return measurement;
        }
    }
    panic!("observed ASOF resource Claim did not complete")
}

fn measure_persistent_state(spec: CaseSpec, path: &Path) -> PersistentStateMeasurement {
    let Workload {
        mut fixture,
        port,
        inserted,
        retracted,
        expected_insert_output,
        expected_retract_output,
        expected_min_turns,
        expected_left_after_insert,
        expected_right_after_insert,
    } = prepare_workload(spec, path);
    let groupwise_output_groups = match spec.scenario {
        Scenario::RightActiveOverlay { rows, .. } => Some(rows),
        _ => None,
    };
    let before = fixture.state_snapshot();
    let mut peaks = StatePeaks::default();
    peaks.observe(before);
    let inserted_result = run_observed_claim(
        &mut fixture,
        port,
        &inserted,
        &mut peaks,
        groupwise_output_groups.map(|groups| GroupwiseOutputOracle {
            groups,
            difference: 1,
        }),
    );
    assert_claim(&inserted_result, expected_insert_output, expected_min_turns);
    assert_scenario_turn_bound(spec, &inserted_result);
    let after_insert = fixture.state_snapshot();
    if let Some((expected_entries, expected_weight)) = expected_left_after_insert {
        assert_eq!(after_insert.left.entries, expected_entries);
        assert_eq!(after_insert.left.total_weight, expected_weight);
    }
    if let Some((expected_entries, expected_weight)) = expected_right_after_insert {
        assert_eq!(after_insert.right.entries, expected_entries);
        assert_eq!(after_insert.right.total_weight, expected_weight);
    }
    let retracted_result = run_observed_claim(
        &mut fixture,
        port,
        &retracted,
        &mut peaks,
        groupwise_output_groups.map(|groups| GroupwiseOutputOracle {
            groups,
            difference: -1,
        }),
    );
    assert_claim(
        &retracted_result,
        expected_retract_output,
        expected_min_turns,
    );
    assert_scenario_turn_bound(spec, &retracted_result);
    let after_retract = fixture.state_snapshot();
    assert_eq!(after_retract, before);
    assert_eq!(after_insert.left.zero_value_entries, 0);
    assert_eq!(after_insert.right.zero_value_entries, 0);
    PersistentStateMeasurement {
        collections: [LEFT_ROWS, RIGHT_ROWS],
        coverage: "decoded logical row-map entries and key-plus-u64 bytes observed after every committed turn in a separate unprofiled pass; excludes continuation bytes and RocksDB/WAL/LSM/cache/filesystem bytes",
        peak_left_entries: peaks.left_entries,
        peak_right_entries: peaks.right_entries,
        peak_total_entries: peaks.total_entries,
        peak_logical_key_value_bytes: peaks.logical_key_value_bytes,
        before,
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
    let (input_arrow_bytes, claim, rust_heap) = measure_heap(spec, &sample.join("heap-store"));
    let persistent_state = measure_persistent_state(spec, &sample.join("state-store"));
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
        rss_status: "unavailable: this portable runner does not sample process RSS; allocator bytes, Arrow bytes, and logical Store bytes must not be interpreted as RSS",
    }
}

fn test_cases() -> Vec<CaseSpec> {
    vec![
        CaseSpec::left_lookup("candidate_page_boundary", 65, 0, false),
        CaseSpec::left_lookup("wide_candidates", 8, 4 * 1024, false),
        CaseSpec::left_lookup("oversized_candidate", 1, 1024 * 1024 + 1, false),
        CaseSpec::left_lookup("residual_far_fallback", 65, 0, true),
        CaseSpec::whole_claim("whole_claim", 33, 256),
        CaseSpec::right_historical_rematch("right_historical_rematch", 17, 17, 0),
        CaseSpec::right_empty_left_distinct("right_empty_distinct_base", 257, 0),
        CaseSpec::right_empty_left_distinct("right_empty_distinct_double", 514, 0),
        CaseSpec::right_empty_left_same_key("right_empty_same_key", 514, 0),
        CaseSpec::right_null_left_history("right_null_left_base", 257, 129, 0),
        CaseSpec::right_null_left_history("right_null_left_double", 514, 129, 0),
        CaseSpec::left_null_right_history("left_null_right_base", 257, 0),
        CaseSpec::left_null_right_history("left_null_right_double", 514, 0),
        CaseSpec::right_active_overlay("right_active_overlay_base", 129, 0),
        CaseSpec::right_active_overlay("right_active_overlay_double", 258, 0),
    ]
}

fn benchmark_cases(profile: PerformanceProfile) -> Vec<CaseSpec> {
    let (
        candidates,
        wide_candidates,
        wide_payload,
        claim_rows,
        claim_payload,
        rematch_rows,
        right_preload_rows,
        right_active_rows,
    ) = match profile {
        PerformanceProfile::Smoke => (128, 16, 16 * 1024, 129, 1024, 65, 257, 129),
        PerformanceProfile::Reference => (1_024, 64, 64 * 1024, 1_024, 4 * 1024, 129, 1_024, 512),
    };
    vec![
        CaseSpec::left_lookup("candidate_pages", candidates, 0, false),
        CaseSpec::left_lookup("wide_candidates", wide_candidates, wide_payload, false),
        CaseSpec::left_lookup("oversized_candidate", 1, 1024 * 1024 + 1, false),
        CaseSpec::left_lookup("residual_far_fallback", candidates, 0, true),
        CaseSpec::whole_claim("whole_claim", claim_rows, claim_payload),
        CaseSpec::right_historical_rematch(
            "right_historical_rematch",
            rematch_rows,
            rematch_rows,
            0,
        ),
        CaseSpec::right_empty_left_distinct("right_empty_distinct_base", right_preload_rows, 0),
        CaseSpec::right_empty_left_distinct(
            "right_empty_distinct_double",
            right_preload_rows.saturating_mul(2),
            0,
        ),
        CaseSpec::right_empty_left_same_key(
            "right_empty_same_key",
            right_preload_rows.saturating_mul(2),
            0,
        ),
        CaseSpec::right_null_left_history("right_null_left_base", right_preload_rows, 129, 0),
        CaseSpec::right_null_left_history(
            "right_null_left_double",
            right_preload_rows.saturating_mul(2),
            129,
            0,
        ),
        CaseSpec::left_null_right_history("left_null_right_base", right_preload_rows, 0),
        CaseSpec::left_null_right_history(
            "left_null_right_double",
            right_preload_rows.saturating_mul(2),
            0,
        ),
        CaseSpec::right_active_overlay("right_active_overlay_base", right_active_rows, 0),
        CaseSpec::right_active_overlay(
            "right_active_overlay_double",
            right_active_rows.saturating_mul(2),
            0,
        ),
    ]
}

fn cases(profile: PerformanceProfile, invocation: Invocation) -> Vec<CaseSpec> {
    if invocation == Invocation::Test {
        test_cases()
    } else {
        benchmark_cases(profile)
    }
}

fn parse_profile(value: &str) -> PerformanceProfile {
    match value {
        "smoke" => PerformanceProfile::Smoke,
        "reference" => PerformanceProfile::Reference,
        _ => panic!("unknown ASOF resource profile {value:?}"),
    }
}

fn run_child(arguments: &[OsString]) {
    assert_eq!(arguments.len(), 5, "invalid ASOF resource child arguments");
    let case = arguments[1]
        .to_str()
        .expect("ASOF resource case name must be Unicode");
    let profile = parse_profile(
        arguments[2]
            .to_str()
            .expect("ASOF resource profile must be Unicode"),
    );
    let invocation = Invocation::parse(
        arguments[3]
            .to_str()
            .expect("ASOF resource invocation must be Unicode"),
    );
    let sample = PathBuf::from(&arguments[4]);
    let spec = cases(profile, invocation)
        .into_iter()
        .find(|spec| spec.name == case)
        .unwrap_or_else(|| panic!("unknown ASOF resource case {case:?}"));
    let record = measure_case(spec, &sample, profile, invocation);
    serde_json::to_writer(std::io::stdout().lock(), &record)
        .expect("write ASOF resource child result");
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
        .expect("start ASOF resource child");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "ASOF resource child {:?} exited with {}: {stderr}",
            spec.name, output.status
        );
    }
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "decode ASOF resource child {:?}: {error}; stdout={}",
            spec.name,
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn observe_null_history_pair(baseline: &mut Option<Value>, record: &Value, description: &str) {
    let Some(baseline) = baseline.take() else {
        *baseline = Some(record.clone());
        return;
    };
    assert_eq!(
        baseline.get("input_arrow_bytes"),
        record.get("input_arrow_bytes"),
        "{description} must use the same driving Arrow input"
    );
    assert_eq!(
        baseline.get("claim"),
        record.get("claim"),
        "{description} must not add turns or output work when NULL history doubles"
    );
    assert_eq!(
        baseline.get("rust_heap"),
        record.get("rust_heap"),
        "{description} must not add Rust allocator work when NULL history doubles"
    );
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
            "rust_heap": "Each fresh child builds its fixture, seed, and input before starting one dhat Profiler. Stats cover allocations made through Rust's global allocator during one complete driving Claim. They exclude input Arrow allocation, pre-existing fixture/seed memory, and RocksDB native allocations.",
            "persistent_logical_state": "A second unprofiled fixture scans asof_join.left_rows and asof_join.right_rows after every committed turn. Counts and decoded key-plus-u64 bytes exclude continuation, RocksDB cache, WAL, LSM, compression, tombstones, and filesystem allocation.",
            "output_arrow_bytes": "Arrow get_array_memory_size plus the diff array, summed for emitted Changes. Arrow may count shared buffers more than once.",
            "turns": "Committed turns needed to finish the driving Claim; Store scan-page and read/write logical-byte counters are not exposed.",
            "rss": "Unavailable. The portable owner runner intentionally does not treat allocator counters, logical Store bytes, ps samples, or platform-specific high-water units as process RSS.",
            "comparison": "Only benchmark-mode records from the same code, rustc, host, profile, filesystem, workload, and baseline epoch are comparable. Test-mode heap values validate the protocol only."
        },
        "cases": specs.iter().copied().map(CaseSpec::context).collect::<Vec<_>>(),
    });
    fs::write(
        root.path().join("resource-context.json"),
        serde_json::to_vec_pretty(&context).expect("encode ASOF resource context"),
    )
    .expect("write ASOF resource context");
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
    let executable = std::env::current_exe().expect("locate ASOF resource executable");
    let result_file = File::create(root.path().join("resources.jsonl"))
        .expect("create ASOF resource result file");
    let mut result_file = BufWriter::new(result_file);
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let mut right_null_left_baseline = None;
    let mut left_null_right_baseline = None;
    for spec in specs {
        eprintln!("{BENCHMARK}: measuring {}", spec.name);
        let sample = root.sample(spec.name);
        let record = invoke_child(&executable, profile, invocation, spec, sample.path());
        serde_json::to_writer(&mut result_file, &record).expect("write ASOF resource file record");
        result_file
            .write_all(b"\n")
            .expect("terminate ASOF resource file record");
        result_file
            .flush()
            .expect("flush ASOF resource file record");
        serde_json::to_writer(&mut stdout, &record).expect("write ASOF resource stdout record");
        stdout
            .write_all(b"\n")
            .expect("terminate ASOF resource stdout record");
        stdout.flush().expect("flush ASOF resource stdout");
        match spec.scenario {
            Scenario::RightNullLeftHistory { .. } => observe_null_history_pair(
                &mut right_null_left_baseline,
                &record,
                "right Claim over NULL-order left history",
            ),
            Scenario::LeftNullRightHistory { .. } => observe_null_history_pair(
                &mut left_null_right_baseline,
                &record,
                "left Claim over NULL-order right history",
            ),
            _ => {}
        }
    }
    assert!(
        right_null_left_baseline.is_none() && left_null_right_baseline.is_none(),
        "each NULL-history resource scenario must have one base/double pair"
    );
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
