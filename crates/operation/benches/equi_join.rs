//! EquiJoin-owned match and key-presence transition workloads with synchronous commits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkGroup, Criterion, Throughput, measurement::WallTime};
use datafusion_expr::{Expr, col};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, create_operation,
    operation::{
        Action, Operation, OperationInput, Turn,
        transform::{EquiJoinDefinition, EquiJoinKind},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Store, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "equi_join";

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    _root: TempDir,
}

#[derive(Default)]
struct ClaimResult {
    output_rows: usize,
    turns: usize,
    positive_rows: usize,
    negative_rows: usize,
}

impl Fixture {
    fn new(root: &RunRoot, kind: EquiJoinKind, schema: &SchemaRef, residual: Option<Expr>) -> Self {
        let sample = root.sample(BENCHMARK);
        let output_names: &[&str] = match kind {
            EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti => &["left_key", "left_value"],
            EquiJoinKind::Inner | EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => {
                &["left_key", "left_value", "right_key", "right_value"]
            }
        };
        let definition = EquiJoinDefinition::try_new(
            kind,
            [(col("key"), col("key"))],
            output_names.iter().copied(),
            residual,
        )
        .expect("define equi-join");
        let binding = (&definition as &dyn OperationDefinition)
            .bind(&[Arc::clone(schema), Arc::clone(schema)])
            .expect("bind equi-join");
        let mut setup = Store::setup(sample.path().join("store")).expect("create store setup");
        let operation = create_operation(binding, &mut setup, "operation", RuntimeResource::none())
            .expect("create equi-join");
        let transactions = setup.commit(|_| Ok(())).expect("commit store setup");
        Self {
            operation,
            transactions,
            _root: sample,
        }
    }

    fn apply(&mut self, port: usize, change: &Change) -> ClaimResult {
        self.apply_inner(port, change, false)
    }

    fn apply_checked(&mut self, port: usize, change: &Change) -> ClaimResult {
        self.apply_inner(port, change, true)
    }

    fn apply_inner(&mut self, port: usize, change: &Change, check_diffs: bool) -> ClaimResult {
        let mut result = ClaimResult::default();
        loop {
            let Turn::Ready(prepared) = self
                .operation
                .turn(Some(OperationInput { port, change }))
                .expect("prepare equi-join")
            else {
                panic!("equi-join must be ready")
            };
            let transaction = self.transactions.begin();
            let (action, completion) = prepared
                .apply(transaction.access())
                .expect("apply equi-join");
            transaction.commit().expect("commit equi-join");
            completion.run().expect("complete equi-join");
            result.turns += 1;
            match action {
                Action::Commit(output) => {
                    result.record(output.as_ref(), check_diffs);
                }
                Action::Complete(output) => {
                    result.record(output.as_ref(), check_diffs);
                    return result;
                }
                Action::Idle => panic!("equi-join returned Idle for a pinned input"),
            }
        }
    }
}

impl ClaimResult {
    fn record(&mut self, output: Option<&Change>, check_diffs: bool) {
        let Some(output) = output else {
            return;
        };
        self.output_rows += output.num_rows();
        if check_diffs {
            for difference in output.diffs().values() {
                match difference.cmp(&0) {
                    std::cmp::Ordering::Less => self.negative_rows += 1,
                    std::cmp::Ordering::Greater => self.positive_rows += 1,
                    std::cmp::Ordering::Equal => panic!("Change admitted a zero difference"),
                }
            }
        }
    }
}

fn change(schema: &SchemaRef, key: u64, values: Vec<i64>, difference: i64) -> Change {
    let rows = values.len();
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(vec![key; rows])),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("build equi-join records");
    Change::try_new(records, Int64Array::from(vec![difference; rows]))
        .expect("build equi-join Change")
}

fn right_toggle(schema: &SchemaRef, value: i64) -> (Change, Change) {
    (
        change(schema, 7, vec![value], 1),
        change(schema, 7, vec![value], -1),
    )
}

fn validate_pair(first: &ClaimResult, second: &ClaimResult, expected_rows: usize) {
    assert_eq!(first.output_rows + second.output_rows, expected_rows);
    assert!(first.turns > 0);
    assert!(second.turns > 0);
}

fn validate_directions(first: &ClaimResult, second: &ClaimResult, expected: [(usize, usize); 2]) {
    assert_eq!((first.positive_rows, first.negative_rows), expected[0]);
    assert_eq!((second.positive_rows, second.negative_rows), expected[1]);
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, fanout: usize) {
    let half = fanout / 2;
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "same_key_left_rows": fanout,
            "criterion_samples": 10,
            "warmup_ms": 100,
            "measurement_ms": match profile {
                PerformanceProfile::Smoke => 200,
                PerformanceProfile::Reference => 5_000,
            },
            "timed_boundary": "two complete Claims, all Probe/ClearShadow/Emit turns, synchronous commits, AfterCommit",
            "throughput_unit": "Claims (two per timed iteration)",
            "untimed": "fixture, relation seed, warmup, output validation, teardown",
            "cases": {
                "inner_first_last_match": "Inner control without key-count traffic",
                "left_semi_presence_stable": "exact right row changes while another right row preserves key presence",
                "left_semi_first_last_match": "first/last right match publishes every preserved left row",
                "full_outer_first_last_match": "first/last right match flips null-padded output for every left row",
                "inner_residual_zero_selectivity": "residual rejects every equality-key candidate",
                "inner_residual_half_selective": "residual accepts half of equality-key candidates without match-count state",
                "inner_residual_full_selectivity": "residual accepts every equality-key candidate",
                "left_semi_residual_presence_stable": "same exact right row changes multiplicity without changing any left support",
                "left_semi_residual_left_presence_stable": "same exact left row changes multiplicity and reads its existing support without rescanning right rows",
                "left_semi_residual_partial_transition": "one right row changes match support for only half of preserved left rows",
                "full_outer_residual_partial_transition": "one right row changes two-sided match support for half of the candidate pairs"
            },
            "residual_workloads": {
                "predicate": "left.value < right.value for every residual case; the driving right.value changes selectivity without changing the expression shape",
                "scope": "one timed iteration is a +1 Claim followed by its -1 Claim; right-stable keeps one identical right row present, left-stable keeps the left row and a right fanout present",
                "scanning_case_candidate_pairs_per_iteration": 2 * fanout,
                "scanning_case_predicate_candidate_evaluations_per_iteration": 4 * fanout,
                "selectivity_cases": [0.0, 0.5, 1.0],
                "match_count_traffic": {
                    "inner_residual_half_selective": {
                        "tracked_sides_per_qualifying_pair": 0,
                        "shadow_adjustments_per_iteration": 0,
                        "actual_adjustments_per_iteration": 0,
                        "shadow_cleanup_entries_per_iteration": 0
                    },
                    "left_semi_residual_presence_stable": {
                        "candidate_scans_per_iteration": 0,
                        "match_count_writes_per_iteration": 0
                    },
                    "left_semi_residual_left_presence_stable": {
                        "candidate_scans_per_iteration": 0,
                        "match_count_writes_per_iteration": 0,
                        "match_count_reads": "constant per Probe and Emit, independent of right fanout"
                    },
                    "left_semi_residual_partial_transition": {
                        "tracked_sides_per_qualifying_pair": 1,
                        "shadow_adjustments_per_iteration": 2 * half,
                        "actual_adjustments_per_iteration": 2 * half,
                        "shadow_cleanup_entries_per_iteration": 2 * half
                    },
                    "full_outer_residual_partial_transition": {
                        "tracked_sides_per_qualifying_pair": 2,
                        "opposite_row_shadow_adjustments_per_iteration": 2 * half,
                        "opposite_row_actual_adjustments_per_iteration": 2 * half,
                        "driving_row_adjustments": "one checked adjustment per qualifying scan page in Probe and Emit",
                        "shadow_cleanup_entries_per_iteration": 2 * (half + 1)
                    }
                }
            }
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode context"),
    )
    .expect("write context");
}

fn benchmark_pure_equi(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    left_seed: &Change,
    toggled_right: &Change,
    retracted_right: &Change,
    stable_right: &Change,
) {
    let fanout = left_seed.num_rows();
    for (name, kind, stable_presence, expected_rows, expected_directions) in [
        (
            "inner_first_last_match",
            EquiJoinKind::Inner,
            false,
            2 * fanout,
            [(fanout, 0), (0, fanout)],
        ),
        (
            "left_semi_presence_stable",
            EquiJoinKind::LeftSemi,
            true,
            0,
            [(0, 0), (0, 0)],
        ),
        (
            "left_semi_first_last_match",
            EquiJoinKind::LeftSemi,
            false,
            2 * fanout,
            [(fanout, 0), (0, fanout)],
        ),
        (
            "full_outer_first_last_match",
            EquiJoinKind::FullOuter,
            false,
            4 * fanout,
            [(fanout, fanout), (fanout, fanout)],
        ),
    ] {
        let mut fixture = Fixture::new(root, kind, schema, None);
        let seed = fixture.apply(0, left_seed);
        assert!(seed.turns > 0);
        if stable_presence {
            let anchor = fixture.apply(1, stable_right);
            assert!(anchor.turns > 0);
        }
        let first = fixture.apply_checked(1, toggled_right);
        let second = fixture.apply_checked(1, retracted_right);
        validate_pair(&first, &second, expected_rows);
        validate_directions(&first, &second, expected_directions);
        group.bench_function(name, |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let first = fixture.apply(1, toggled_right);
                    let second = fixture.apply(1, retracted_right);
                    elapsed += started.elapsed();
                    validate_pair(&first, &second, expected_rows);
                }
                elapsed
            });
        });
    }
}

fn benchmark_residual(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    left_seed: &Change,
    right_seed: &Change,
) {
    let fanout = left_seed.num_rows();
    let half = fanout / 2;
    let threshold = i64::try_from(half).expect("benchmark fanout fits i64");
    let residual = col("left.value").lt(col("right.value"));
    let (zero_right, retracted_zero_right) = right_toggle(schema, 0);
    let (half_right, retracted_half_right) = right_toggle(schema, threshold);
    let full_value = i64::try_from(fanout).expect("benchmark fanout fits i64");
    let (full_right, retracted_full_right) = right_toggle(schema, full_value);
    for (
        name,
        kind,
        toggled_right,
        retracted_right,
        stable_presence,
        expected_rows,
        expected_directions,
    ) in [
        (
            "inner_residual_zero_selectivity",
            EquiJoinKind::Inner,
            &zero_right,
            &retracted_zero_right,
            false,
            0,
            [(0, 0), (0, 0)],
        ),
        (
            "inner_residual_half_selective",
            EquiJoinKind::Inner,
            &half_right,
            &retracted_half_right,
            false,
            2 * half,
            [(half, 0), (0, half)],
        ),
        (
            "inner_residual_full_selectivity",
            EquiJoinKind::Inner,
            &full_right,
            &retracted_full_right,
            false,
            2 * fanout,
            [(fanout, 0), (0, fanout)],
        ),
        (
            "left_semi_residual_presence_stable",
            EquiJoinKind::LeftSemi,
            &full_right,
            &retracted_full_right,
            true,
            0,
            [(0, 0), (0, 0)],
        ),
        (
            "left_semi_residual_partial_transition",
            EquiJoinKind::LeftSemi,
            &half_right,
            &retracted_half_right,
            false,
            2 * half,
            [(half, 0), (0, half)],
        ),
        (
            "full_outer_residual_partial_transition",
            EquiJoinKind::FullOuter,
            &half_right,
            &retracted_half_right,
            false,
            4 * half,
            [(half, half), (half, half)],
        ),
    ] {
        let mut fixture = Fixture::new(root, kind, schema, Some(residual.clone()));
        let seed = fixture.apply(0, left_seed);
        assert!(seed.turns > 0);
        if stable_presence {
            let anchor = fixture.apply(1, toggled_right);
            assert!(anchor.turns > 0);
        }
        let first = fixture.apply_checked(1, toggled_right);
        let second = fixture.apply_checked(1, retracted_right);
        validate_pair(&first, &second, expected_rows);
        validate_directions(&first, &second, expected_directions);
        group.bench_function(name, |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let first = fixture.apply(1, toggled_right);
                    let second = fixture.apply(1, retracted_right);
                    elapsed += started.elapsed();
                    validate_pair(&first, &second, expected_rows);
                }
                elapsed
            });
        });
    }

    benchmark_residual_left_stable(group, root, schema, right_seed, residual);
}

fn benchmark_residual_left_stable(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    right_seed: &Change,
    residual: Expr,
) {
    let stable_left = change(schema, 7, vec![0], 1);
    let retracted_left = change(schema, 7, vec![0], -1);
    let mut fixture = Fixture::new(root, EquiJoinKind::LeftSemi, schema, Some(residual));
    assert!(fixture.apply(0, &stable_left).turns > 0);
    assert!(fixture.apply(1, right_seed).turns > 0);
    let first = fixture.apply_checked(0, &stable_left);
    let second = fixture.apply_checked(0, &retracted_left);
    validate_pair(&first, &second, 2);
    validate_directions(&first, &second, [(1, 0), (0, 1)]);
    group.bench_function("left_semi_residual_left_presence_stable", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let first = fixture.apply(0, &stable_left);
                let second = fixture.apply(0, &retracted_left);
                elapsed += started.elapsed();
                validate_pair(&first, &second, 2);
            }
            elapsed
        });
    });
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
    }
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let fanout = match profile {
        PerformanceProfile::Smoke => 64,
        PerformanceProfile::Reference => 1_024,
    };
    write_context(&root, profile, fanout);
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(200),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let left_seed = change(
        &schema,
        7,
        (0..fanout)
            .map(|value| i64::try_from(value).expect("benchmark fanout fits i64"))
            .collect(),
        1,
    );
    let (toggled_right, retracted_right) = right_toggle(&schema, 90_000);
    let stable_right = change(&schema, 7, vec![80_000], 1);
    let right_seed = change(
        &schema,
        7,
        (0..fanout)
            .map(|value| {
                i64::try_from(value)
                    .expect("benchmark fanout fits i64")
                    .saturating_add(100_000)
            })
            .collect(),
        1,
    );

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(2));
    benchmark_pure_equi(
        &mut group,
        &root,
        &schema,
        &left_seed,
        &toggled_right,
        &retracted_right,
        &stable_right,
    );
    benchmark_residual(&mut group, &root, &schema, &left_seed, &right_seed);
    group.finish();
    criterion.final_summary();
}
