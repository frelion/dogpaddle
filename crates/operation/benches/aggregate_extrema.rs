//! Aggregate-owned extrema workloads, including synchronous Store commits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput};
use datafusion_expr::col;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{
        Action, Operation, OperationInput, Turn,
        transform::{AggregateCall, AggregateDefinition},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{StoreSetup, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "aggregate_extrema";

/// Rows carried by one Change in the per-row workload below.
///
/// The single-row cases measure one turn each; this case keeps the turn count
/// fixed and varies the rows inside it, which is where the aggregate used to pay
/// two ordered partition reads per row.
const BULK_ROWS: usize = 4096;

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    _root: TempDir,
}

impl Fixture {
    fn new(root: &RunRoot, schema: &SchemaRef, pairs: usize, distinct_layouts: bool) -> Self {
        let sample = root.sample(BENCHMARK);
        let definition = AggregateDefinition::try_new(
            [("group", col("group"))],
            (0..pairs).flat_map(|index| {
                let expression = if distinct_layouts {
                    col(format!("value_{index}"))
                } else {
                    col("value")
                };
                [
                    (
                        format!("min_{index}"),
                        AggregateCall::min(expression.clone()),
                    ),
                    (format!("max_{index}"), AggregateCall::max(expression)),
                ]
            }),
        )
        .expect("define aggregate");
        let mut setup = StoreSetup::new();
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema)],
                &mut setup.data_scope(),
                "operation",
                RuntimeResource::none(),
            )
            .expect("construct aggregate")
            .into_parts();
        let transactions = setup
            .commit(sample.path().join("store"), |_| Ok(()))
            .expect("commit store setup");
        Self {
            operation,
            transactions,
            _root: sample,
        }
    }

    fn apply(&mut self, change: &Change) -> Action {
        let Turn::Ready(prepared) = self
            .operation
            .turn(Some(OperationInput { port: 0, change }))
            .expect("prepare aggregate")
        else {
            panic!("aggregate must be ready")
        };
        let transaction = self.transactions.begin();
        let (action, completion) = prepared
            .apply(transaction.access())
            .expect("apply aggregate");
        transaction.commit().expect("commit aggregate");
        completion.run().expect("complete aggregate");
        action
    }
}

fn change(schema: &SchemaRef, values: &[i64], diffs: Vec<i64>) -> Change {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> =
        vec![Arc::new(Int64Array::from(vec![1; values.len()]))];
    columns.extend((1..schema.fields().len()).map(|index| {
        Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value + i64::try_from(index - 1).expect("layout index fits i64"))
                .collect::<Vec<_>>(),
        )) as Arc<dyn arrow_array::Array>
    }));
    let records = RecordBatch::try_new(Arc::clone(schema), columns).expect("build input records");
    Change::try_new(records, Int64Array::from(diffs)).expect("build input Change")
}

fn validate(
    action: &Action,
    expected: Option<([i64; 2], [i64; 2])>,
    pairs: usize,
    distinct_layouts: bool,
) {
    let Action::Complete(output) = action else {
        panic!("aggregate must complete input")
    };
    let Some((minima, maxima)) = expected else {
        assert!(
            output.is_none(),
            "non-extreme weight changes must not emit rows"
        );
        return;
    };
    let output = output.as_ref().expect("extrema transition output");
    assert_eq!(output.diffs().values(), &[-1, 1]);
    let column = |index| {
        output
            .records()
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 output")
    };
    assert_eq!(column(0).values(), &[1, 1]);
    assert_eq!(output.records().num_columns(), 1 + 2 * pairs);
    for pair in 0..pairs {
        let offset = if distinct_layouts {
            i64::try_from(pair).expect("pair index fits i64")
        } else {
            0
        };
        assert_eq!(
            column(1 + 2 * pair).values(),
            &[minima[0] + offset, minima[1] + offset]
        );
        assert_eq!(
            column(2 + 2 * pair).values(),
            &[maxima[0] + offset, maxima[1] + offset]
        );
    }
}

/// Extrema of the last row one turn emitted.
///
/// The per-row cases assert their whole two-row transition; a turn carrying many
/// rows emits one transition per row, so its closing state is the stable check.
fn last_row_extrema(action: &Action, pairs: usize) -> (Vec<i64>, Vec<i64>) {
    let Action::Complete(output) = action else {
        panic!("aggregate must complete input")
    };
    let output = output.as_ref().expect("extrema transition output");
    assert_eq!(output.records().num_columns(), 1 + 2 * pairs);
    let column = |index| {
        output
            .records()
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 output")
    };
    let last = output.num_rows() - 1;
    (
        (0..pairs)
            .map(|pair| column(1 + 2 * pair).value(last))
            .collect(),
        (0..pairs)
            .map(|pair| column(2 + 2 * pair).value(last))
            .collect(),
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "the benchmark keeps its workload registry and timed boundaries together"
)]
fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
    }
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "seed_values": [0, 50, 100], "group_count": 1,
            "criterion_samples": 10, "warmup_ms": 100,
            "measurement_ms": match profile {
                PerformanceProfile::Smoke => 200,
                PerformanceProfile::Reference => 5_000,
            },
            "non_extreme_weight": 1_000_000, "repeated_extrema_pairs": 8,
            "distinct_extrema_layouts": 8,
            "bulk_rows_per_turn": BULK_ROWS,
            "timed_boundary": "two turns, apply, synchronous commit, AfterCommit",
            "untimed": "fixture, seed, warmup, output validation, teardown"
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode context"),
    )
    .expect("write context");
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(200),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let historical_schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let distinct_schema = Arc::new(Schema::new(
        std::iter::once(Field::new("group", DataType::Int64, false))
            .chain((0..8).map(|index| Field::new(format!("value_{index}"), DataType::Int64, false)))
            .collect::<Vec<_>>(),
    ));
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(2));
    for (name, pairs, distinct_layouts, value, difference, transition) in [
        (
            "same_group_high_multiplicity",
            1,
            false,
            50,
            1_000_000,
            false,
        ),
        ("extrema_retraction", 1, false, 0, -1, true),
        ("repeated_min_max", 8, false, 0, -1, true),
        ("distinct_layout_min_max", 8, true, 0, -1, true),
    ] {
        let schema = if distinct_layouts {
            &distinct_schema
        } else {
            &historical_schema
        };
        let mut fixture = Fixture::new(&root, schema, pairs, distinct_layouts);
        let seed = change(schema, &[0, 50, 100], vec![1, 1, 1]);
        let seed_action = fixture.apply(&seed);
        assert!(matches!(seed_action, Action::Complete(Some(_))));
        let first = change(schema, &[value], vec![difference]);
        let second = change(schema, &[value], vec![-difference]);
        let first_expected = transition.then_some(([0, 50], [100, 100]));
        let second_expected = transition.then_some(([50, 0], [100, 100]));
        // One explicit untimed round verifies the seed and returns to its state.
        validate(
            &fixture.apply(&first),
            first_expected,
            pairs,
            distinct_layouts,
        );
        validate(
            &fixture.apply(&second),
            second_expected,
            pairs,
            distinct_layouts,
        );
        group.bench_function(name, |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let first_action = fixture.apply(&first);
                    let second_action = fixture.apply(&second);
                    elapsed += started.elapsed();
                    validate(&first_action, first_expected, pairs, distinct_layouts);
                    validate(&second_action, second_expected, pairs, distinct_layouts);
                }
                elapsed
            });
        });
    }
    group.finish();
    let seed = change(&historical_schema, &[0, 50, 100], vec![1, 1, 1]);
    bench_bulk_rows(&mut criterion, &root, &historical_schema, &seed);
    criterion.final_summary();
}

/// One turn carrying many rows: the shape that pays the aggregate's per-row cost
/// rather than only the per-turn commit cost.
fn bench_bulk_rows(criterion: &mut Criterion, root: &RunRoot, schema: &SchemaRef, seed: &Change) {
    let rows = i64::try_from(BULK_ROWS).expect("bulk rows fit i64");
    let bulk_values: Vec<i64> = (0..rows).collect();
    let bulk = change(schema, &bulk_values, vec![1; BULK_ROWS]);
    let bulk_undo = change(schema, &bulk_values, vec![-1; BULK_ROWS]);
    let mut fixture = Fixture::new(root, schema, 1, false);
    assert!(matches!(fixture.apply(seed), Action::Complete(Some(_))));
    // Untimed rounds verify the closing state and return the group to the seed.
    assert_eq!(
        last_row_extrema(&fixture.apply(&bulk), 1),
        (vec![0], vec![rows - 1])
    );
    assert_eq!(
        last_row_extrema(&fixture.apply(&bulk_undo), 1),
        (vec![0], vec![100])
    );

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(BULK_ROWS * 2).expect("bulk elements fit u64"),
    ));
    group.bench_function("many_rows_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let up = fixture.apply(&bulk);
                let down = fixture.apply(&bulk_undo);
                elapsed += started.elapsed();
                assert!(matches!(up, Action::Complete(Some(_))));
                assert!(matches!(down, Action::Complete(Some(_))));
            }
            elapsed
        });
    });
    group.finish();
}
