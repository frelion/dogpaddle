//! Aggregate-owned extrema workloads, including synchronous Store commits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput};
use datafusion_expr::col;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{
        Operation, OperationInput, StepBudget,
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
const ARGUMENT_COUNT: usize = 64;
const ARGUMENT_ROWS: usize = 512;

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    _root: TempDir,
}

impl Fixture {
    fn new(root: &RunRoot, schema: &SchemaRef, pairs: usize, distinct_layouts: bool) -> Self {
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
                        AggregateCall::Min(expression.clone()),
                    ),
                    (format!("max_{index}"), AggregateCall::Max(expression)),
                ]
            }),
        )
        .expect("define aggregate");
        Self::with_definition(root, schema, definition)
    }

    fn with_definition(
        root: &RunRoot,
        schema: &SchemaRef,
        definition: AggregateDefinition,
    ) -> Self {
        let sample = root.sample(BENCHMARK);
        let mut setup = StoreSetup::new();
        let (operation, _) = OperationDefinition::from(definition)
            .construct(
                &[Arc::clone(schema)],
                &mut setup.data_scope().scoped("operation"),
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

    fn apply(&mut self, change: &Change) -> Option<Change> {
        let Operation::Atomic(operation) = &self.operation else {
            panic!("aggregate must be atomic")
        };
        let transaction = self.transactions.begin();
        // Owner benchmark measures the complete supplied batch; production
        // Flow chooses the bounded head slice before invoking this same kernel.
        let output = operation
            .apply(
                OperationInput { port: 0, change },
                transaction.access(),
                &mut StepBudget::new(0, 64 * 1024 * 1024),
            )
            .expect("apply aggregate");
        transaction.commit().expect("commit aggregate");
        output
    }
}

fn change(schema: &SchemaRef, values: &[i64], diffs: Vec<i64>) -> Change {
    let groups = vec![1; values.len()];
    change_with_groups(schema, &groups, values, diffs)
}

fn change_with_groups(
    schema: &SchemaRef,
    groups: &[i64],
    values: &[i64],
    diffs: Vec<i64>,
) -> Change {
    assert_eq!(groups.len(), values.len());
    let mut columns: Vec<Arc<dyn arrow_array::Array>> =
        vec![Arc::new(Int64Array::from(groups.to_vec()))];
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
    output: Option<&Change>,
    expected: Option<([i64; 2], [i64; 2])>,
    pairs: usize,
    distinct_layouts: bool,
) {
    let Some((minima, maxima)) = expected else {
        assert!(
            output.is_none(),
            "non-extreme weight changes must not emit rows"
        );
        return;
    };
    let output = output.expect("extrema transition output");
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
fn last_row_extrema(output: Option<&Change>, pairs: usize) -> (Vec<i64>, Vec<i64>) {
    let output = output.expect("extrema transition output");
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
            "single_group_cases": {
                "group_count": 1,
                "seed_values": [0, 50, 100]
            },
            "many_new_groups_case": {
                "group_count": BULK_ROWS,
                "rows_per_turn": BULK_ROWS
            },
            "many_existing_groups_case": {
                "group_count": BULK_ROWS,
                "rows_per_turn": BULK_ROWS,
                "extrema_key_type": "Int64"
            },
            "criterion_samples": 10, "warmup_ms": 100,
            "measurement_ms": match profile {
                PerformanceProfile::Smoke => 200,
                PerformanceProfile::Reference => 5_000,
            },
            "non_extreme_weight": 1_000_000, "repeated_extrema_pairs": 8,
            "distinct_extrema_layouts": 8,
            "bulk_rows_per_turn": BULK_ROWS,
            "hot_extrema_value": 150,
            "zero_net_group_cycle_rows": BULK_ROWS,
            "bulk_new_groups_per_turn": BULK_ROWS,
            "many_argument_cases": {
                "unique_arguments": ARGUMENT_COUNT,
                "rows_per_turn": ARGUMENT_ROWS,
                "group_count": 1,
                "seed_weight": 1,
                "argument_values": "50 + argument index, repeated within the batch",
                "count_only_statistics": ARGUMENT_COUNT,
                "sparse_statistics": 2,
                "sparse_extrema_partitions": ARGUMENT_COUNT,
                "sparse_extrema_slots": ARGUMENT_COUNT * 2,
                "oracle": "untimed two-event insert/retract trace and every output column/diff of the full timed batches",
                "scope": "complete apply and commit; constructor binding is outside timing"
            },
            "timed_boundary": "one or two full-batch Atomic apply calls per case under an explicit 64MiB logical byte allowance; each includes Transaction::commit; writes synchronize WAL",
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
        assert!(seed_action.is_some());
        let first = change(schema, &[value], vec![difference]);
        let second = change(schema, &[value], vec![-difference]);
        let first_expected = transition.then_some(([0, 50], [100, 100]));
        let second_expected = transition.then_some(([50, 0], [100, 100]));
        // One explicit untimed round verifies the seed and returns to its state.
        validate(
            fixture.apply(&first).as_ref(),
            first_expected,
            pairs,
            distinct_layouts,
        );
        validate(
            fixture.apply(&second).as_ref(),
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
                    validate(
                        first_action.as_ref(),
                        first_expected,
                        pairs,
                        distinct_layouts,
                    );
                    validate(
                        second_action.as_ref(),
                        second_expected,
                        pairs,
                        distinct_layouts,
                    );
                }
                elapsed
            });
        });
    }
    group.finish();
    let seed = change(&historical_schema, &[0, 50, 100], vec![1, 1, 1]);
    bench_bulk_rows(&mut criterion, &root, &historical_schema, &seed);
    bench_hot_extrema_key(&mut criterion, &root, &historical_schema, &seed);
    bench_zero_net_group_cycles(&mut criterion, &root, &historical_schema, &seed);
    bench_existing_groups(&mut criterion, &root, &historical_schema);
    bench_new_groups(&mut criterion, &root, &historical_schema);
    bench_count_arguments(&mut criterion, &root);
    bench_sparse_statistics(&mut criterion, &root);
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
    assert!(fixture.apply(seed).is_some());
    // Untimed rounds verify the closing state and return the group to the seed.
    assert_eq!(
        last_row_extrema(fixture.apply(&bulk).as_ref(), 1),
        (vec![0], vec![rows - 1])
    );
    assert_eq!(
        last_row_extrema(fixture.apply(&bulk_undo).as_ref(), 1),
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
                assert!(up.is_some());
                assert!(down.is_some());
            }
            elapsed
        });
    });
    group.finish();
}

/// One repeated ordered argument in a Change measures the bounded extrema
/// cache while its first and last events still change the visible maximum.
fn bench_hot_extrema_key(
    criterion: &mut Criterion,
    root: &RunRoot,
    schema: &SchemaRef,
    seed: &Change,
) {
    let values = vec![150; BULK_ROWS];
    let insert = change(schema, &values, vec![1; BULK_ROWS]);
    let retract = change(schema, &values, vec![-1; BULK_ROWS]);
    let mut fixture = Fixture::new(root, schema, 1, false);
    assert!(fixture.apply(seed).is_some());
    assert_eq!(
        last_row_extrema(fixture.apply(&insert).as_ref(), 1),
        (vec![0], vec![150])
    );
    assert_eq!(
        last_row_extrema(fixture.apply(&retract).as_ref(), 1),
        (vec![0], vec![100])
    );

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(BULK_ROWS * 2).expect("bulk elements fit u64"),
    ));
    group.bench_function("repeated_extrema_key_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let up = fixture.apply(&insert);
                let down = fixture.apply(&retract);
                elapsed += started.elapsed();
                assert_eq!(last_row_extrema(up.as_ref(), 1), (vec![0], vec![150]));
                assert_eq!(last_row_extrema(down.as_ref(), 1), (vec![0], vec![100]));
            }
            elapsed
        });
    });
    group.finish();
}

/// Repeated positive/negative pairs restore both the existing group and its
/// extrema partition within one turn, leaving no persistent writes to commit.
fn bench_zero_net_group_cycles(
    criterion: &mut Criterion,
    root: &RunRoot,
    schema: &SchemaRef,
    seed: &Change,
) {
    let values = vec![100; BULK_ROWS];
    let diffs = (0..BULK_ROWS)
        .map(|row| if row % 2 == 0 { 1 } else { -1 })
        .collect();
    let cycle = change(schema, &values, diffs);
    let retract = change(schema, &[100], vec![-1]);
    let restore = change(schema, &[100], vec![1]);
    let mut fixture = Fixture::new(root, schema, 1, false);
    assert!(fixture.apply(seed).is_some());
    assert!(fixture.apply(&cycle).is_none());
    // The visible max transition proves the seed's multiplicity is exactly one.
    validate(
        fixture.apply(&retract).as_ref(),
        Some(([0, 0], [100, 50])),
        1,
        false,
    );
    validate(
        fixture.apply(&restore).as_ref(),
        Some(([0, 0], [50, 100])),
        1,
        false,
    );

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(BULK_ROWS).expect("bulk elements fit u64"),
    ));
    group.bench_function("zero_net_group_extrema_cycles_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let action = fixture.apply(&cycle);
                elapsed += started.elapsed();
                assert!(action.is_none());
            }
            elapsed
        });
    });
    group.finish();
}

/// One row per existing group exercises the common path without a group clone.
fn bench_existing_groups(criterion: &mut Criterion, root: &RunRoot, schema: &SchemaRef) {
    let groups: Vec<i64> = (0..i64::try_from(BULK_ROWS).expect("bulk rows fit i64")).collect();
    let values = vec![50; BULK_ROWS];
    let insert = change_with_groups(schema, &groups, &values, vec![1; BULK_ROWS]);
    let retract = change_with_groups(schema, &groups, &values, vec![-1; BULK_ROWS]);
    let mut fixture = Fixture::new(root, schema, 1, false);

    validate_group_lifecycle(fixture.apply(&insert).as_ref(), &groups, 1);
    assert!(fixture.apply(&insert).is_none());
    assert!(fixture.apply(&retract).is_none());
    // Untimed death and rebirth prove every group returned to weight one.
    validate_group_lifecycle(fixture.apply(&retract).as_ref(), &groups, -1);
    validate_group_lifecycle(fixture.apply(&insert).as_ref(), &groups, 1);

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(BULK_ROWS * 2).expect("bulk elements fit u64"),
    ));
    group.bench_function("many_existing_groups_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let increased = fixture.apply(&insert);
                let restored = fixture.apply(&retract);
                elapsed += started.elapsed();
                assert!(increased.is_none());
                assert!(restored.is_none());
            }
            elapsed
        });
    });
    group.finish();
}

/// Many groups born in one turn exercise the aggregate's durable ID allocator.
fn bench_new_groups(criterion: &mut Criterion, root: &RunRoot, schema: &SchemaRef) {
    let groups: Vec<i64> = (0..i64::try_from(BULK_ROWS).expect("bulk rows fit i64")).collect();
    let values = vec![50; BULK_ROWS];
    let insert = change_with_groups(schema, &groups, &values, vec![1; BULK_ROWS]);
    let retract = change_with_groups(schema, &groups, &values, vec![-1; BULK_ROWS]);
    let mut fixture = Fixture::new(root, schema, 1, false);

    validate_group_lifecycle(fixture.apply(&insert).as_ref(), &groups, 1);
    validate_group_lifecycle(fixture.apply(&retract).as_ref(), &groups, -1);

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(BULK_ROWS * 2).expect("bulk elements fit u64"),
    ));
    group.bench_function("many_new_groups_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let inserted = fixture.apply(&insert);
                let retracted = fixture.apply(&retract);
                elapsed += started.elapsed();
                validate_group_lifecycle(inserted.as_ref(), &groups, 1);
                validate_group_lifecycle(retracted.as_ref(), &groups, -1);
            }
            elapsed
        });
    });
    group.finish();
}

fn validate_group_lifecycle(output: Option<&Change>, groups: &[i64], difference: i64) {
    let Some(output) = output else {
        panic!("group lifecycle must produce output")
    };
    assert_eq!(output.num_rows(), groups.len());
    assert!(
        output
            .diffs()
            .values()
            .iter()
            .all(|value| *value == difference)
    );
    let output_groups = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 group output");
    assert_eq!(output_groups.values(), groups);
    assert_eq!(output.records().num_columns(), 3);
    for column in &output.records().columns()[1..] {
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 extrema output");
        assert!(values.values().iter().all(|value| *value == 50));
    }
}

fn argument_schema() -> SchemaRef {
    Arc::new(Schema::new(
        std::iter::once(Field::new("group", DataType::Int64, false))
            .chain(
                (0..ARGUMENT_COUNT)
                    .map(|index| Field::new(format!("value_{index}"), DataType::Int64, true)),
            )
            .collect::<Vec<_>>(),
    ))
}

fn assert_argument_output(output: Option<&Change>, columns: &[Vec<i64>], diffs: &[i64]) {
    let output = output.expect("argument transition output");
    assert_eq!(output.num_rows(), diffs.len());
    assert_eq!(output.diffs().values(), diffs);
    assert_eq!(output.records().num_columns(), columns.len());
    for (column, expected) in output.records().columns().iter().zip(columns) {
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 output");
        assert_eq!(values.null_count(), 0);
        assert_eq!(values.values(), expected.as_slice());
    }
}

fn count_columns(counts: &[i64]) -> Vec<Vec<i64>> {
    std::iter::once(vec![1; counts.len()])
        .chain((0..ARGUMENT_COUNT).map(|_| counts.to_vec()))
        .collect()
}

fn sparse_columns(counts: &[i64], extrema: &[(i64, i64)]) -> Vec<Vec<i64>> {
    assert_eq!(counts.len(), extrema.len());
    let mut columns = vec![vec![1; counts.len()], counts.to_vec(), counts.to_vec()];
    for argument in 0..ARGUMENT_COUNT {
        let offset = i64::try_from(argument).expect("argument index fits i64");
        columns.push(extrema.iter().map(|(min, _)| min + offset).collect());
        columns.push(extrema.iter().map(|(_, max)| max + offset).collect());
    }
    columns
}

/// The two-event oracle checks each COUNT argument's NULL handling before the
/// common non-NULL timed workload.
fn check_count_arguments(fixture: &mut Fixture, schema: &SchemaRef) {
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 1]))];
    let mut inserted = vec![vec![1; 3]];
    let mut retracted = vec![vec![1; 3]];
    for argument in 0..ARGUMENT_COUNT {
        let first = (argument % 3 != 1).then_some(10);
        let second = (argument % 3 != 2).then_some(20);
        columns.push(Arc::new(Int64Array::from(vec![first, second])));
        let before = i64::from(first.is_some());
        let after = i64::from(second.is_some());
        inserted.push(vec![before, before, before + after]);
        retracted.push(vec![before + after, after, after]);
    }
    let records = RecordBatch::try_new(Arc::clone(schema), columns).expect("two-event records");
    let insert = Change::try_new(records.clone(), Int64Array::from(vec![1, 1])).unwrap();
    let retract = Change::try_new(records, Int64Array::from(vec![-1, -1])).unwrap();
    assert_argument_output(fixture.apply(&insert).as_ref(), &inserted, &[1, -1, 1]);
    assert_argument_output(fixture.apply(&retract).as_ref(), &retracted, &[-1, 1, -1]);
}

/// All 64 COUNT arguments participate in every event. Construction remains
/// untimed; this case measures traversal of the wider bound argument records.
fn bench_count_arguments(criterion: &mut Criterion, root: &RunRoot) {
    let schema = argument_schema();
    let definition = AggregateDefinition::try_new(
        [("group", col("group"))],
        (0..ARGUMENT_COUNT).map(|index| {
            (
                format!("count_{index}"),
                AggregateCall::Count(col(format!("value_{index}"))),
            )
        }),
    )
    .expect("define COUNT arguments");
    let mut fixture = Fixture::with_definition(root, &schema, definition);
    check_count_arguments(&mut fixture, &schema);
    let seed = change(&schema, &[50], vec![1]);
    assert_argument_output(fixture.apply(&seed).as_ref(), &count_columns(&[1]), &[1]);
    let insert = change(&schema, &vec![50; ARGUMENT_ROWS], vec![1; ARGUMENT_ROWS]);
    let retract = change(&schema, &vec![50; ARGUMENT_ROWS], vec![-1; ARGUMENT_ROWS]);
    let rows = i64::try_from(ARGUMENT_ROWS).expect("argument rows fit i64");
    let up = count_columns(
        &(1..=rows)
            .flat_map(|count| [count, count + 1])
            .collect::<Vec<_>>(),
    );
    let down = count_columns(
        &(2..=rows + 1)
            .rev()
            .flat_map(|count| [count, count - 1])
            .collect::<Vec<_>>(),
    );
    let diffs: Vec<_> = (0..ARGUMENT_ROWS).flat_map(|_| [-1, 1]).collect();
    assert_argument_output(fixture.apply(&insert).as_ref(), &up, &diffs);
    assert_argument_output(fixture.apply(&retract).as_ref(), &down, &diffs);

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(ARGUMENT_ROWS * 2).unwrap(),
    ));
    group.bench_function("many_count_arguments_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let increased = fixture.apply(&insert);
                let restored = fixture.apply(&retract);
                elapsed += started.elapsed();
                assert_argument_output(increased.as_ref(), &up, &diffs);
                assert_argument_output(restored.as_ref(), &down, &diffs);
            }
            elapsed
        });
    });
    group.finish();
}

/// Two statistical roles are sparse among 64 extrema arguments. Repeated keys
/// use the existing pending-key path while both role loops inspect arguments.
fn bench_sparse_statistics(criterion: &mut Criterion, root: &RunRoot) {
    let schema = argument_schema();
    let calls = (0..2)
        .map(|index| {
            (
                format!("count_{index}"),
                AggregateCall::Count(col(format!("value_{index}"))),
            )
        })
        .chain((0..ARGUMENT_COUNT).flat_map(|index| {
            let expression = col(format!("value_{index}"));
            [
                (
                    format!("min_{index}"),
                    AggregateCall::Min(expression.clone()),
                ),
                (format!("max_{index}"), AggregateCall::Max(expression)),
            ]
        }));
    let definition = AggregateDefinition::try_new([("group", col("group"))], calls)
        .expect("define sparse statistics and extrema");
    let mut fixture = Fixture::with_definition(root, &schema, definition);
    let small = change(&schema, &[10, 20], vec![1, 1]);
    let small_undo = change(&schema, &[10, 20], vec![-1, -1]);
    assert_argument_output(
        fixture.apply(&small).as_ref(),
        &sparse_columns(&[1, 1, 2], &[(10, 10), (10, 10), (10, 20)]),
        &[1, -1, 1],
    );
    assert_argument_output(
        fixture.apply(&small_undo).as_ref(),
        &sparse_columns(&[2, 1, 1], &[(10, 20), (20, 20), (20, 20)]),
        &[-1, 1, -1],
    );
    let seed = change(&schema, &[50], vec![1]);
    assert_argument_output(
        fixture.apply(&seed).as_ref(),
        &sparse_columns(&[1], &[(50, 50)]),
        &[1],
    );
    let insert = change(&schema, &vec![50; ARGUMENT_ROWS], vec![1; ARGUMENT_ROWS]);
    let retract = change(&schema, &vec![50; ARGUMENT_ROWS], vec![-1; ARGUMENT_ROWS]);
    let rows = i64::try_from(ARGUMENT_ROWS).expect("argument rows fit i64");
    let extrema = vec![(50, 50); ARGUMENT_ROWS * 2];
    let up = sparse_columns(
        &(1..=rows)
            .flat_map(|count| [count, count + 1])
            .collect::<Vec<_>>(),
        &extrema,
    );
    let down = sparse_columns(
        &(2..=rows + 1)
            .rev()
            .flat_map(|count| [count, count - 1])
            .collect::<Vec<_>>(),
        &extrema,
    );
    let diffs: Vec<_> = (0..ARGUMENT_ROWS).flat_map(|_| [-1, 1]).collect();
    assert_argument_output(fixture.apply(&insert).as_ref(), &up, &diffs);
    assert_argument_output(fixture.apply(&retract).as_ref(), &down, &diffs);

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(ARGUMENT_ROWS * 2).unwrap(),
    ));
    group.bench_function("sparse_statistics_many_extrema_one_turn", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let increased = fixture.apply(&insert);
                let restored = fixture.apply(&retract);
                elapsed += started.elapsed();
                assert_argument_output(increased.as_ref(), &up, &diffs);
                assert_argument_output(restored.as_ref(), &down, &diffs);
            }
            elapsed
        });
    });
    group.finish();
}
