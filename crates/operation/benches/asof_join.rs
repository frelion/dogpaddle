//! ASOF-owned ordered lookup and dynamic historical-rematch workloads.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkGroup, Criterion, Throughput, measurement::WallTime};
use datafusion_expr::col;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{
        BudgetExceeded, Operation, OperationInput, Progress, StepBudget,
        transform::{AsOfDirection, AsOfEqualityKey, AsOfJoinDefinition, AsOfOrderKey},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{StoreSetup, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "asof_join";

#[derive(Clone, Copy)]
struct DefinitionOptions {
    direction: AsOfDirection,
    partitioned: bool,
}

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    _root: TempDir,
}

#[derive(Default)]
struct InputResult {
    output_rows: usize,
    turns: usize,
    positive_rows: usize,
    negative_rows: usize,
}

impl Fixture {
    fn new(root: &RunRoot, schema: &SchemaRef, options: DefinitionOptions) -> Self {
        let sample = root.sample(BENCHMARK);
        let equalities = options
            .partitioned
            .then(|| AsOfEqualityKey::new(col("group"), col("group")));
        let definition = AsOfJoinDefinition::try_new(
            options.direction,
            equalities,
            AsOfOrderKey::new(col("at"), col("at")),
            ["left", "right"].into_iter().flat_map(|side| {
                schema
                    .fields()
                    .iter()
                    .map(move |field| format!("{side}_{}", field.name()))
            }),
        )
        .expect("define ASOF benchmark");
        let mut setup = StoreSetup::new();
        let (operation, _) = OperationDefinition::from(definition.clone())
            .construct(
                &[Arc::clone(schema), Arc::clone(schema)],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .expect("construct ASOF benchmark")
            .into_parts();
        let transactions = setup
            .commit(sample.path().join("store"), |_| Ok(()))
            .expect("commit setup");
        Self {
            operation,
            transactions,
            _root: sample,
        }
    }

    fn apply(&mut self, port: usize, change: &Change) -> InputResult {
        let mut result = InputResult::default();
        let mut resume = self.operation.initial_resume();
        for _ in 0..1_000_000 {
            let mut items = 256;
            let step = loop {
                let transaction = self.transactions.begin();
                match self.operation.step(
                    OperationInput { port, change },
                    &resume,
                    transaction.access(),
                    &mut StepBudget::new(items, 4 * 1024 * 1024),
                ) {
                    Ok(step) => {
                        transaction.commit().expect("commit ASOF benchmark page");
                        break step;
                    }
                    Err(error) if error.is::<BudgetExceeded>() && items > 1 => items /= 2,
                    Err(error) => panic!("apply ASOF benchmark page: {error}"),
                }
            };
            result.turns += 1;
            result.observe(step.output.as_ref());
            match step.progress {
                Progress::More(next) => resume = next,
                Progress::Done => return result,
            }
        }
        panic!("ASOF benchmark input did not complete")
    }
}

impl InputResult {
    fn observe(&mut self, output: Option<&Change>) {
        let Some(output) = output else {
            return;
        };
        self.output_rows += output.num_rows();
        for difference in output.diffs().values() {
            match difference.cmp(&0) {
                std::cmp::Ordering::Less => self.negative_rows += 1,
                std::cmp::Ordering::Greater => self.positive_rows += 1,
                std::cmp::Ordering::Equal => panic!("Change admitted a zero difference"),
            }
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::UInt64, false),
        Field::new("at", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn change(
    schema: &SchemaRef,
    groups: Vec<u64>,
    orders: Vec<i64>,
    values: Vec<i64>,
    difference: i64,
) -> Change {
    assert_eq!(groups.len(), orders.len());
    assert_eq!(groups.len(), values.len());
    let rows = groups.len();
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(groups)),
            Arc::new(Int64Array::from(orders)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("build ASOF benchmark records");
    Change::try_new(records, Int64Array::from(vec![difference; rows]))
        .expect("build ASOF benchmark Change")
}

fn signed_ordinals(rows: usize) -> Vec<i64> {
    (0..rows)
        .map(|value| i64::try_from(value).expect("ASOF workload ordinal fits i64"))
        .collect()
}

fn benchmark_pair(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    fixture: &mut Fixture,
    port: usize,
    inserted: &Change,
    retracted: &Change,
    expected: [(usize, usize); 2],
) {
    let first = fixture.apply(port, inserted);
    let second = fixture.apply(port, retracted);
    validate_pair(&first, &second, expected);
    group.bench_function(name, |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let first = fixture.apply(port, inserted);
                let second = fixture.apply(port, retracted);
                elapsed += started.elapsed();
                validate_pair(&first, &second, expected);
            }
            elapsed
        });
    });
}

fn validate_pair(first: &InputResult, second: &InputResult, expected: [(usize, usize); 2]) {
    assert_eq!(first.output_rows, expected[0].0 + expected[0].1);
    assert_eq!(second.output_rows, expected[1].0 + expected[1].1);
    assert_eq!((first.positive_rows, first.negative_rows), expected[0]);
    assert_eq!((second.positive_rows, second.negative_rows), expected[1]);
    assert!(first.turns > 0);
    assert!(second.turns > 0);
}

fn benchmark_partitioned_lookup(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    partitions: usize,
) {
    const VERSIONS: usize = 4;
    let mut fixture = Fixture::new(
        root,
        schema,
        DefinitionOptions {
            direction: AsOfDirection::Backward { allow_exact: true },
            partitioned: true,
        },
    );
    let mut groups = Vec::with_capacity(partitions * VERSIONS);
    let mut orders = Vec::with_capacity(partitions * VERSIONS);
    let mut values = Vec::with_capacity(partitions * VERSIONS);
    for partition in 0..partitions {
        for version in 0..VERSIONS {
            groups.push(u64::try_from(partition).expect("partition fits u64"));
            orders.push(i64::try_from(version).expect("version fits i64"));
            values.push(i64::try_from(version).expect("version fits i64"));
        }
    }
    assert!(
        fixture
            .apply(1, &change(schema, groups, orders, values, 1))
            .turns
            > 0
    );
    let left_groups = (0..partitions)
        .map(|value| u64::try_from(value).expect("partition fits u64"))
        .collect::<Vec<_>>();
    let left_orders = vec![i64::try_from(VERSIONS).expect("version count fits i64"); partitions];
    let left_values = signed_ordinals(partitions);
    let inserted = change(
        schema,
        left_groups.clone(),
        left_orders.clone(),
        left_values.clone(),
        1,
    );
    let retracted = change(schema, left_groups, left_orders, left_values, -1);
    benchmark_pair(
        group,
        "partitioned_lookup",
        &mut fixture,
        0,
        &inserted,
        &retracted,
        [(partitions, 0), (0, partitions)],
    );
}

fn benchmark_global_lookup(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    versions: usize,
) {
    let mut fixture = Fixture::new(
        root,
        schema,
        DefinitionOptions {
            direction: AsOfDirection::Backward { allow_exact: true },
            partitioned: false,
        },
    );
    let orders = signed_ordinals(versions);
    assert!(
        fixture
            .apply(
                1,
                &change(schema, vec![0; versions], orders.clone(), orders, 1),
            )
            .turns
            > 0
    );
    let order = i64::try_from(versions).expect("version count fits i64");
    let inserted = change(schema, vec![0], vec![order], vec![0], 1);
    let retracted = change(schema, vec![0], vec![order], vec![0], -1);
    benchmark_pair(
        group,
        "global_partition_lookup",
        &mut fixture,
        0,
        &inserted,
        &retracted,
        [(1, 0), (0, 1)],
    );
}

fn rematch_fixture(
    root: &RunRoot,
    schema: &SchemaRef,
    left_rows: usize,
    right_versions: usize,
    tail_anchor: bool,
) -> Fixture {
    let mut fixture = Fixture::new(
        root,
        schema,
        DefinitionOptions {
            direction: AsOfDirection::Backward { allow_exact: true },
            partitioned: true,
        },
    );
    let future_base = i64::try_from(left_rows)
        .expect("left count fits i64")
        .saturating_add(10_000);
    let mut right_orders = vec![0];
    if tail_anchor {
        right_orders.push(
            i64::try_from(left_rows)
                .expect("left count fits i64")
                .saturating_sub(2),
        );
    }
    while right_orders.len() < right_versions {
        right_orders
            .push(future_base.saturating_add(
                i64::try_from(right_orders.len()).expect("right ordinal fits i64"),
            ));
    }
    assert!(
        fixture
            .apply(
                1,
                &change(
                    schema,
                    vec![7; right_orders.len()],
                    right_orders.clone(),
                    right_orders,
                    1,
                ),
            )
            .turns
            > 0
    );
    // Seed candidates before probes: the benchmarked state is identical, while fixture setup
    // does not itself perform every intermediate right-presence rematch.
    let left_orders = (1..=left_rows)
        .map(|value| i64::try_from(value).expect("left ordinal fits i64"))
        .collect::<Vec<_>>();
    assert!(
        fixture
            .apply(
                0,
                &change(
                    schema,
                    vec![7; left_rows],
                    left_orders.clone(),
                    left_orders,
                    1,
                ),
            )
            .turns
            > 0
    );
    fixture
}

fn benchmark_right_rematch(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    schema: &SchemaRef,
    left_rows: usize,
    right_versions: usize,
) {
    let tail_order = i64::try_from(left_rows)
        .expect("left count fits i64")
        .saturating_sub(1);
    let tail_inserted = change(schema, vec![7], vec![tail_order], vec![tail_order], 1);
    let tail_retracted = change(schema, vec![7], vec![tail_order], vec![tail_order], -1);
    let mut tail = rematch_fixture(root, schema, left_rows, right_versions, true);
    benchmark_pair(
        group,
        "right_tail_small_rematch",
        &mut tail,
        1,
        &tail_inserted,
        &tail_retracted,
        [(2, 2), (2, 2)],
    );

    let historical_inserted = change(schema, vec![7], vec![1], vec![1], 1);
    let historical_retracted = change(schema, vec![7], vec![1], vec![1], -1);
    let mut historical = rematch_fixture(root, schema, left_rows, right_versions, false);
    benchmark_pair(
        group,
        "right_historical_full_rematch",
        &mut historical,
        1,
        &historical_inserted,
        &historical_retracted,
        [(left_rows, left_rows), (left_rows, left_rows)],
    );
}

fn write_context(
    root: &RunRoot,
    profile: PerformanceProfile,
    partitions: usize,
    versions: usize,
    rematch_left_rows: usize,
    rematch_right_versions: usize,
) {
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "criterion_samples": 10,
            "warmup_ms": 100,
            "measurement_ms": match profile {
                PerformanceProfile::Smoke => 200,
                PerformanceProfile::Reference => 5_000,
            },
            "timed_boundary": "one insert input plus its exact retract input, including every Operation::step attempt, deterministic item-budget halving from 256 to one on BudgetExceeded, and synchronous successful-page commit; the pair restores the initial relation",
            "throughput_unit": "complete inputs (two per timed iteration)",
            "untimed": "fixture, relation seed, warmup, output validation, teardown",
            "runtime_counters": "unavailable: Operation does not expose scan-page or logical read/write-byte counters; workload cardinalities and committed page/output counts are retained instead",
            "cases": {
                "partitioned_lookup": {
                    "partitions": partitions,
                    "right_versions_per_partition": 4,
                    "left_rows_per_input": partitions,
                },
                "global_partition_lookup": {
                    "partitions": 1,
                    "right_versions": versions,
                    "left_rows_per_input": 1,
                },
                "right_tail_small_rematch": {
                    "left_rows": rematch_left_rows,
                    "right_versions": rematch_right_versions,
                    "corrected_left_rows_per_input": 2,
                },
                "right_historical_full_rematch": {
                    "left_rows": rematch_left_rows,
                    "right_versions": rematch_right_versions,
                    "corrected_left_rows_per_input": rematch_left_rows,
                    "candidate_shape": "one eligible old version plus future ineligible history forces the full left-by-right correctness path",
                },
                "right_wide_winner_rematch": {
                    "left_rows": wide_workload(profile).0,
                    "winner_utf8_bytes": wide_workload(profile).1,
                    "right_versions": 1,
                    "left_utf8_bytes": 1,
                    "outputs_per_left_per_input": 2,
                    "page_budget": "256 items and 4 MiB logical bytes; page count may differ with implementation scratch cost",
                },
            },
        },
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode ASOF context"),
    )
    .expect("write ASOF context");
}

fn wide_workload(profile: PerformanceProfile) -> (usize, usize) {
    match profile {
        PerformanceProfile::Smoke => (16, 4 * 1024),
        PerformanceProfile::Reference => (128, 64 * 1024),
    }
}

fn wide_change(schema: &SchemaRef, orders: Vec<i64>, payload: &str, difference: i64) -> Change {
    let rows = orders.len();
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(vec![7; rows])),
            Arc::new(Int64Array::from(orders)),
            Arc::new(Int64Array::from(vec![0; rows])),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                payload, rows,
            ))),
        ],
    )
    .expect("build wide ASOF benchmark records");
    Change::try_new(records, Int64Array::from(vec![difference; rows]))
        .expect("build wide ASOF benchmark Change")
}

fn benchmark_wide_rematch(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    profile: PerformanceProfile,
) {
    let (left_rows, payload_bytes) = wide_workload(profile);
    let mut fields = schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, false)));
    let schema = Arc::new(Schema::new(fields));
    let mut fixture = Fixture::new(
        root,
        &schema,
        DefinitionOptions {
            direction: AsOfDirection::Backward { allow_exact: true },
            partitioned: true,
        },
    );
    fixture.apply(
        1,
        &wide_change(&schema, vec![0], &"o".repeat(payload_bytes), 1),
    );
    let orders = (1..=left_rows)
        .map(|value| i64::try_from(value).expect("wide ASOF ordinal fits i64"))
        .collect();
    fixture.apply(0, &wide_change(&schema, orders, "l", 1));
    let payload = "n".repeat(payload_bytes);
    let inserted = wide_change(&schema, vec![1], &payload, 1);
    let retracted = wide_change(&schema, vec![1], &payload, -1);
    benchmark_pair(
        group,
        "right_wide_winner_rematch",
        &mut fixture,
        1,
        &inserted,
        &retracted,
        [(left_rows, left_rows), (left_rows, left_rows)],
    );
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
    }
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let (partitions, versions, rematch_left_rows, rematch_right_versions) = match profile {
        PerformanceProfile::Smoke => (16, 64, 16, 16),
        PerformanceProfile::Reference => (256, 1_024, 128, 128),
    };
    write_context(
        &root,
        profile,
        partitions,
        versions,
        rematch_left_rows,
        rematch_right_versions,
    );
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(200),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let schema = schema();
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(2));
    benchmark_partitioned_lookup(&mut group, &root, &schema, partitions);
    benchmark_global_lookup(&mut group, &root, &schema, versions);
    benchmark_right_rematch(
        &mut group,
        &root,
        &schema,
        rematch_left_rows,
        rematch_right_versions,
    );
    benchmark_wide_rematch(&mut group, &root, profile);
    group.finish();
    criterion.final_summary();
}
