//! EquiJoin-owned match and key-presence transition workloads with synchronous commits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput};
use datafusion_expr::col;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, OperationDefinition, RuntimeResource,
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
    fn new(root: &RunRoot, kind: EquiJoinKind, schema: &SchemaRef) -> Self {
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
        )
        .expect("define equi-join");
        let binding = (&definition as &dyn OperationDefinition)
            .bind(&[Arc::clone(schema), Arc::clone(schema)])
            .expect("bind equi-join");
        let mut store = Store::create(sample.path().join("store")).expect("create store");
        let mut data = DataInstances::new();
        for declaration in definition.data() {
            declaration
                .create(&mut store, declaration.name())
                .expect("create equi-join resource");
            data.insert(
                declaration
                    .open(&store, declaration.name())
                    .expect("open equi-join resource"),
            )
            .expect("insert equi-join resource");
        }
        Self {
            operation: binding
                .materialize(data, RuntimeResource::none())
                .expect("materialize equi-join"),
            transactions: store.into_transactions(),
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
            "timed_boundary": "two complete Claims, all Probe/Emit turns, synchronous commits, AfterCommit",
            "untimed": "fixture, relation seed, warmup, output validation, teardown",
            "cases": {
                "inner_first_last_match": "Inner control without key-count traffic",
                "left_semi_presence_stable": "exact right row changes while another right row preserves key presence",
                "left_semi_first_last_match": "first/last right match publishes every preserved left row",
                "full_outer_first_last_match": "first/last right match flips null-padded output for every left row"
            }
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode context"),
    )
    .expect("write context");
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
    let toggled_right = change(&schema, 7, vec![90_000], 1);
    let retracted_right = change(&schema, 7, vec![90_000], -1);
    let stable_right = change(&schema, 7, vec![80_000], 1);

    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(2));
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
        let mut fixture = Fixture::new(&root, kind, &schema);
        let seed = fixture.apply(0, &left_seed);
        assert!(seed.turns > 0);
        if stable_presence {
            let anchor = fixture.apply(1, &stable_right);
            assert!(anchor.turns > 0);
        }
        let first = fixture.apply_checked(1, &toggled_right);
        let second = fixture.apply_checked(1, &retracted_right);
        validate_pair(&first, &second, expected_rows);
        validate_directions(&first, &second, expected_directions);
        group.bench_function(name, |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let first = fixture.apply(1, &toggled_right);
                    let second = fixture.apply(1, &retracted_right);
                    elapsed += started.elapsed();
                    validate_pair(&first, &second, expected_rows);
                }
                elapsed
            });
        });
    }
    group.finish();
    criterion.final_summary();
}
