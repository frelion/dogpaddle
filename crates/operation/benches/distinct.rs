//! Distinct row-locality workloads, including synchronous Store commits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{Action, Operation, OperationInput, Turn, transform::DistinctDefinition},
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{StoreSetup, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "distinct";

struct Fixture {
    operation: Operation,
    transactions: Transactions,
    _root: TempDir,
}

impl Fixture {
    fn new(root: &RunRoot, name: &str, schema: &SchemaRef) -> Self {
        let sample = root.sample(name);
        let mut setup = StoreSetup::new();
        let definition = DistinctDefinition::new();
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(schema)],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .expect("construct Distinct")
            .into_parts();
        let transactions = setup
            .commit(sample.path().join("store"), |_| Ok(()))
            .expect("commit Store setup");
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
            .expect("prepare Distinct")
        else {
            panic!("Distinct must be ready")
        };
        let transaction = self.transactions.begin();
        let (action, completion) = prepared
            .apply(transaction.access())
            .expect("apply Distinct");
        transaction.commit().expect("commit Distinct");
        completion.run().expect("complete Distinct");
        action
    }
}

fn input(schema: &SchemaRef, rows: usize, contiguous: bool) -> Change {
    assert_eq!(rows % 4, 0);
    let values = (0..rows)
        .map(|index| {
            if contiguous {
                7
            } else {
                7 + i64::try_from(index % 2).expect("small key offset")
            }
        })
        .collect::<Vec<_>>();
    let differences = (0..rows)
        .map(|index| {
            if (contiguous && index % 2 == 0) || (!contiguous && index % 4 < 2) {
                1
            } else {
                -1
            }
        })
        .collect::<Vec<_>>();
    let records =
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])
            .expect("build Distinct records");
    Change::try_new(records, Int64Array::from(differences)).expect("build Distinct Change")
}

fn validate(action: &Action, input: &Change) {
    let Action::Complete(Some(output)) = action else {
        panic!("every cycle must emit its presence transitions")
    };
    assert_eq!(output.records(), input.records());
    assert_eq!(output.diffs(), input.diffs());
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
    }
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let rows = match profile {
        PerformanceProfile::Smoke => 1_024,
        PerformanceProfile::Reference => 4_096,
    };
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "rows_per_turn": rows,
            "workloads": ["contiguous_key_cycles", "interleaved_two_key_cycles"],
            "each_key_final_weight": 0,
            "each_event_emits_a_presence_transition": true,
            "criterion_samples": 10,
            "warmup_ms": 100,
            "measurement_ms": match profile {
                PerformanceProfile::Smoke => 200,
                PerformanceProfile::Reference => 5_000,
            },
            "timed_boundary": "one complete turn, apply, Transaction::commit, AfterCommit; writes synchronize WAL",
            "untimed": "fixture, input construction, output oracle, teardown"
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode context"),
    )
    .expect("write context");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(200),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(rows).expect("row count fits u64"),
    ));
    for (name, contiguous) in [
        ("contiguous_key_cycles", true),
        ("interleaved_two_key_cycles", false),
    ] {
        let mut fixture = Fixture::new(&root, name, &schema);
        let input = input(&schema, rows, contiguous);
        validate(&fixture.apply(&input), &input);
        group.bench_function(name, |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    let action = fixture.apply(&input);
                    elapsed += started.elapsed();
                    validate(&action, &input);
                }
                elapsed
            });
        });
    }
    group.finish();
    criterion.final_summary();
}
