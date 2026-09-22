//! Expression projection execution, excluding binding and Store commit overhead.
use std::{hint::black_box, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col,
    operation::{
        Operation, OperationInput,
        transform::{SchemaAlignDefinition, SchemaAlignField, SelectDefinition},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::StoreSetup;
use serde_json::json;

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|arg| arg == "--bench") {
        require_release_build("projection");
    }
    let root = RunRoot::for_profile("projection", profile);
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&json!({
            "benchmark": "projection", "profile": profile,
            "host": HostEnvironment::collect(Some(root.filesystem_root())),
            "widths": [8, 128, 512], "rows": [1, 256],
            "column_projection_rows": [1, 256, 65536],
            "column_projection_cases": ["identity", "subset", "decimal", "empty"],
            "timed_boundary": "atomic apply and output drop; no Store commit",
            "untimed": "construction, fixture, validation, transaction creation",
        }))
        .unwrap(),
    )
    .unwrap();
    println!("projection artifacts: {}", root.path().display());
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(200),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    bench_projections(&mut criterion, &root);
    bench_column_projection(&mut criterion, &root);
    criterion.final_summary();
}

fn bench_projections(criterion: &mut Criterion, root: &RunRoot) {
    let mut group = criterion.benchmark_group("projection");
    for width in [8, 128, 512] {
        let schema = Arc::new(Schema::new(
            (0..width)
                .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        for rows in [1, 256] {
            let values = Arc::new(Int64Array::from(vec![7; rows])) as ArrayRef;
            let input = Change::try_new(
                RecordBatch::try_new(Arc::clone(&schema), vec![values; width]).unwrap(),
                Int64Array::from(vec![1; rows]),
            )
            .unwrap();
            let definitions: [(&str, Box<dyn OperationDefinition>); 2] = [
                (
                    "select",
                    Box::new(
                        SelectDefinition::try_new(
                            (0..width).map(|i| (format!("v{i}"), col(format!("v{i}")))),
                        )
                        .unwrap(),
                    ),
                ),
                (
                    "align",
                    Box::new(
                        SchemaAlignDefinition::try_new((0..width).map(|i| {
                            SchemaAlignField::try_new(format!("v{i}"), col(format!("v{i}")), false)
                                .unwrap()
                        }))
                        .unwrap(),
                    ),
                ),
            ];
            for (name, definition) in definitions {
                let sample = root.sample(name);
                let mut setup = StoreSetup::new();
                let (operation, _) = definition
                    .construct(
                        &[Arc::clone(&schema)],
                        &mut setup.data_scope().scoped("projection"),
                        RuntimeResource::none(),
                    )
                    .unwrap()
                    .into_parts();
                let Operation::Atomic(mut operation) = operation else {
                    panic!("projection must be atomic")
                };
                let mut transactions = setup
                    .commit(sample.path().join("store"), |_| Ok(()))
                    .unwrap();
                let transaction = transactions.begin();
                let mut apply = || {
                    operation
                        .apply(
                            OperationInput {
                                port: 0,
                                change: &input,
                            },
                            transaction.access(),
                        )
                        .unwrap()
                        .unwrap()
                };
                let output = apply();
                assert_eq!(output.records(), input.records());
                assert_eq!(output.diffs(), input.diffs());
                group.bench_function(BenchmarkId::new(name, format!("{width}x{rows}")), |b| {
                    b.iter(|| black_box(apply()));
                });
                let output = apply();
                assert_eq!(output.records(), input.records());
                assert_eq!(output.diffs(), input.diffs());
            }
        }
    }
    group.finish();
}

// Row-count scaling of the exact selection path formerly owned by Project.
fn bench_column_projection(criterion: &mut Criterion, root: &RunRoot) {
    use arrow_array::Decimal128Array;
    let mut group = criterion.benchmark_group("column_projection");
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
    ]));
    for rows in [1, 256, 65_536] {
        let input = Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![7; rows])),
                    Arc::new(
                        Decimal128Array::from(vec![123_i128; rows])
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap(),
            Int64Array::from(vec![1; rows]),
        )
        .unwrap();
        for (name, indices) in [
            ("identity", vec![0, 1]),
            ("subset", vec![0]),
            ("decimal", vec![1]),
            ("empty", vec![]),
        ] {
            let definition = SelectDefinition::try_new(indices.iter().map(|&index| {
                let name = schema.field(index).name();
                (name.clone(), col(name.as_str()))
            }))
            .unwrap();
            let sample = root.sample(name);
            let mut setup = StoreSetup::new();
            let (operation, _) = (&definition as &dyn OperationDefinition)
                .construct(
                    &[Arc::clone(&schema)],
                    &mut setup.data_scope().scoped("projection"),
                    RuntimeResource::none(),
                )
                .unwrap()
                .into_parts();
            let Operation::Atomic(mut operation) = operation else {
                panic!("projection must be atomic")
            };
            let mut transactions = setup
                .commit(sample.path().join("store"), |_| Ok(()))
                .unwrap();
            let transaction = transactions.begin();
            let mut apply = || {
                operation
                    .apply(
                        OperationInput {
                            port: 0,
                            change: &input,
                        },
                        transaction.access(),
                    )
                    .unwrap()
                    .unwrap()
            };
            let expected = input.records().project(&indices).unwrap();
            let output = apply();
            assert_eq!(output.records(), &expected);
            assert_eq!(output.diffs(), input.diffs());
            group.bench_function(BenchmarkId::new(name, rows), |b| {
                b.iter(|| black_box(apply()));
            });
            let output = apply();
            assert_eq!(output.records(), &expected);
            assert_eq!(output.diffs(), input.diffs());
        }
    }
    group.finish();
}
