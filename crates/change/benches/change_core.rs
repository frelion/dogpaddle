//! Criterion measurements for in-memory Change construction and structural views.

use std::{fs, hint::black_box, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use dogpaddle_change::{Change, ChangeProjection, encode_change};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use serde_json::json;

use support::fixture::{DEFAULT_WORKLOADS, Fixture, fixtures, validate_dimensions};

mod support;

const BENCHMARK: &str = "change_core";
const SCENARIOS: &[&str] = &["try_new", "projection_new", "try_slice", "try_project"];
const SMOKE_ROWS: &[usize] = &[4];
const REFERENCE_ROWS: &[usize] = &[1, 64, 1_024, 16_384];

struct Config {
    rows: &'static [usize],
    payload_bytes: usize,
    workloads: Vec<String>,
    criterion_sample_size: usize,
    criterion_warm_up: Duration,
    criterion_measurement: Duration,
}

impl Config {
    fn for_profile(profile: PerformanceProfile) -> Self {
        let (rows, payload_bytes, criterion_sample_size, criterion_warm_up, criterion_measurement) =
            match profile {
                PerformanceProfile::Smoke => (
                    SMOKE_ROWS,
                    16,
                    10,
                    Duration::from_millis(20),
                    Duration::from_millis(50),
                ),
                PerformanceProfile::Reference => (
                    REFERENCE_ROWS,
                    1_024,
                    30,
                    Duration::from_secs(2),
                    Duration::from_secs(5),
                ),
            };
        let workloads = DEFAULT_WORKLOADS
            .iter()
            .map(|workload| (*workload).to_owned())
            .collect::<Vec<_>>();
        for &rows in rows {
            validate_dimensions(rows, payload_bytes, &workloads);
        }
        Self {
            rows,
            payload_bytes,
            workloads,
            criterion_sample_size,
            criterion_warm_up,
            criterion_measurement,
        }
    }
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if is_cargo_bench() {
        require_release_build(BENCHMARK);
    }
    let config = Config::for_profile(profile);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let fixtures = config
        .rows
        .iter()
        .flat_map(|&rows| fixtures(rows, config.payload_bytes, &config.workloads))
        .collect::<Vec<_>>();
    write_context(&root, profile, &config, &fixtures);

    let mut criterion = Criterion::default()
        .sample_size(config.criterion_sample_size)
        .warm_up_time(config.criterion_warm_up)
        .measurement_time(config.criterion_measurement)
        .without_plots()
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    benchmark(&mut criterion, &fixtures);
    criterion.final_summary();
}

fn benchmark(criterion: &mut Criterion, fixtures: &[Fixture]) {
    let mut group = criterion.benchmark_group(BENCHMARK);
    for fixture in fixtures {
        let rows = fixture.change.num_rows();
        let parameter = format!("{}/rows={rows}", fixture.name);
        let schema = fixture.change.schema();
        let projection =
            ChangeProjection::try_new(Arc::clone(&schema), fixture.narrow_fields.iter().copied())
                .expect("construct valid narrow benchmark projection");
        let slice_offset = usize::from(rows > 1) * (rows / 4);
        let slice_length = if rows > 1 { (rows / 2).max(1) } else { 1 };
        validate_fixture(fixture, &projection, slice_offset, slice_length);
        group.throughput(Throughput::Elements(
            u64::try_from(rows).expect("Change benchmark row count fits u64"),
        ));

        group.bench_function(BenchmarkId::new("try_new", &parameter), |bencher| {
            bencher.iter_batched(
                || {
                    (
                        fixture.change.records().clone(),
                        fixture.change.diffs().clone(),
                    )
                },
                |(records, diffs)| {
                    let change = Change::try_new(records, diffs)
                        .expect("reconstruct valid benchmark Change");
                    black_box(change);
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function(BenchmarkId::new("projection_new", &parameter), |bencher| {
            bencher.iter_batched(
                || Arc::clone(&schema),
                |schema| {
                    let projection =
                        ChangeProjection::try_new(schema, fixture.narrow_fields.iter().copied())
                            .expect("construct valid benchmark projection");
                    black_box(projection);
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function(BenchmarkId::new("try_slice", &parameter), |bencher| {
            bencher.iter(|| {
                black_box(
                    fixture
                        .change
                        .try_slice(slice_offset, slice_length)
                        .expect("slice valid benchmark Change"),
                );
            });
        });
        group.bench_function(BenchmarkId::new("try_project", &parameter), |bencher| {
            bencher.iter(|| {
                black_box(
                    fixture
                        .change
                        .try_project(&projection)
                        .expect("project valid benchmark Change"),
                );
            });
        });
    }
    group.finish();
}

fn validate_fixture(
    fixture: &Fixture,
    projection: &ChangeProjection,
    slice_offset: usize,
    slice_length: usize,
) {
    let reconstructed = Change::try_new(
        fixture.change.records().clone(),
        fixture.change.diffs().clone(),
    )
    .expect("reconstruct benchmark Change outside timing");
    assert_eq!(reconstructed.records(), fixture.change.records());
    assert_eq!(reconstructed.diffs(), fixture.change.diffs());
    validate_slice(&fixture.change, slice_offset, slice_length);
    validate_projection(&fixture.change, projection, fixture.narrow_fields);
}

fn validate_slice(change: &Change, offset: usize, length: usize) {
    let actual = change
        .try_slice(offset, length)
        .expect("slice valid benchmark Change outside timing");
    let expected_records = change.records().slice(offset, length);
    let expected_diffs = change.diffs().slice(offset, length);

    assert_eq!(actual.schema(), change.schema());
    assert_eq!(actual.records(), &expected_records);
    assert_eq!(actual.diffs(), &expected_diffs);
}

fn validate_projection(change: &Change, projection: &ChangeProjection, fields: &[usize]) {
    let input_schema = change.schema();
    let expected_schema = Arc::new(Schema::new_with_metadata(
        fields
            .iter()
            .map(|&index| Arc::clone(&input_schema.fields()[index]))
            .collect::<Vec<_>>(),
        input_schema.metadata().clone(),
    ));
    let expected_columns = fields
        .iter()
        .map(|&index| Arc::clone(change.records().column(index)))
        .collect::<Vec<ArrayRef>>();
    let expected_records = RecordBatch::try_new_with_options(
        Arc::clone(&expected_schema),
        expected_columns,
        &RecordBatchOptions::new().with_row_count(Some(change.num_rows())),
    )
    .expect("construct independent projected benchmark oracle");
    let actual = change
        .try_project(projection)
        .expect("project valid benchmark Change outside timing");

    assert_eq!(projection.output_schema(), expected_schema);
    assert_eq!(actual.schema(), expected_schema);
    assert_eq!(actual.records(), &expected_records);
    assert_eq!(actual.diffs(), change.diffs());
}

fn write_context(
    root: &RunRoot,
    profile: PerformanceProfile,
    config: &Config,
    fixtures: &[Fixture],
) {
    let fixture_context = fixtures
        .iter()
        .map(|fixture| {
            json!({
                "workload": fixture.name,
                "rows_per_change": fixture.change.num_rows(),
                "narrow_fields": fixture.narrow_fields,
                "encoded_bytes_per_change": encode_change(&fixture.change)
                    .expect("encode benchmark fixture outside timing")
                    .len(),
            })
        })
        .collect::<Vec<_>>();
    let context = json!({
        "benchmark": BENCHMARK,
        "runner": "criterion",
        "criterion_version": "0.8.2",
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "rows_per_change": config.rows,
            "payload_bytes": config.payload_bytes,
            "workloads": config.workloads,
            "sample_size": config.criterion_sample_size,
            "warm_up_time_ns": nanos(config.criterion_warm_up),
            "measurement_time_ns": nanos(config.criterion_measurement),
            "scenarios": SCENARIOS,
            "execution": "single_thread",
            "cache": "warm",
            "validation": "outside_timing",
        },
        "fixtures": fixture_context,
    });
    fs::write(
        root.path().join("criterion-context.json"),
        serde_json::to_vec_pretty(&context).expect("serialize Change core Criterion context"),
    )
    .expect("write Change core Criterion context");
}

fn is_cargo_bench() -> bool {
    std::env::args_os().any(|argument| argument == "--bench")
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("Change core duration fits u64 nanoseconds")
}
