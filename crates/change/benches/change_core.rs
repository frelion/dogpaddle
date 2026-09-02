//! In-memory construction, slicing, and projection scenarios for `Change`.

use std::{hint::black_box, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use dogpaddle_bench_protocol::{BenchmarkProfile, CaseId, Plan, Run};
use dogpaddle_change::{Change, ChangeProjection, encode_change};

use support::{
    fixture::{Fixture, fixtures},
    runner::{Config, FixturePlan, Timed, plan_fixtures, record, timed},
};

mod support;

const BENCHMARK: &str = "change_core";
const SCENARIOS: &[&str] = &["try_new", "projection_new", "try_slice", "try_project"];

fn main() {
    let profile = BenchmarkProfile::from_environment();
    let config = Config::load(profile);
    let mut plan = Plan::new(profile, config.fields());
    let plans = plan_fixtures(&mut plan, &config, SCENARIOS);
    let mut run = Run::memory(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    let mut plans = plans.iter();
    for &rows in &config.rows {
        for fixture in fixtures(rows, config.payload_bytes, &config.workloads) {
            benchmark_fixture(
                &config,
                &fixture,
                plans.next().expect("one frozen plan per Change fixture"),
                &mut run,
            );
        }
    }
    assert!(
        plans.next().is_none(),
        "all Change fixture plans are consumed"
    );
    run.finish(|| {});
}

fn benchmark_fixture(config: &Config, fixture: &Fixture, plan: &FixturePlan, run: &mut Run) {
    let rows = fixture.change.num_rows();
    let iterations = config.iterations(rows);
    let encoded_bytes = encode_change(&fixture.change)
        .expect("encode valid benchmark fixture")
        .len();
    plan.observe(run, fixture, encoded_bytes);
    let schema = fixture.change.schema();
    let projection =
        ChangeProjection::try_new(Arc::clone(&schema), fixture.narrow_fields.iter().copied())
            .expect("construct valid narrow benchmark projection");
    let slice_offset = usize::from(rows > 1) * (rows / 4);
    let slice_length = if rows > 1 { (rows / 2).max(1) } else { 1 };
    validate_slice(&fixture.change, slice_offset, slice_length);
    validate_projection(&fixture.change, &projection, fixture.narrow_fields);
    benchmark(run, plan.case("try_new"), config.samples, || {
        measure_try_new(&fixture.change, iterations)
    });
    benchmark(run, plan.case("projection_new"), config.samples, || {
        measure_projection_new(&schema, fixture.narrow_fields, iterations)
    });
    benchmark(run, plan.case("try_slice"), config.samples, || {
        measure_slice(&fixture.change, slice_offset, slice_length, iterations)
    });
    benchmark(run, plan.case("try_project"), config.samples, || {
        measure_project(&fixture.change, &projection, iterations)
    });
}

fn benchmark(run: &mut Run, case: CaseId, samples: usize, mut operation: impl FnMut() -> Timed) {
    let warm = operation();
    black_box(warm.checksum);
    let measurements = (0..samples)
        .map(|_| {
            let measurement = operation();
            assert_eq!(measurement.checksum, warm.checksum);
            measurement
        })
        .collect::<Vec<_>>();
    record(run, case, &measurements);
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

fn measure_try_new(source: &Change, iterations: usize) -> Timed {
    let records = (0..iterations)
        .map(|_| source.records().clone())
        .collect::<Vec<_>>();
    let diffs = (0..iterations)
        .map(|_| source.diffs().clone())
        .collect::<Vec<_>>();
    let expected = u64::try_from(source.num_rows()).expect("row count fits in u64");
    let mut inputs = records.into_iter().zip(diffs);
    let measurement = timed(iterations, || {
        let (records, diffs) = inputs.next().expect("one prepared input per iteration");
        let change = Change::try_new(records, diffs).expect("reconstruct valid benchmark Change");
        black_box(change.records());
        u64::try_from(change.num_rows()).expect("row count fits in u64")
    });
    assert_eq!(
        measurement.checksum,
        expected.wrapping_mul(u64::try_from(iterations).expect("iteration count fits in u64"))
    );
    measurement
}

fn measure_projection_new(
    schema: &arrow_schema::SchemaRef,
    fields: &[usize],
    iterations: usize,
) -> Timed {
    let schemas = (0..iterations)
        .map(|_| Arc::clone(schema))
        .collect::<Vec<_>>();
    let expected = u64::try_from(fields.len()).expect("field count fits in u64");
    let mut schemas = schemas.into_iter();
    let measurement = timed(iterations, || {
        let schema = schemas.next().expect("one prepared Schema per iteration");
        let projection = ChangeProjection::try_new(schema, fields.iter().copied())
            .expect("construct valid benchmark projection");
        black_box(projection.output_schema());
        expected
    });
    assert_eq!(
        measurement.checksum,
        expected.wrapping_mul(u64::try_from(iterations).expect("iteration count fits in u64"))
    );
    measurement
}

fn measure_slice(change: &Change, offset: usize, length: usize, iterations: usize) -> Timed {
    let expected = u64::try_from(length).expect("slice length fits in u64");
    let measurement = timed(iterations, || {
        let slice = change
            .try_slice(offset, length)
            .expect("slice valid benchmark Change");
        black_box(slice.records());
        u64::try_from(slice.num_rows()).expect("row count fits in u64")
    });
    assert_eq!(
        measurement.checksum,
        expected.wrapping_mul(u64::try_from(iterations).expect("iteration count fits in u64"))
    );
    measurement
}

fn measure_project(change: &Change, projection: &ChangeProjection, iterations: usize) -> Timed {
    let expected = u64::try_from(projection.output_schema().fields().len())
        .expect("field count fits in u64")
        .wrapping_add(u64::try_from(change.num_rows()).expect("row count fits in u64"));
    let measurement = timed(iterations, || {
        let projected = change
            .try_project(projection)
            .expect("project valid benchmark Change");
        black_box(projected.records());
        u64::try_from(projected.records().num_columns())
            .expect("column count fits in u64")
            .wrapping_add(u64::try_from(projected.num_rows()).expect("row count fits in u64"))
    });
    assert_eq!(
        measurement.checksum,
        expected.wrapping_mul(u64::try_from(iterations).expect("iteration count fits in u64"))
    );
    measurement
}
