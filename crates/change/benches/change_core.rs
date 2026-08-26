//! In-memory construction, slicing, and projection scenarios for `Change`.

use std::{hint::black_box, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use dogpaddle_change::{Change, ChangeProjection, encode_change};

use support::{
    fixture::{Fixture, fixtures},
    runner::{
        Config, Measurement, SampleRecord, print_core_header, print_sample_csv, report_latency,
        report_rows, timed,
    },
};

mod support;

fn main() {
    if cfg!(debug_assertions) {
        return;
    }

    let config = Config::load();
    config.print("DogPaddle Change core benchmark");
    print_core_header("Change public in-memory operations; '-' means rows/s is not meaningful");
    let mut samples = Vec::<SampleRecord>::new();
    for &rows in &config.rows {
        for fixture in fixtures(rows, config.payload_bytes, &config.workloads) {
            benchmark_fixture(&config, &fixture, &mut samples);
        }
    }
    print_sample_csv(&samples);
}

fn benchmark_fixture(config: &Config, fixture: &Fixture, samples: &mut Vec<SampleRecord>) {
    let rows = fixture.change.num_rows();
    let iterations = config.iterations(rows);
    let encoded_bytes = encode_change(&fixture.change)
        .expect("encode valid benchmark fixture")
        .len();
    let schema = fixture.change.schema();
    let projection =
        ChangeProjection::try_new(Arc::clone(&schema), fixture.narrow_fields.iter().copied())
            .expect("construct valid narrow benchmark projection");
    let slice_offset = usize::from(rows > 1) * (rows / 4);
    let slice_length = if rows > 1 { (rows / 2).max(1) } else { 1 };
    validate_slice(&fixture.change, slice_offset, slice_length);
    validate_projection(&fixture.change, &projection, fixture.narrow_fields);

    report_rows(
        fixture.name,
        "try_new",
        rows,
        encoded_bytes,
        iterations,
        config.samples,
        samples,
        || measure_try_new(&fixture.change, iterations),
    );
    report_latency(
        fixture.name,
        "projection_new",
        rows,
        encoded_bytes,
        iterations,
        config.samples,
        samples,
        || measure_projection_new(&schema, fixture.narrow_fields, iterations),
    );
    report_latency(
        fixture.name,
        "try_slice",
        rows,
        encoded_bytes,
        iterations,
        config.samples,
        samples,
        || measure_slice(&fixture.change, slice_offset, slice_length, iterations),
    );
    report_latency(
        fixture.name,
        "try_project",
        rows,
        encoded_bytes,
        iterations,
        config.samples,
        samples,
        || measure_project(&fixture.change, &projection, iterations),
    );
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

fn measure_try_new(source: &Change, iterations: usize) -> Measurement {
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
) -> Measurement {
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

fn measure_slice(change: &Change, offset: usize, length: usize, iterations: usize) -> Measurement {
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

fn measure_project(
    change: &Change,
    projection: &ChangeProjection,
    iterations: usize,
) -> Measurement {
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
