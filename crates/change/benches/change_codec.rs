//! Arrow IPC encoding and full or selective decoding scenarios for `Change`.

use std::{hint::black_box, sync::Arc};

use dogpaddle_bench_protocol::require_benchmark_build;
use dogpaddle_change::{
    Change, ChangeProjection, decode_change, decode_change_projected, encode_change,
};

use support::{
    fixture::{Fixture, fixtures},
    runner::{BenchmarkCase, Config, MachineRecords, Measurement, Metric, timed},
};

#[path = "support/codec_report.rs"]
mod report;
mod support;

const BENCHMARK: &str = "change_codec";

#[derive(Clone, Copy)]
enum CodecMode<'fixture> {
    Encode(&'fixture Change),
    DecodeFull(&'fixture [u8]),
    DecodeProjected(&'fixture [u8], &'fixture ChangeProjection),
}

struct CodecCase<'fixture> {
    scenario: &'static str,
    mode: CodecMode<'fixture>,
    warm_checksum: Option<u64>,
    measurements: Vec<Measurement>,
}

impl CodecMode<'_> {
    fn measure(self, iterations: usize) -> Measurement {
        match self {
            Self::Encode(change) => measure_encode(change, iterations),
            Self::DecodeFull(encoded) => measure_decode(encoded, iterations),
            Self::DecodeProjected(encoded, projection) => {
                measure_decode_projected(encoded, projection, iterations)
            }
        }
    }
}

fn main() {
    require_benchmark_build(BENCHMARK);

    let config = Config::load();
    config.print(BENCHMARK, "DogPaddle Change codec benchmark");
    report::print_header("one complete self-contained Arrow IPC Stream per Change");
    let mut records = MachineRecords::new(BENCHMARK);
    for &rows in &config.rows {
        for fixture in fixtures(rows, config.payload_bytes, &config.workloads) {
            benchmark_fixture(&config, &fixture, &mut records);
        }
    }
    records.print();
}

fn benchmark_fixture(config: &Config, fixture: &Fixture, records: &mut MachineRecords) {
    let rows = fixture.change.num_rows();
    let iterations = config.iterations(rows);
    let encoded = encode_change(&fixture.change).expect("encode valid benchmark fixture");
    let schema = fixture.change.schema();
    let diff_only = ChangeProjection::try_new(Arc::clone(&schema), [])
        .expect("construct diff-only benchmark projection");
    let narrow =
        ChangeProjection::try_new(Arc::clone(&schema), fixture.narrow_fields.iter().copied())
            .expect("construct narrow benchmark projection");
    let identity = ChangeProjection::try_new(Arc::clone(&schema), 0..schema.fields().len())
        .expect("construct identity benchmark projection");

    validate_fixture(fixture, &encoded, &diff_only, &narrow, &identity);
    let mut cases = vec![
        CodecCase {
            scenario: "encode",
            mode: CodecMode::Encode(&fixture.change),
            warm_checksum: None,
            measurements: Vec::with_capacity(config.samples),
        },
        CodecCase {
            scenario: "decode_full",
            mode: CodecMode::DecodeFull(&encoded),
            warm_checksum: None,
            measurements: Vec::with_capacity(config.samples),
        },
        CodecCase {
            scenario: "decode_diff_only",
            mode: CodecMode::DecodeProjected(&encoded, &diff_only),
            warm_checksum: None,
            measurements: Vec::with_capacity(config.samples),
        },
        CodecCase {
            scenario: "decode_narrow",
            mode: CodecMode::DecodeProjected(&encoded, &narrow),
            warm_checksum: None,
            measurements: Vec::with_capacity(config.samples),
        },
        CodecCase {
            scenario: "decode_identity",
            mode: CodecMode::DecodeProjected(&encoded, &identity),
            warm_checksum: None,
            measurements: Vec::with_capacity(config.samples),
        },
    ];

    // Warm every case independently before collecting paired samples. Rotating
    // the first case in each sample avoids consistently favouring one mode.
    for case in &mut cases {
        let warm = case.mode.measure(iterations);
        black_box(warm.checksum);
        case.warm_checksum = Some(warm.checksum);
    }
    let case_count = cases.len();
    for sample in 0..config.samples {
        for position in 0..case_count {
            let index = (sample + position) % case_count;
            let measurement = cases[index].mode.measure(iterations);
            assert_eq!(measurement.checksum, cases[index].warm_checksum.unwrap());
            cases[index].measurements.push(measurement);
        }
    }

    let metric = Metric::new(rows, encoded.len(), iterations);
    for case in cases {
        report::measurements(
            BenchmarkCase::new(fixture.name, case.scenario, metric),
            &case.measurements,
            records,
        );
    }
}

fn validate_fixture(
    fixture: &Fixture,
    encoded: &[u8],
    diff_only: &ChangeProjection,
    narrow: &ChangeProjection,
    identity: &ChangeProjection,
) {
    let decoded = decode_change(encoded).expect("decode valid benchmark fixture");
    assert_eq!(decoded.records(), fixture.change.records());
    assert_eq!(decoded.diffs(), fixture.change.diffs());
    for projection in [diff_only, narrow, identity] {
        let decoded = decode_change_projected(encoded, projection)
            .expect("selectively decode valid benchmark fixture");
        let expected = fixture
            .change
            .try_project(projection)
            .expect("project valid benchmark fixture");
        assert_eq!(decoded.records(), expected.records());
        assert_eq!(decoded.diffs(), expected.diffs());
    }
}

fn measure_encode(change: &Change, iterations: usize) -> Measurement {
    timed(iterations, || {
        let encoded = encode_change(black_box(change)).expect("encode valid benchmark Change");
        black_box(encoded.as_slice());
        u64::try_from(encoded.len()).expect("encoded length fits in u64")
    })
}

fn measure_decode(encoded: &[u8], iterations: usize) -> Measurement {
    timed(iterations, || {
        let decoded = decode_change(black_box(encoded)).expect("decode valid benchmark Change");
        black_box(decoded.records());
        decoded_checksum(&decoded)
    })
}

fn measure_decode_projected(
    encoded: &[u8],
    projection: &ChangeProjection,
    iterations: usize,
) -> Measurement {
    timed(iterations, || {
        let decoded = decode_change_projected(black_box(encoded), projection)
            .expect("selectively decode valid benchmark Change");
        black_box(decoded.records());
        decoded_checksum(&decoded)
    })
}

fn decoded_checksum(change: &Change) -> u64 {
    let rows = u64::try_from(change.num_rows()).expect("row count fits in u64");
    let columns = u64::try_from(change.records().num_columns()).expect("column count fits in u64");
    let first_diff = change.diffs().value(0).unsigned_abs();
    rows.wrapping_mul(31)
        .wrapping_add(columns.wrapping_mul(17))
        .wrapping_add(first_diff)
}
