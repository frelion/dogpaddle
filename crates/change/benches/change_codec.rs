//! Owner-local rotating JSONL measurements for Change's Arrow IPC codec.

use std::{
    hint::black_box,
    io::{self, BufWriter, Write},
    sync::Arc,
    time::{Duration, Instant},
};

use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use serde_json::{Value, json};

use support::fixture::{DEFAULT_WORKLOADS, Fixture, fixtures, validate_dimensions};

mod support;

const BENCHMARK: &str = "change_codec";
const RECORD_SCHEMA: &str = "dogpaddle.change-codec.v1";
const SMOKE_ROWS: &[usize] = &[4];
const REFERENCE_ROWS: &[usize] = &[1, 64, 1_024, 16_384];
const SCENARIOS: &[&str] = &["encode_schema_bound", "decode_full_schema_bound"];

struct Config {
    rows: &'static [usize],
    payload_bytes: usize,
    samples: usize,
    target_rows: usize,
    max_changes: usize,
    workloads: Vec<String>,
}

impl Config {
    fn for_profile(profile: PerformanceProfile) -> Self {
        let (rows, payload_bytes, samples, target_rows, max_changes) = match profile {
            PerformanceProfile::Smoke => (SMOKE_ROWS, 16, 1, 4, 1),
            PerformanceProfile::Reference => (REFERENCE_ROWS, 1_024, 9, 65_536, 1_024),
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
            samples,
            target_rows,
            max_changes,
            workloads,
        }
    }

    fn iterations(&self, rows: usize) -> usize {
        self.target_rows.div_ceil(rows).clamp(1, self.max_changes)
    }
}

#[derive(Clone, Copy)]
enum CodecMode<'fixture> {
    EncodeSchemaBound(&'fixture SchemaBoundChangeCodec, &'fixture Change),
    DecodeFullSchemaBound(&'fixture SchemaBoundChangeCodec, &'fixture [u8]),
}

struct CodecCase<'fixture> {
    scenario: &'static str,
    mode: CodecMode<'fixture>,
    warm_checksum: u64,
}

#[derive(Clone, Copy)]
struct Timed {
    elapsed: Duration,
    checksum: u64,
}

impl CodecMode<'_> {
    fn measure(self, iterations: usize) -> Timed {
        match self {
            Self::EncodeSchemaBound(codec, change) => {
                measure_encode_schema_bound(codec, change, iterations)
            }
            Self::DecodeFullSchemaBound(codec, encoded) => {
                measure_decode_schema_bound(codec, encoded, iterations)
            }
        }
    }
}

fn main() {
    if !std::env::args_os().any(|argument| argument == "--bench") {
        return;
    }
    require_release_build(BENCHMARK);
    let profile = PerformanceProfile::from_environment();
    let config = Config::for_profile(profile);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let stdout = io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    emit(
        &mut output,
        &json!({
            "schema": RECORD_SCHEMA,
            "record": "context",
            "benchmark": BENCHMARK,
            "profile": profile,
            "result_directory": root.path().display().to_string(),
            "host": HostEnvironment::collect(Some(root.filesystem_root())),
            "configuration": {
                "rows_per_change": config.rows,
                "payload_bytes": config.payload_bytes,
                "samples_per_fixture": config.samples,
                "target_rows_per_sample": config.target_rows,
                "max_changes_per_sample": config.max_changes,
                "workloads": config.workloads,
                "scenarios": SCENARIOS,
                "execution": "single_thread",
                "cache": "warm",
                "validation": "outside_timing",
                "sample_order": "two_way_rotating_first",
            },
        }),
    );

    let mut fixture_count = 0_usize;
    let mut sample_count = 0_usize;
    for &rows in config.rows {
        for fixture in fixtures(rows, config.payload_bytes, &config.workloads) {
            eprintln!(
                "{BENCHMARK}: workload={} rows={rows} samples={}",
                fixture.name, config.samples
            );
            sample_count += benchmark_fixture(&config, &fixture, &mut output);
            fixture_count += 1;
        }
    }
    emit(
        &mut output,
        &json!({
            "schema": RECORD_SCHEMA,
            "record": "complete",
            "benchmark": BENCHMARK,
            "profile": profile,
            "fixtures": fixture_count,
            "paired_samples": sample_count,
        }),
    );
    output.flush().expect("flush Change codec JSONL");
    eprintln!("{BENCHMARK}: complete fixtures={fixture_count} paired_samples={sample_count}");
}

fn benchmark_fixture(config: &Config, fixture: &Fixture, output: &mut impl Write) -> usize {
    let rows = fixture.change.num_rows();
    let iterations = config.iterations(rows);
    let schema = fixture.change.schema();
    let bound_codec = SchemaBoundChangeCodec::try_new(Arc::clone(&schema))
        .expect("bind valid benchmark fixture Schema");
    let bound_encoded = bound_codec
        .encode(&fixture.change)
        .expect("encode valid schema-bound benchmark fixture");
    let decoded = bound_codec
        .decode(&bound_encoded)
        .expect("decode valid benchmark fixture");
    assert_eq!(decoded.records(), fixture.change.records());
    assert_eq!(decoded.diffs(), fixture.change.diffs());
    drop(decoded);
    emit(
        output,
        &json!({
            "schema": RECORD_SCHEMA,
            "record": "fixture",
            "workload": fixture.name,
            "rows_per_change": rows,
            "operations_per_measurement": iterations,
            "schema_bound_bytes_per_change": bound_encoded.len(),
        }),
    );

    let modes = [
        CodecMode::EncodeSchemaBound(&bound_codec, &fixture.change),
        CodecMode::DecodeFullSchemaBound(&bound_codec, &bound_encoded),
    ];
    let cases = SCENARIOS
        .iter()
        .zip(modes)
        .map(|(&scenario, mode)| {
            let warm = mode.measure(iterations);
            black_box(warm.checksum);
            CodecCase {
                scenario,
                mode,
                warm_checksum: warm.checksum,
            }
        })
        .collect::<Vec<_>>();

    let case_count = cases.len();
    for sample in 0..config.samples {
        let mut measurements = Vec::with_capacity(case_count);
        for position in 0..case_count {
            let index = (sample + position) % case_count;
            let case = &cases[index];
            let measurement = case.mode.measure(iterations);
            assert_eq!(measurement.checksum, case.warm_checksum);
            measurements.push(json!({
                "scenario": case.scenario,
                "execution_position": position,
                "elapsed_ns": nanos(measurement.elapsed),
                "checksum": measurement.checksum,
            }));
        }
        emit(
            output,
            &json!({
                "schema": RECORD_SCHEMA,
                "record": "sample",
                "workload": fixture.name,
                "rows_per_change": rows,
                "operations": iterations,
                "sample": sample,
                "first_scenario": cases[sample % case_count].scenario,
                "measurements": measurements,
            }),
        );
    }
    config.samples
}

fn measure_encode_schema_bound(
    codec: &SchemaBoundChangeCodec,
    change: &Change,
    iterations: usize,
) -> Timed {
    timed(iterations, || {
        let encoded = codec
            .encode(black_box(change))
            .expect("encode valid schema-bound benchmark Change");
        black_box(encoded.as_slice());
        u64::try_from(encoded.len()).expect("encoded length fits in u64")
    })
}

fn measure_decode_schema_bound(
    codec: &SchemaBoundChangeCodec,
    encoded: &[u8],
    iterations: usize,
) -> Timed {
    timed(iterations, || {
        let decoded = codec
            .decode(black_box(encoded))
            .expect("decode valid schema-bound benchmark Change");
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

fn timed(iterations: usize, mut operation: impl FnMut() -> u64) -> Timed {
    let mut checksum = 0_u64;
    let started = Instant::now();
    for _ in 0..iterations {
        checksum = checksum.wrapping_add(operation());
    }
    black_box(checksum);
    Timed {
        elapsed: started.elapsed(),
        checksum,
    }
}

fn emit(output: &mut impl Write, record: &Value) {
    serde_json::to_writer(&mut *output, record).expect("serialize Change codec JSONL record");
    output
        .write_all(b"\n")
        .expect("write Change codec JSONL record terminator");
    output.flush().expect("flush Change codec JSONL record");
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("Change codec duration fits u64 nanoseconds")
}
