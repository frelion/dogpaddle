#[path = "support/mod.rs"]
mod support;

use std::{
    fs,
    hint::black_box,
    path::Path,
    time::{Duration, Instant},
};

use arrow_array::UInt64Array;
use criterion::{BenchmarkId, Criterion, Throughput};
use dogpaddle_change::{Change, ChangeProjection, decode_change_projected};
use dogpaddle_change_store_integration::{
    EncodedChanges, heterogeneous_pages_fixture, order_checksum, projectable_fixture,
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{
    AppendLog, Cell, CodecError as StoreCodecError, ScanLimit, Store, StoreError,
};
use serde_json::json;

use support::{SampleStore, decode_entry};

const BENCHMARK: &str = "change_append_log";

#[derive(Clone, Copy)]
struct Config {
    rows_per_change: usize,
    changes_per_transaction: usize,
    transactions_per_iteration: usize,
    payload_bytes: usize,
    sample_size: usize,
    warm_up_time: Duration,
    measurement_time: Duration,
    max_working_set_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScenarioMeasurement {
    elapsed: Duration,
    pages: usize,
    checksum: u64,
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if is_cargo_bench() {
        require_release_build(BENCHMARK);
    }
    let config = Config::for_profile(profile);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    write_context(&root, profile, config);
    let mut criterion = Criterion::default()
        .sample_size(config.sample_size)
        .warm_up_time(config.warm_up_time)
        .measurement_time(config.measurement_time)
        .without_plots()
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    benchmark(&mut criterion, &root, config);
    criterion.final_summary();
}

impl Config {
    fn for_profile(profile: PerformanceProfile) -> Self {
        let config = match profile {
            PerformanceProfile::Smoke => Self {
                rows_per_change: 8,
                changes_per_transaction: 2,
                transactions_per_iteration: 2,
                payload_bytes: 16,
                sample_size: 10,
                warm_up_time: Duration::from_millis(20),
                measurement_time: Duration::from_millis(50),
                max_working_set_bytes: 64 * 1_024 * 1_024,
            },
            PerformanceProfile::Reference => Self {
                rows_per_change: 1_024,
                changes_per_transaction: 32,
                transactions_per_iteration: 8,
                payload_bytes: 256,
                sample_size: 15,
                warm_up_time: Duration::from_secs(3),
                measurement_time: Duration::from_secs(5),
                max_working_set_bytes: 512 * 1_024 * 1_024,
            },
        };
        assert!(config.total_changes() >= 2, "benchmark needs two Changes");
        config
    }

    fn total_changes(&self) -> usize {
        self.changes_per_transaction
            .checked_mul(self.transactions_per_iteration)
            .expect("benchmark Change count fits usize")
    }
}

fn benchmark(criterion: &mut Criterion, root: &RunRoot, config: Config) {
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(config.total_changes()).expect("Change count fits u64"),
    ));
    group.bench_function(
        BenchmarkId::new("append_durable", config.total_changes()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || measure_append(root, &config))
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("full_replay", config.total_changes()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || measure_full_replay(root, &config))
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("projected_replay", config.total_changes()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || measure_projected_replay(root, &config))
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("consumer_durable", config.total_changes()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || measure_consumer(root, &config))
            });
        },
    );
    group.finish();
}

fn measure_iterations(
    iterations: u64,
    mut measure: impl FnMut() -> ScenarioMeasurement,
) -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut oracle = None;
    for _ in 0..iterations {
        let measurement = measure();
        let actual_oracle = (measurement.pages, measurement.checksum);
        if let Some(expected) = oracle {
            assert_eq!(
                actual_oracle, expected,
                "benchmark oracle changed between iterations"
            );
        } else {
            oracle = Some(actual_oracle);
        }
        black_box(actual_oracle);
        elapsed = elapsed
            .checked_add(measurement.elapsed)
            .expect("benchmark elapsed duration fits Duration");
    }
    elapsed
}

fn representative_workload(config: &Config) -> EncodedChanges {
    let workload = heterogeneous_pages_fixture(
        config.total_changes(),
        config.rows_per_change,
        config.payload_bytes,
    );
    assert!(
        workload.scan_bytes() <= config.max_working_set_bytes,
        "encoded workload exceeds working-set budget"
    );
    workload
}

fn measure_append(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let workload = representative_workload(config);
    let sample = SampleStore::new(run, "append");
    let mut store = Store::create(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();

    let started = Instant::now();
    for batch in workload.encoded.chunks(config.changes_per_transaction) {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(batch)
            .unwrap();
        transaction.commit().unwrap();
    }
    let elapsed = started.elapsed();
    drop(transactions);
    validate_log(sample.path(), "changes", &workload.encoded);
    ScenarioMeasurement {
        elapsed,
        pages: config.transactions_per_iteration,
        checksum: workload.order_checksum(),
    }
}

fn measure_full_replay(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let workload = representative_workload(config);
    let sample = SampleStore::new(run, "full-replay");
    seed_log(sample.path(), "changes", &workload.encoded);
    let expected_checksum = changes_checksum(&workload.changes);

    let store = Store::open(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let started = Instant::now();
    let (pages, checksum) = scan_decoded(
        &access,
        workload.encoded.len(),
        config.changes_per_transaction,
        config.max_working_set_bytes,
        None,
    );
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_checksum);
    drop(transaction);
    drop(transactions);
    validate_log(sample.path(), "changes", &workload.encoded);
    ScenarioMeasurement {
        elapsed,
        pages,
        checksum,
    }
}

fn measure_projected_replay(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let fixtures = (0..config.total_changes())
        .map(|index| {
            projectable_fixture(
                10_000 + u64::try_from(index * config.rows_per_change).unwrap(),
                config.rows_per_change,
                config.payload_bytes,
            )
        })
        .collect::<Vec<_>>();
    let encoded = fixtures
        .iter()
        .map(|fixture| fixture.encoded.clone())
        .collect::<Vec<_>>();
    let projected = fixtures
        .iter()
        .map(|fixture| fixture.projected.clone())
        .collect::<Vec<_>>();
    let projection = &fixtures[0].projection;
    assert!(
        encoded
            .iter()
            .map(|entry| entry.len() + size_of::<u64>())
            .sum::<usize>()
            <= config.max_working_set_bytes,
        "encoded workload exceeds working-set budget"
    );
    let sample = SampleStore::new(run, "projected-replay");
    seed_log(sample.path(), "changes", &encoded);
    let expected_checksum = changes_checksum(&projected);

    let store = Store::open(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let started = Instant::now();
    let (pages, checksum) = scan_decoded(
        &access,
        encoded.len(),
        config.changes_per_transaction,
        config.max_working_set_bytes,
        Some(projection),
    );
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_checksum);
    drop(transaction);
    drop(transactions);
    validate_log(sample.path(), "changes", &encoded);
    ScenarioMeasurement {
        elapsed,
        pages,
        checksum,
    }
}

fn measure_consumer(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let workload = representative_workload(config);
    let sample = SampleStore::new(run, "consumer");
    let mut store = Store::create(sample.path()).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let cursor: Cell<u64> = store.create_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append_batch(&workload.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }

    let started = Instant::now();
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0_u64;
    while usize::try_from(offset).unwrap() < workload.encoded.len() {
        let transaction = transactions.begin().unwrap();
        let input_access = input.access(transaction.access()).unwrap();
        let mut output_access = output.access(transaction.access()).unwrap();
        let progress = input_access
            .scan(
                offset,
                ScanLimit::new(config.changes_per_transaction, config.max_working_set_bytes)
                    .unwrap(),
                |entry| {
                    let change = entry.project(decode_entry)?;
                    checksum = mix(checksum, change_checksum(&change));
                    output_access.append_entry(&entry)?;
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        offset = progress.next_offset;
        cursor
            .access(transaction.access())
            .unwrap()
            .set(&offset)
            .unwrap();
        transaction.commit().unwrap();
        pages += 1;
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, changes_checksum(&workload.changes));
    drop(transactions);
    validate_log(sample.path(), "output", &workload.encoded);
    validate_cursor(sample.path(), offset);
    ScenarioMeasurement {
        elapsed,
        pages,
        checksum,
    }
}

fn seed_log(path: &Path, name: &str, encoded: &[Vec<u8>]) {
    let mut store = Store::create(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data(name).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    log.access(transaction.access())
        .unwrap()
        .append_batch(encoded)
        .unwrap();
    transaction.commit().unwrap();
}

fn validate_log(path: &Path, name: &str, expected: &[Vec<u8>]) {
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data(name).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(
        access.bounds().unwrap(),
        0..u64::try_from(expected.len()).unwrap()
    );
    let mut raw = Vec::new();
    let progress = access
        .scan(
            0,
            ScanLimit::new(
                expected.len(),
                expected
                    .iter()
                    .map(|entry| entry.len() + size_of::<u64>())
                    .sum(),
            )
            .unwrap(),
            |entry| {
                raw.push(entry.project(|bytes| Ok(bytes.to_vec()))?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert!(progress.caught_up);
    assert_eq!(raw, expected);
    assert_eq!(order_checksum(&raw), order_checksum(expected));
}

fn validate_cursor(path: &Path, expected: u64) {
    let store = Store::open(path).unwrap();
    let cursor: Cell<u64> = store.open_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        cursor.access(transaction.access()).unwrap().get().unwrap(),
        Some(expected)
    );
}

fn scan_decoded(
    access: &dogpaddle_store::AppendLogAccess<'_, Vec<u8>>,
    entries: usize,
    page_items: usize,
    page_bytes: usize,
    projection: Option<&ChangeProjection>,
) -> (usize, u64) {
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0_u64;
    while usize::try_from(offset).unwrap() < entries {
        let progress = access
            .scan(
                offset,
                ScanLimit::new(page_items, page_bytes).unwrap(),
                |entry| {
                    let change = match projection {
                        Some(projection) => entry.project(|bytes| {
                            decode_change_projected(bytes, projection)
                                .map_err(|error| StoreCodecError::new(error.to_string()))
                        })?,
                        None => entry.project(decode_entry)?,
                    };
                    checksum = mix(checksum, change_checksum(&change));
                    black_box(&change);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        offset = progress.next_offset;
        pages += 1;
    }
    (pages, checksum)
}

fn changes_checksum(changes: &[Change]) -> u64 {
    changes.iter().fold(0_u64, |checksum, change| {
        mix(checksum, change_checksum(change))
    })
}

fn change_checksum(change: &Change) -> u64 {
    let ids = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("seam fixtures begin with UInt64 IDs");
    ids.values()
        .iter()
        .copied()
        .zip(change.diffs().values().iter().copied())
        .fold(
            mix(
                change.records().num_columns() as u64,
                change.num_rows() as u64,
            ),
            |checksum, (id, diff)| mix(mix(checksum, id), u64::from_ne_bytes(diff.to_ne_bytes())),
        )
}

const fn mix(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(0x0000_0100_0000_01b3)
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, config: Config) {
    let context = json!({
        "benchmark": BENCHMARK,
        "runner": "criterion",
        "criterion_version": "0.8.2",
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "fixtures": ["heterogeneous_pages", "projectable"],
            "rows_per_change": config.rows_per_change,
            "changes_per_transaction": config.changes_per_transaction,
            "transactions_per_iteration": config.transactions_per_iteration,
            "changes_per_iteration": config.total_changes(),
            "payload_bytes": config.payload_bytes,
            "sample_size": config.sample_size,
            "warm_up_time_ns": nanos(config.warm_up_time),
            "measurement_time_ns": nanos(config.measurement_time),
            "max_working_set_bytes": config.max_working_set_bytes,
            "scenarios": [
                "append_durable",
                "full_replay",
                "projected_replay",
                "consumer_durable"
            ],
            "timing_scope": {
                "append_durable": "transaction begin, append, and durable commit",
                "full_replay": "paged AppendLog scan, full IPC decode, and checksum",
                "projected_replay": "paged AppendLog scan, projected IPC decode, and checksum",
                "consumer_durable": "transaction begin, input decode, raw output append, cursor update, and durable commit"
            },
            "fixture_and_validation": "outside_timing",
            "execution": "single_thread",
            "mdbx_sync_mode": "durable"
        },
    });
    fs::write(
        root.path().join("criterion-context.json"),
        serde_json::to_vec_pretty(&context).expect("serialize Change + Store Criterion context"),
    )
    .expect("write Change + Store Criterion context");
}

fn is_cargo_bench() -> bool {
    std::env::args_os().any(|argument| argument == "--bench")
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos())
        .expect("Change + Store benchmark duration fits u64 nanoseconds")
}
