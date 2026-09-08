#[path = "support/mod.rs"]
mod support;

use std::{
    fs,
    hint::black_box,
    num::NonZeroU64,
    path::Path,
    time::{Duration, Instant},
};

use arrow_array::UInt64Array;
use criterion::{BenchmarkId, Criterion, Throughput};
use dogpaddle_change::Change;
use dogpaddle_change_store_integration::{EncodedChanges, heterogeneous_changes_fixture};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Store, SubscribedLog};
use serde_json::json;

use support::{SampleStore, decode_entry};

const BENCHMARK: &str = "change_subscribed_log";

#[derive(Clone, Copy)]
struct Config {
    rows_per_change: usize,
    changes_per_transaction: usize,
    transactions_per_iteration: usize,
    payload_bytes: usize,
    sample_size: usize,
    warm_up_time: Duration,
    measurement_time: Duration,
    max_retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScenarioMeasurement {
    elapsed: Duration,
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
                max_retained_bytes: 64 * 1_024 * 1_024,
            },
            PerformanceProfile::Reference => Self {
                rows_per_change: 1_024,
                changes_per_transaction: 32,
                transactions_per_iteration: 8,
                payload_bytes: 256,
                sample_size: 15,
                warm_up_time: Duration::from_secs(3),
                measurement_time: Duration::from_secs(5),
                max_retained_bytes: 512 * 1_024 * 1_024,
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

    fn capacity(&self) -> NonZeroU64 {
        NonZeroU64::new(self.max_retained_bytes).expect("retained-byte limit is non-zero")
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
        BenchmarkId::new("consume_durable", config.total_changes()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || measure_consume(root, &config))
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
        if let Some(expected) = oracle {
            assert_eq!(measurement.checksum, expected, "benchmark oracle changed");
        } else {
            oracle = Some(measurement.checksum);
        }
        black_box(measurement.checksum);
        elapsed = elapsed
            .checked_add(measurement.elapsed)
            .expect("benchmark elapsed duration fits Duration");
    }
    elapsed
}

fn representative_workload(config: &Config) -> EncodedChanges {
    let workload = heterogeneous_changes_fixture(
        config.total_changes(),
        config.rows_per_change,
        config.payload_bytes,
    );
    assert!(
        retained_bytes(&workload.encoded) <= config.max_retained_bytes,
        "encoded workload exceeds retained-byte budget"
    );
    workload
}

fn measure_append(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let workload = representative_workload(config);
    let sample = SampleStore::new(run, "append");
    let mut store = Store::create(sample.path()).unwrap();
    let log: SubscribedLog<Vec<u8>> = store.create_data("changes").unwrap();
    let writer = log.writer();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        transaction.commit().unwrap();
    }

    let started = Instant::now();
    for batch in workload.encoded.chunks(config.changes_per_transaction) {
        let transaction = transactions.begin();
        for encoded in batch {
            assert!(
                writer
                    .try_append(encoded, config.capacity(), transaction.access())
                    .unwrap(),
                "configured capacity must admit the representative workload"
            );
        }
        transaction.commit().unwrap();
    }
    let elapsed = started.elapsed();
    drop(transactions);

    validate_appended(sample.path(), &workload.encoded);
    ScenarioMeasurement {
        elapsed,
        checksum: workload.order_checksum(),
    }
}

fn measure_consume(run: &RunRoot, config: &Config) -> ScenarioMeasurement {
    let workload = representative_workload(config);
    let sample = SampleStore::new(run, "consume");
    seed_log(sample.path(), &workload.encoded, config.capacity());
    let expected_checksum = changes_checksum(&workload.changes);

    let store = Store::open(sample.path()).unwrap();
    let log: SubscribedLog<Vec<u8>> = store.open_data("changes").unwrap();
    let subscription = log.subscription(0);
    let (mut writes, reads) = store.into_transactions().split();
    {
        let snapshot = reads.begin();
        log.validate(NonZeroU64::MIN, snapshot.access()).unwrap();
    }

    let started = Instant::now();
    let mut checksum = 0_u64;
    for expected_offset in 0..u64::try_from(workload.encoded.len()).unwrap() {
        let (offset, encoded) = {
            let snapshot = reads.begin();
            subscription.peek(snapshot.access()).unwrap().unwrap()
        };
        assert_eq!(offset, expected_offset);
        let change = decode_entry(&encoded);
        checksum = mix(checksum, change_checksum(&change));
        black_box(&change);

        let transaction = writes.begin();
        subscription
            .acknowledge(offset, transaction.access())
            .unwrap();
        transaction.commit().unwrap();
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_checksum);
    drop((reads, writes));

    validate_consumed(sample.path(), workload.encoded.len());
    ScenarioMeasurement { elapsed, checksum }
}

fn seed_log(path: &Path, encoded: &[Vec<u8>], capacity: NonZeroU64) {
    let mut store = Store::create(path).unwrap();
    let log: SubscribedLog<Vec<u8>> = store.create_data("changes").unwrap();
    let writer = log.writer();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    log.initialize(NonZeroU64::MIN, transaction.access())
        .unwrap();
    for entry in encoded {
        assert!(
            writer
                .try_append(entry, capacity, transaction.access())
                .unwrap()
        );
    }
    transaction.commit().unwrap();
}

fn validate_appended(path: &Path, expected: &[Vec<u8>]) {
    let store = Store::open(path).unwrap();
    let log: SubscribedLog<Vec<u8>> = store.open_data("changes").unwrap();
    let writer = log.writer();
    let subscription = log.subscription(0);
    let snapshot = store.read_transaction();
    log.validate(NonZeroU64::MIN, snapshot.access()).unwrap();
    let status = writer.status(snapshot.access()).unwrap();
    assert_eq!(status.head, 0);
    assert_eq!(status.tail, u64::try_from(expected.len()).unwrap());
    assert_eq!(status.retained_bytes, retained_bytes(expected));
    let (offset, entry) = subscription.peek(snapshot.access()).unwrap().unwrap();
    assert_eq!(offset, 0);
    assert_eq!(entry, expected[0]);
}

fn validate_consumed(path: &Path, expected_entries: usize) {
    let store = Store::open(path).unwrap();
    let log: SubscribedLog<Vec<u8>> = store.open_data("changes").unwrap();
    let writer = log.writer();
    let subscription = log.subscription(0);
    let snapshot = store.read_transaction();
    log.validate(NonZeroU64::MIN, snapshot.access()).unwrap();
    assert!(subscription.peek(snapshot.access()).unwrap().is_none());
    let status = writer.status(snapshot.access()).unwrap();
    let tail = u64::try_from(expected_entries).unwrap();
    assert_eq!(status.head, tail);
    assert_eq!(status.tail, tail);
    assert_eq!(status.retained_bytes, 0);
}

fn retained_bytes(encoded: &[Vec<u8>]) -> u64 {
    encoded.iter().fold(0_u64, |total, entry| {
        total
            .checked_add(8)
            .and_then(|bytes| bytes.checked_add(u64::try_from(entry.len()).unwrap()))
            .expect("fixture retained-byte charge fits u64")
    })
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
            "fixture": "heterogeneous_changes",
            "rows_per_change": config.rows_per_change,
            "changes_per_transaction": config.changes_per_transaction,
            "transactions_per_iteration": config.transactions_per_iteration,
            "changes_per_iteration": config.total_changes(),
            "payload_bytes": config.payload_bytes,
            "sample_size": config.sample_size,
            "warm_up_time_ns": nanos(config.warm_up_time),
            "measurement_time_ns": nanos(config.measurement_time),
            "max_retained_bytes": config.max_retained_bytes,
            "store": {
                "engine": "RocksDB",
                "collection": "SubscribedLog<Vec<u8>>",
                "subscribers": 1,
                "write_mode": "WAL enabled, sync=true"
            },
            "scenarios": ["append_durable", "consume_durable"],
            "timing_scope": {
                "append_durable": "transaction begin, SubscribedLog append, and durable commit",
                "consume_durable": "per entry read snapshot, subscription peek, full Change decode, exact-offset acknowledge, and durable commit"
            },
            "fixture_and_validation": "outside_timing",
            "execution": "single_thread"
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
