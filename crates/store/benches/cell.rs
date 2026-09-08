//! Hot-access and durable-update scenarios for `Cell`.

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Cell, Store, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "cell";
const DEFAULT_READS: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 10;

struct Fixture {
    transactions: Transactions,
    cell: Cell<u64>,
    _root: TempDir,
}

#[derive(Clone, Copy)]
struct Config {
    reads: usize,
    commits: usize,
    samples: usize,
}

impl Fixture {
    fn populated(root: &RunRoot) -> Self {
        let sample = root.sample("cell");
        let mut store =
            Store::create(sample.path().join("store")).expect("create cell benchmark store");
        let cell = store
            .create_data::<Cell<u64>>("cell")
            .expect("create benchmark cell");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            cell,
            _root: sample,
        };
        let transaction = fixture.transactions.begin();
        fixture
            .cell
            .access(transaction.access())
            .expect("access seeded cell")
            .set(&0)
            .expect("seed benchmark cell");
        transaction.commit().expect("commit benchmark cell seed");
        fixture
    }
}

impl Config {
    const fn for_profile(profile: PerformanceProfile) -> Self {
        match profile {
            PerformanceProfile::Smoke => Self {
                reads: 1,
                commits: 1,
                samples: 10,
            },
            PerformanceProfile::Reference => Self {
                reads: DEFAULT_READS,
                commits: DEFAULT_COMMITS,
                samples: DEFAULT_SAMPLES,
            },
        }
    }
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|argument| argument == "--bench") {
        require_release_build(BENCHMARK);
    }
    let config = Config::for_profile(profile);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    write_context(&root, profile, config);
    let mut criterion = Criterion::default()
        .sample_size(config.samples)
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    benchmark(&mut criterion, &root, config);
    criterion.final_summary();
}

fn benchmark(criterion: &mut Criterion, root: &RunRoot, config: Config) {
    let mut group = criterion.benchmark_group(BENCHMARK);
    group.throughput(Throughput::Elements(
        u64::try_from(config.reads).expect("read count fits u64"),
    ));
    let mut fixture = Fixture::populated(root);
    group.bench_function(
        BenchmarkId::new("hot_get_one_tx", config.reads),
        |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    elapsed += measure_get(&mut fixture, config.reads);
                }
                elapsed
            });
        },
    );

    group.throughput(Throughput::Elements(
        u64::try_from(config.commits).expect("commit count fits u64"),
    ));
    group.bench_function(
        BenchmarkId::new("read_update_commit", config.commits),
        |bencher| {
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    elapsed += measure_updates(&mut Fixture::populated(root), config.commits);
                }
                elapsed
            });
        },
    );
    group.finish();
}

fn measure_get(fixture: &mut Fixture, operations: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture.transactions.begin();
    let cell = fixture
        .cell
        .access(transaction.access())
        .expect("access benchmark cell");
    let mut checksum = 0_u64;
    for _ in 0..operations {
        checksum = checksum.wrapping_add(
            cell.get()
                .expect("read benchmark cell")
                .expect("seeded benchmark cell"),
        );
    }
    black_box(checksum);
    transaction.commit().expect("finish cell read transaction");
    let elapsed = started.elapsed();
    assert_eq!(checksum, 0, "seeded Cell reads must preserve the oracle");
    elapsed
}

fn measure_updates(fixture: &mut Fixture, commits: usize) -> Duration {
    let mut expected = None;
    let started = std::time::Instant::now();
    for _ in 0..commits {
        let transaction = fixture.transactions.begin();
        let mut cell = fixture
            .cell
            .access(transaction.access())
            .expect("access benchmark cell");
        let value = cell
            .get()
            .expect("read benchmark cell")
            .expect("seeded benchmark cell");
        let next = value.wrapping_add(1);
        cell.set(&next).expect("update benchmark cell");
        transaction.commit().expect("commit benchmark cell update");
        expected = Some(next);
    }
    let elapsed = started.elapsed();

    let transaction = fixture.transactions.begin();
    let actual = fixture
        .cell
        .access(transaction.access())
        .expect("access benchmark cell for validation")
        .get()
        .expect("read benchmark cell for validation");
    assert_eq!(actual, expected);
    transaction
        .commit()
        .expect("finish cell validation transaction");
    elapsed
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, config: Config) {
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "reads": config.reads,
            "commits": config.commits,
            "samples": config.samples,
            "execution": "single_thread",
            "cache": "warm",
            "validation": "outside_timing",
            "store": {
                "engine": "RocksDB",
                "write_mode": "WAL enabled, sync=true"
            },
        },
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("serialize Cell performance context"),
    )
    .expect("write Cell performance context");
}
