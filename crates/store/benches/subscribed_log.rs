//! Large-entry observation, durable consumption, and bounded fan-out retention churn.

use std::{
    hint::black_box,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use criterion::{BenchmarkGroup, BenchmarkId, Criterion, Throughput, measurement::WallTime};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{ReadTransactions, Store, SubscribedLog, Transactions};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "subscribed_log";
const SUBSCRIBERS: u64 = 3;
const SEED: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Copy)]
struct Config {
    large_bytes: usize,
    churn_bytes: usize,
    burst: u64,
    warmup: Duration,
    measurement: Duration,
}

impl Config {
    const fn for_profile(profile: PerformanceProfile) -> Self {
        match profile {
            PerformanceProfile::Smoke => Self {
                large_bytes: 1_024 * 1_024,
                churn_bytes: 4_096,
                burst: 4,
                warmup: Duration::from_millis(20),
                measurement: Duration::from_millis(50),
            },
            PerformanceProfile::Reference => Self {
                large_bytes: 64 * 1_024 * 1_024,
                churn_bytes: 64 * 1_024,
                burst: 64,
                warmup: Duration::from_secs(3),
                measurement: Duration::from_secs(5),
            },
        }
    }
}

struct Fixture {
    transactions: Transactions,
    reads: ReadTransactions,
    log: SubscribedLog<Vec<u8>>,
    root: TempDir,
}

impl Fixture {
    fn empty(root: &RunRoot, subscribers: u64) -> Self {
        let sample = root.sample(BENCHMARK);
        let mut store = Store::create(sample.path().join("store")).expect("create log store");
        let log = store
            .create_data::<SubscribedLog<Vec<u8>>>("log")
            .expect("create log");
        let (mut transactions, reads) = store.into_transactions().split();
        {
            let transaction = transactions.begin();
            log.initialize(NonZeroU64::new(subscribers).unwrap(), transaction.access())
                .expect("initialize subscriptions");
            transaction.commit().expect("commit initialization");
        }
        Self {
            transactions,
            reads,
            log,
            root: sample,
        }
    }

    fn reopen(self, subscribers: u64) -> Self {
        let Self {
            transactions,
            reads,
            log: _,
            root,
        } = self;
        drop(transactions);
        drop(reads);
        let store = Store::open(root.path().join("store")).expect("reopen log store");
        let log = store
            .open_data::<SubscribedLog<Vec<u8>>>("log")
            .expect("open log");
        {
            let snapshot = store.read_transaction();
            log.validate(NonZeroU64::new(subscribers).unwrap(), snapshot.access())
                .expect("validate reopened log");
        }
        let (transactions, reads) = store.into_transactions().split();
        Self {
            transactions,
            reads,
            log,
            root,
        }
    }

    fn append(&mut self, payload: &Vec<u8>) -> bool {
        let transaction = self.transactions.begin();
        let accepted = self
            .log
            .writer()
            .try_append(payload, NonZeroU64::MAX, transaction.access())
            .expect("append log entry");
        transaction.commit().expect("commit append");
        accepted
    }

    fn acknowledge(&mut self, subscriber: u64, offset: u64) {
        let transaction = self.transactions.begin();
        self.log
            .subscription(subscriber)
            .acknowledge(offset, transaction.access())
            .expect("acknowledge log entry");
        transaction.commit().expect("commit acknowledgement");
    }

    fn verify(&self, positions: &[u64], tail: u64, payload: &[u8]) {
        let snapshot = self.reads.begin();
        let head = *positions.iter().min().unwrap();
        let status = self
            .log
            .writer()
            .status(snapshot.access())
            .expect("read log status");
        assert_eq!((status.head, status.tail), (head, tail));
        assert_eq!(
            status.retained_bytes,
            (tail - head) * (8 + u64::try_from(payload.len()).unwrap())
        );
        for (subscriber, &position) in positions.iter().enumerate() {
            let subscription = self.log.subscription(u64::try_from(subscriber).unwrap());
            let status = subscription
                .status(snapshot.access())
                .expect("read subscription status");
            assert_eq!((status.position, status.tail), (position, tail));
            let next = subscription
                .peek(snapshot.access())
                .expect("peek oracle entry");
            if position == tail {
                assert!(next.is_none());
            } else {
                let (offset, actual) = next.expect("retained oracle entry");
                assert_eq!(offset, position);
                assert_eq!(actual, payload);
            }
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
        .sample_size(10)
        .warm_up_time(config.warmup)
        .measurement_time(config.measurement)
        .without_plots()
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    benchmark(&mut criterion, &root, config);
    criterion.final_summary();
}

fn benchmark(criterion: &mut Criterion, root: &RunRoot, config: Config) {
    let mut group = criterion.benchmark_group(BENCHMARK);
    benchmark_status(&mut group, root, config);
    benchmark_consume(&mut group, root, config);
    benchmark_churn(&mut group, root, config);
    group.finish();
}

fn benchmark_status(group: &mut BenchmarkGroup<'_, WallTime>, root: &RunRoot, config: Config) {
    group.throughput(Throughput::Elements(1));
    for bytes in [1_024, config.large_bytes] {
        let payload = payload(bytes);
        let mut fixture = Fixture::empty(root, 1);
        assert!(fixture.append(&payload));
        fixture.verify(&[0], 1, &payload);
        group.bench_function(BenchmarkId::new("snapshot_status", bytes), |bencher| {
            bencher.iter(|| {
                let snapshot = fixture.reads.begin();
                black_box(
                    fixture
                        .log
                        .writer()
                        .status(snapshot.access())
                        .expect("read status"),
                );
            });
        });
        fixture.verify(&[0], 1, &payload);
    }
}

fn benchmark_consume(group: &mut BenchmarkGroup<'_, WallTime>, root: &RunRoot, config: Config) {
    let large = payload(config.large_bytes);
    group.throughput(Throughput::Bytes(u64::try_from(large.len()).unwrap()));
    group.bench_function(
        BenchmarkId::new("peek_ack_commit", large.len()),
        |bencher| {
            bencher.iter_custom(|iterations| {
                let mut fixture = Fixture::empty(root, 1);
                let mut elapsed = Duration::ZERO;
                for offset in 0..iterations {
                    assert!(fixture.append(&large));
                    let started = Instant::now();
                    let (actual_offset, actual) = {
                        let snapshot = fixture.reads.begin();
                        fixture
                            .log
                            .subscription(0)
                            .peek(snapshot.access())
                            .expect("consume entry")
                            .expect("pending entry")
                    };
                    fixture.acknowledge(0, actual_offset);
                    elapsed += started.elapsed();
                    assert_eq!(actual_offset, offset);
                    assert_eq!(actual, large);
                }
                fixture.verify(&[iterations], iterations, &large);
                fixture = fixture.reopen(1);
                fixture.verify(&[iterations], iterations, &large);
                elapsed
            });
        },
    );
}

fn benchmark_churn(group: &mut BenchmarkGroup<'_, WallTime>, root: &RunRoot, config: Config) {
    let churn = payload(config.churn_bytes);
    group.throughput(Throughput::Elements(config.burst));
    group.bench_function(
        BenchmarkId::new("fanout_append_ack_churn", config.burst),
        |bencher| {
            bencher.iter_custom(|iterations| {
                let mut fixture = Fixture::empty(root, SUBSCRIBERS);
                let mut elapsed = Duration::ZERO;
                for cycle in 0..iterations {
                    let head = cycle * config.burst;
                    let tail = head + config.burst;
                    let started = Instant::now();
                    let mut accepted = 0;
                    for _ in head..tail {
                        accepted += u64::from(fixture.append(&churn));
                    }
                    elapsed += started.elapsed();
                    assert_eq!(accepted, config.burst);
                    fixture.verify(&[head, head, head], tail, &churn);

                    let started = Instant::now();
                    for subscriber in 0..SUBSCRIBERS - 1 {
                        for offset in head..tail {
                            fixture.acknowledge(subscriber, offset);
                        }
                    }
                    elapsed += started.elapsed();
                    fixture.verify(&[tail, tail, head], tail, &churn);
                    // Reopen with a lagging subscriber: retention must survive independently
                    // of runtime handles. Recovery and its oracle are outside timing.
                    fixture = fixture.reopen(SUBSCRIBERS);
                    fixture.verify(&[tail, tail, head], tail, &churn);

                    let started = Instant::now();
                    for offset in head..tail {
                        fixture.acknowledge(SUBSCRIBERS - 1, offset);
                    }
                    elapsed += started.elapsed();
                    fixture.verify(&[tail, tail, tail], tail, &churn);
                }
                fixture = fixture.reopen(SUBSCRIBERS);
                fixture.verify(
                    &[iterations * config.burst; 3],
                    iterations * config.burst,
                    &churn,
                );
                elapsed
            });
        },
    );
}

fn payload(bytes: usize) -> Vec<u8> {
    let mut state = SEED;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, config: Config) {
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "large_payload_bytes": config.large_bytes,
            "churn_payload_bytes": config.churn_bytes,
            "burst_entries": config.burst,
            "fanout": SUBSCRIBERS,
            "seed": SEED,
            "samples": 10,
            "execution": "single_thread",
            "cache": "status warmed; churn reopens after fast subscribers catch up",
            "timing": "status snapshot; consume peek+ACK+sync commit; churn append+ACK+sync commits",
            "outside_timing": "payload, setup, consume replenishment, oracle, reopen",
            "reopen": "each churn burst with backlog retained; end of consume/churn sample",
            "store": {"engine": "RocksDB", "write_mode": "WAL enabled, sync=true"}
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("serialize log benchmark context"),
    )
    .expect("write log benchmark context");
}
