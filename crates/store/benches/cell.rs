//! Hot-access and durable-update scenarios for `Cell`.

use std::{hint::black_box, time::Duration};

use dogpaddle_store::{Cell, Store, Transactions};
use tempfile::TempDir;

const DEFAULT_READS: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;

struct Fixture {
    transactions: Transactions,
    cell: Cell<u64>,
    _root: TempDir,
}

impl Fixture {
    fn populated() -> Self {
        let root = tempfile::tempdir().expect("temporary cell benchmark directory");
        let mut store =
            Store::create(root.path().join("store")).expect("create cell benchmark store");
        let cell = store
            .create_data::<Cell<u64>>("cell")
            .expect("create benchmark cell");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            cell,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin cell seed transaction");
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

fn main() {
    if cfg!(debug_assertions) {
        return;
    }

    let reads = setting("DOGPADDLE_BENCH_CELL_READS", DEFAULT_READS);
    let commits = setting("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS);
    let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
    assert!(reads > 0 && commits > 0 && samples > 0);

    println!("DogPaddle Cell benchmark");
    println!("reads={reads} commits={commits} samples={samples}");
    println!("sync=durable execution=single-thread cache=warm validation=outside-timing");
    println!(
        "platform={}-{} temp_root={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::temp_dir().display()
    );
    println!();
    println!("=== Cell<T> ===");
    println!("one shared value; hot access and one durable state update per transaction");
    println!(
        "{:<30} {:>12} {:>12} {:>12} {:>12} {:>12} {:>14}",
        "workload", "operations", "min", "median", "max", "median/op", "median ops/s"
    );

    let mut fixture = Fixture::populated();
    report("hot get, one tx", reads, samples, || {
        measure_get(&mut fixture, reads)
    });

    let mut fixture = Fixture::populated();
    report("read + update + commit", commits, samples, || {
        measure_updates(&mut fixture, commits)
    });
}

fn measure_get(fixture: &mut Fixture, operations: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin cell read transaction");
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
    started.elapsed()
}

fn measure_updates(fixture: &mut Fixture, commits: usize) -> Duration {
    let mut expected = None;
    let started = std::time::Instant::now();
    for _ in 0..commits {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin cell update transaction");
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

    let transaction = fixture
        .transactions
        .begin()
        .expect("begin cell validation transaction");
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

fn report(
    workload: &str,
    operations: usize,
    samples: usize,
    mut measure: impl FnMut() -> Duration,
) {
    measure();
    let mut durations = (0..samples).map(|_| measure()).collect::<Vec<_>>();
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let rate = operations as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_operation = average_duration(median, operations);
    println!(
        "{workload:<30} {operations:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        duration(min),
        duration(median),
        duration(max),
    );
}

fn average_duration(total: Duration, operations: usize) -> String {
    let nanos = total.as_nanos() / operations as u128;
    duration(Duration::from_nanos(
        u64::try_from(nanos).expect("average benchmark duration fits in u64 nanoseconds"),
    ))
}

fn duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("benchmark setting must be an integer")
    })
}
