//! Hot-access and durable-update scenarios for `Cell`.

use std::{hint::black_box, time::Duration};

use dogpaddle_store::{Cell, Store, Transactions};
use tempfile::TempDir;

mod support;

use support::{
    SampleWork, average_duration, emit_configuration, emit_samples, emit_summary, format_duration,
    initialize, sample_dir, setting,
};

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
        let root = sample_dir("cell");
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
    initialize("store_cell");

    let reads = setting("DOGPADDLE_BENCH_CELL_READS", DEFAULT_READS);
    let commits = setting("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS);
    let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
    emit_configuration(
        "store_cell",
        &format!("\"reads\":{reads},\"commits\":{commits},\"samples\":{samples}"),
    );

    println!("DogPaddle Cell benchmark");
    println!("reads={reads} commits={commits} samples={samples}");
    println!("sync=durable execution=single-thread cache=warm validation=outside-timing");
    println!();
    println!("=== Cell<T> ===");
    println!("one shared value; hot access and one durable state update per transaction");
    println!(
        "{:<30} {:>12} {:>12} {:>12} {:>12} {:>12} {:>14}",
        "workload", "operations", "min", "median", "max", "median/op", "median ops/s"
    );

    let mut fixture = Fixture::populated();
    report("hot get, one tx", reads, 1, 8 * reads, samples, || {
        measure_get(&mut fixture, reads)
    });

    report(
        "read + update + commit",
        commits,
        commits,
        16 * commits,
        samples,
        || {
            let mut fixture = Fixture::populated();
            measure_updates(&mut fixture, commits)
        },
    );
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
    let elapsed = started.elapsed();
    assert_eq!(checksum, 0, "seeded Cell reads must preserve the oracle");
    elapsed
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
    transactions: usize,
    logical_bytes: usize,
    samples: usize,
    mut measure: impl FnMut() -> Duration,
) {
    measure();
    let mut durations = (0..samples).map(|_| measure()).collect::<Vec<_>>();
    let work = SampleWork {
        operations,
        transactions,
        logical_bytes,
    };
    emit_samples("store_cell", workload, "Cell", &durations, work);
    emit_summary("store_cell", workload, "Cell", &durations, work);
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let rate = operations as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_operation = average_duration(median, operations);
    println!(
        "{workload:<30} {operations:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        format_duration(min),
        format_duration(median),
        format_duration(max),
    );
}
