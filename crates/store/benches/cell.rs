//! Hot-access and durable-update scenarios for `Cell`.

use std::{hint::black_box, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, ConfigurationRecord, DurationSummary, Fields, SampleRecord, SummaryRecord,
};
use dogpaddle_store::{Cell, Store, Transactions};
use tempfile::TempDir;

mod support;

use support::{BenchRoot, average_duration, complete, format_duration, initialize, write_record};

const BENCHMARK: &str = "cell";
const DEFAULT_READS: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;

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

#[derive(Clone, Copy)]
struct SampleWork {
    operations: usize,
    transactions: usize,
    logical_bytes: usize,
}

impl Fixture {
    fn populated(bench_root: &BenchRoot) -> Self {
        let root = bench_root.sample("cell");
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

impl Config {
    const fn for_profile(profile: BenchmarkProfile) -> Self {
        match profile {
            BenchmarkProfile::Smoke => Self {
                reads: 1,
                commits: 1,
                samples: 1,
            },
            BenchmarkProfile::Reference => Self {
                reads: DEFAULT_READS,
                commits: DEFAULT_COMMITS,
                samples: DEFAULT_SAMPLES,
            },
        }
    }
}

fn main() {
    let bench_root = initialize(BENCHMARK);
    let Config {
        reads,
        commits,
        samples,
    } = Config::for_profile(bench_root.profile());
    let mut fields = Fields::new();
    for (name, value) in [("reads", reads), ("commits", commits), ("samples", samples)] {
        fields
            .insert(name, value)
            .expect("construct Cell configuration fields");
    }
    let expected_data_records = NonZeroUsize::new(2 * (samples + 1)).unwrap();
    let record = ConfigurationRecord::new(BENCHMARK, expected_data_records, fields)
        .expect("construct Cell configuration record");
    write_record(&record);

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

    let mut fixture = Fixture::populated(&bench_root);
    report(
        "hot get, one tx",
        SampleWork {
            operations: reads,
            transactions: 1,
            logical_bytes: 8 * reads,
        },
        samples,
        || measure_get(&mut fixture, reads),
    );

    report(
        "read + update + commit",
        SampleWork {
            operations: commits,
            transactions: commits,
            logical_bytes: 16 * commits,
        },
        samples,
        || {
            let mut fixture = Fixture::populated(&bench_root);
            measure_updates(&mut fixture, commits)
        },
    );
    complete(BENCHMARK);
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

fn report(workload: &str, work: SampleWork, samples: usize, mut measure: impl FnMut() -> Duration) {
    measure();
    let durations = (0..samples).map(|_| measure()).collect::<Vec<_>>();
    for (sample, elapsed) in durations.iter().copied().enumerate() {
        let record = SampleRecord::new(
            BENCHMARK,
            workload,
            sample,
            elapsed,
            measurement_fields(work),
        )
        .expect("construct Cell sample record");
        write_record(&record);
    }
    let summary = DurationSummary::from_samples(&durations).expect("summarize Cell measurements");
    let record = SummaryRecord::new(BENCHMARK, workload, summary, measurement_fields(work))
        .expect("construct Cell summary record");
    write_record(&record);
    let rate = work.operations as u128 * 1_000_000_000 / summary.median().as_nanos();
    let median_per_operation = average_duration(summary.median(), work.operations);
    println!(
        "{workload:<30} {:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        work.operations,
        format_duration(summary.min()),
        format_duration(summary.median()),
        format_duration(summary.max()),
    );
}

fn measurement_fields(work: SampleWork) -> Fields {
    let mut fields = Fields::new();
    fields
        .insert("variant", "Cell")
        .expect("construct Cell variant field");
    for (name, value) in [
        ("operations", work.operations),
        ("transactions", work.transactions),
        ("logical_bytes", work.logical_bytes),
    ] {
        fields
            .insert(name, value)
            .expect("construct Cell work fields");
    }
    fields
}
