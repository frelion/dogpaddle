//! Hot-access and durable-update scenarios for `Cell`.

use std::{hint::black_box, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{BenchmarkProfile, CaseSpec, Fields, Measurement, Plan, Run};
use dogpaddle_store::{Cell, Store, Transactions};
use tempfile::TempDir;

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
    fn populated(run: &Run) -> Self {
        let root = run.sample("cell");
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
    let profile = BenchmarkProfile::from_environment();
    let Config {
        reads,
        commits,
        samples,
    } = Config::for_profile(profile);
    let mut fields = Fields::new();
    for (name, value) in [("reads", reads), ("commits", commits), ("samples", samples)] {
        fields.insert(name, value);
    }
    let mut plan = Plan::new(
        profile,
        fields
            .with("execution", "single_thread")
            .with("cache", "warm")
            .with("validation", "outside_timing")
            .with("mdbx_sync_mode", "durable"),
    );
    let get = plan.case(case(
        "hot get, one tx",
        SampleWork {
            operations: reads,
            transactions: 1,
            logical_bytes: 8 * reads,
        },
        samples,
    ));
    let update = plan.case(case(
        "read + update + commit",
        SampleWork {
            operations: commits,
            transactions: commits,
            logical_bytes: 16 * commits,
        },
        samples,
    ));
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }

    let mut fixture = Fixture::populated(&run);
    measure_get(&mut fixture, reads);
    run.samples(get, |_| Measurement::new(measure_get(&mut fixture, reads)));

    measure_updates(&mut Fixture::populated(&run), commits);
    run.samples(update, |run| {
        Measurement::new(measure_updates(&mut Fixture::populated(run), commits))
    });
    run.finish(|| {});
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

fn case(workload: &str, work: SampleWork, samples: usize) -> CaseSpec {
    CaseSpec::new(
        workload,
        NonZeroUsize::new(samples).unwrap(),
        Fields::new()
            .with("variant", "Cell")
            .with("operations", work.operations)
            .with("transactions", work.transactions)
            .with("logical_bytes", work.logical_bytes),
    )
}
