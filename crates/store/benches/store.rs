use std::{hint::black_box, time::Duration};

use dogpaddle_store::{DataPlacement, OrderedMap, ScanDirection, ScanLimit, Store, Transactions};
use tempfile::TempDir;

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_BACKGROUND_NAMESPACES: usize = 8;
const VALUE_BYTES: usize = 64;
const RANDOM_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

struct Fixture {
    transactions: Transactions,
    map: OrderedMap<u64, Vec<u8>>,
    _root: TempDir,
}

impl Fixture {
    fn empty(placement: DataPlacement) -> Self {
        let root = tempfile::tempdir().expect("temporary benchmark directory");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = OrderedMap::new(
            store
                .create_data("map", placement)
                .expect("create benchmark map"),
        );
        Self {
            transactions: store.into_transactions(),
            map,
            _root: root,
        }
    }

    fn populated(placement: DataPlacement, entries: usize) -> Self {
        let mut fixture = Self::empty(placement);
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin seed transaction");
        {
            let mut map = fixture.map.access(&transaction).expect("access seed map");
            let value = vec![0x5a; VALUE_BYTES];
            for key in 0..entries {
                map.put(&(key as u64), &value).expect("seed benchmark map");
            }
        }
        transaction.commit().expect("commit benchmark seed");
        fixture
    }

    fn populated_with_shared_background(
        placement: DataPlacement,
        entries: usize,
        background_namespaces: usize,
    ) -> Self {
        let root = tempfile::tempdir().expect("temporary benchmark directory");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = OrderedMap::new(
            store
                .create_data("target", placement)
                .expect("create target map"),
        );
        let backgrounds = (0..background_namespaces)
            .map(|index| {
                OrderedMap::<u64, Vec<u8>>::new(
                    store
                        .create_data(&format!("background-{index}"), DataPlacement::Shared)
                        .expect("create background map"),
                )
            })
            .collect::<Vec<_>>();
        let mut fixture = Self {
            transactions: store.into_transactions(),
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin mixed seed transaction");
        let value = vec![0x5a; VALUE_BYTES];
        {
            let mut target = fixture.map.access(&transaction).expect("access target map");
            for key in 0..entries {
                target.put(&(key as u64), &value).expect("seed target map");
            }
        }
        let entries_per_background = entries.div_ceil(background_namespaces);
        for background in &backgrounds {
            let mut background = background
                .access(&transaction)
                .expect("access background map");
            for key in 0..entries_per_background {
                background
                    .put(&(key as u64), &value)
                    .expect("seed background map");
            }
        }
        transaction.commit().expect("commit mixed benchmark seed");
        fixture
    }
}

fn main() {
    if cfg!(debug_assertions) {
        return;
    }

    let entries = setting("DOGPADDLE_BENCH_ENTRIES", DEFAULT_ENTRIES);
    let commits = setting("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS);
    let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
    let background_namespaces = setting(
        "DOGPADDLE_BENCH_BACKGROUND_NAMESPACES",
        DEFAULT_BACKGROUND_NAMESPACES,
    );
    assert!(entries > 0 && commits > 0 && samples > 0 && background_namespaces > 0);

    println!("DogPaddle Store benchmark");
    println!(
        "entries={entries} value_bytes={VALUE_BYTES} commits={commits} samples={samples} background_namespaces={background_namespaces}"
    );
    println!(
        "sync=durable point_scan_cache=warm random_seed={RANDOM_SEED:#x} root=temporary-directory"
    );
    println!();
    println!(
        "{:<28} {:<10} {:>12} {:>12} {:>12} {:>12} {:>14}",
        "workload", "placement", "operations", "min", "median", "max", "median ops/s"
    );

    report_pair(
        "bulk put + commit",
        entries,
        samples,
        || measure_bulk_put(DataPlacement::Shared, entries),
        || measure_bulk_put(DataPlacement::Dedicated, entries),
    );

    let mut shared = Fixture::populated(DataPlacement::Shared, entries);
    let mut dedicated = Fixture::populated(DataPlacement::Dedicated, entries);
    report_pair(
        "hot point get",
        entries,
        samples,
        || measure_point_get(&mut shared, entries),
        || measure_point_get(&mut dedicated, entries),
    );
    report_pair(
        "hot ordered scan",
        entries,
        samples,
        || measure_scan(&mut shared, entries),
        || measure_scan(&mut dedicated, entries),
    );

    let mut shared = Fixture::empty(DataPlacement::Shared);
    let mut dedicated = Fixture::empty(DataPlacement::Dedicated);
    report_pair(
        "durable overwrite commit",
        commits,
        samples,
        || measure_single_put_commits(&mut shared, commits),
        || measure_single_put_commits(&mut dedicated, commits),
    );

    let mut shared = Fixture::populated_with_shared_background(
        DataPlacement::Shared,
        entries,
        background_namespaces,
    );
    let mut dedicated = Fixture::populated_with_shared_background(
        DataPlacement::Dedicated,
        entries,
        background_namespaces,
    );
    report_pair(
        "mixed hot point get",
        entries,
        samples,
        || measure_point_get(&mut shared, entries),
        || measure_point_get(&mut dedicated, entries),
    );
    report_pair(
        "mixed hot ordered scan",
        entries,
        samples,
        || measure_scan(&mut shared, entries),
        || measure_scan(&mut dedicated, entries),
    );
}

fn measure_bulk_put(placement: DataPlacement, entries: usize) -> Duration {
    let mut fixture = Fixture::empty(placement);
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin write transaction");
    {
        let mut map = fixture.map.access(&transaction).expect("access write map");
        for key in 0..entries {
            map.put(&(key as u64), &value)
                .expect("write benchmark item");
        }
    }
    transaction.commit().expect("commit benchmark writes");
    started.elapsed()
}

fn measure_point_get(fixture: &mut Fixture, entries: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin read transaction");
    let map = fixture.map.access(&transaction).expect("access read map");
    let mut state = RANDOM_SEED;
    let mut checksum = 0_usize;
    for _ in 0..entries {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = state % entries as u64;
        let value = map.get(&key).expect("read benchmark item").unwrap();
        checksum = checksum.wrapping_add(usize::from(value[0]));
    }
    black_box(checksum);
    transaction.commit().expect("finish read transaction");
    started.elapsed()
}

fn measure_scan(fixture: &mut Fixture, entries: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin scan transaction");
    let map = fixture.map.access(&transaction).expect("access scan map");
    let limit = ScanLimit::new(1_024, 4 * 1_024 * 1_024).unwrap();
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    loop {
        let batch = map
            .scan(.., ScanDirection::Ascending, continuation.as_ref(), limit)
            .expect("scan benchmark page");
        count += batch.items.len();
        checksum = batch.items.iter().fold(checksum, |checksum, (_, value)| {
            checksum.wrapping_add(usize::from(value[0]))
        });
        if let Some(next) = batch.continuation {
            continuation = Some(next);
        } else {
            break;
        }
    }
    assert_eq!(count, entries);
    black_box(checksum);
    transaction.commit().expect("finish scan transaction");
    started.elapsed()
}

fn measure_single_put_commits(fixture: &mut Fixture, commits: usize) -> Duration {
    let started = std::time::Instant::now();
    for value in 0..commits {
        let mut encoded = vec![0x5a; VALUE_BYTES];
        encoded[..std::mem::size_of::<u64>()].copy_from_slice(&(value as u64).to_be_bytes());
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin single-put transaction");
        fixture
            .map
            .access(&transaction)
            .expect("access single-put map")
            .put(&0, &encoded)
            .expect("write single-put value");
        transaction.commit().expect("commit single-put transaction");
    }
    started.elapsed()
}

fn report_pair(
    workload: &str,
    operations: usize,
    samples: usize,
    mut shared: impl FnMut() -> Duration,
    mut dedicated: impl FnMut() -> Duration,
) {
    shared();
    dedicated();
    let mut shared_durations = Vec::with_capacity(samples);
    let mut dedicated_durations = Vec::with_capacity(samples);
    for sample in 0..samples {
        if sample % 2 == 0 {
            shared_durations.push(shared());
            dedicated_durations.push(dedicated());
        } else {
            dedicated_durations.push(dedicated());
            shared_durations.push(shared());
        }
    }
    report(
        workload,
        DataPlacement::Shared,
        operations,
        shared_durations,
    );
    report(
        workload,
        DataPlacement::Dedicated,
        operations,
        dedicated_durations,
    );
}

fn report(
    workload: &str,
    placement: DataPlacement,
    operations: usize,
    mut durations: Vec<Duration>,
) {
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let rate = operations as u128 * 1_000_000_000 / median.as_nanos();
    println!(
        "{workload:<28} {placement:<10?} {operations:>12} {:>12} {:>12} {:>12} {rate:>14}",
        duration(min),
        duration(median),
        duration(max),
    );
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
