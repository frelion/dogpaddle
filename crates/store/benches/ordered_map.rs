//! Scenario benchmarks for both physical forms of `OrderedMap`.

use std::{hint::black_box, time::Duration};

use dogpaddle_store::{
    Cell, Large, OrderedMap, ScanDirection, ScanLimit, Small, Store, StoreData, Transactions,
};
use tempfile::TempDir;

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_BACKGROUND_NAMESPACES: usize = 8;
const DEFAULT_SCAN_ITEMS: usize = 1_024;
const VALUE_BYTES: usize = 64;
const STAGE_KEYS: usize = 1_024;
const RANDOM_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

type ByteMap<SIZE> = OrderedMap<Vec<u8>, Vec<u8>, SIZE>;
type TypedMap<SIZE> = OrderedMap<u64, Vec<u8>, SIZE>;

struct Fixture<SIZE> {
    transactions: Transactions,
    bytes: ByteMap<SIZE>,
    map: TypedMap<SIZE>,
    _root: TempDir,
}

struct StageFixture<SIZE> {
    transactions: Transactions,
    cursor: Cell<u64>,
    map: TypedMap<SIZE>,
    _root: TempDir,
}

#[derive(Clone, Copy)]
struct ScanWorkload {
    name: &'static str,
    direction: ScanDirection,
    kind: ScanKind,
}

#[derive(Clone, Copy)]
enum ScanKind {
    ByteMap,
    TypedMap,
}

impl<SIZE> Fixture<SIZE>
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    fn empty() -> Self {
        let root = tempfile::tempdir().expect("temporary benchmark directory");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = store
            .create_data::<TypedMap<SIZE>>("map")
            .expect("create benchmark map");
        let bytes = store
            .create_data::<ByteMap<SIZE>>("bytes")
            .expect("create benchmark byte map");
        Self {
            transactions: store.into_transactions(),
            bytes,
            map,
            _root: root,
        }
    }

    fn populated_typed(entries: usize) -> Self {
        let mut fixture = Self::empty();
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin seed transaction");
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access seed map");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..entries {
            map.put(&(key as u64), &value).expect("seed benchmark map");
        }
        transaction.commit().expect("commit benchmark seed");
        fixture
    }

    fn populated_bytes(entries: usize) -> Self {
        let mut fixture = Self::empty();
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin byte map seed transaction");
        let mut bytes = fixture
            .bytes
            .access(transaction.access())
            .expect("access seed byte map");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..entries {
            bytes
                .put(&(key as u64).to_be_bytes().to_vec(), &value)
                .expect("seed benchmark byte map");
        }
        transaction
            .commit()
            .expect("commit benchmark byte map seed");
        fixture
    }

    fn populated_with_small_background(entries: usize, background_namespaces: usize) -> Self {
        let root = tempfile::tempdir().expect("temporary benchmark directory");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = store
            .create_data::<TypedMap<SIZE>>("target")
            .expect("create target map");
        let bytes = store
            .create_data::<ByteMap<SIZE>>("target-bytes")
            .expect("create target byte map");
        let backgrounds = (0..background_namespaces)
            .map(|index| {
                store
                    .create_data::<OrderedMap<u64, Vec<u8>, Small>>(&format!("background-{index}"))
                    .expect("create background map")
            })
            .collect::<Vec<_>>();
        let mut fixture = Self {
            transactions: store.into_transactions(),
            bytes,
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin mixed seed transaction");
        let value = vec![0x5a; VALUE_BYTES];
        {
            let mut target = fixture
                .map
                .access(transaction.access())
                .expect("access target map");
            for key in 0..entries {
                target.put(&(key as u64), &value).expect("seed target map");
            }
        }
        let entries_per_background = entries / background_namespaces;
        let extra_entries = entries % background_namespaces;
        for (index, background) in backgrounds.iter().enumerate() {
            let mut background = background
                .access(transaction.access())
                .expect("access background map");
            let background_entries = entries_per_background + usize::from(index < extra_entries);
            for key in 0..background_entries {
                background
                    .put(&(key as u64), &value)
                    .expect("seed background map");
            }
        }
        transaction.commit().expect("commit mixed benchmark seed");
        fixture
    }
}

impl<SIZE> StageFixture<SIZE>
where
    TypedMap<SIZE>: StoreData,
{
    fn populated() -> Self {
        let root = tempfile::tempdir().expect("temporary stage benchmark directory");
        let mut store = Store::create(root.path().join("store")).expect("create stage store");
        let cursor = store
            .create_data::<Cell<u64>>("cursor")
            .expect("create stage cursor");
        let map = store
            .create_data::<TypedMap<SIZE>>("map")
            .expect("create stage map");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            cursor,
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin stage seed transaction");
        fixture
            .cursor
            .access(transaction.access())
            .expect("access stage cursor")
            .set(&0)
            .expect("seed stage cursor");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access stage map");
            let value = vec![0x5a; VALUE_BYTES];
            for key in 0..STAGE_KEYS {
                map.put(&(key as u64), &value).expect("seed stage map");
            }
        }
        transaction.commit().expect("commit stage seed");
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
    let scan_items = setting("DOGPADDLE_BENCH_SCAN_ITEMS", DEFAULT_SCAN_ITEMS);
    assert!(
        entries > 0 && commits > 0 && samples > 0 && background_namespaces > 0 && scan_items > 0
    );

    println!("DogPaddle OrderedMap benchmark");
    println!(
        "entries={entries} value_bytes={VALUE_BYTES} commits={commits} samples={samples} background_namespaces={background_namespaces} scan_items={scan_items}"
    );
    println!(
        "sync=durable execution=single-thread point_scan_cache=warm random_seed={RANDOM_SEED:#x}"
    );
    println!(
        "platform={}-{} temp_root={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::temp_dir().display()
    );

    print_section(
        "OrderedMap<K, V, Small> vs OrderedMap<K, V, Large>",
        "same workloads; Small shares the main table, Large owns a dedicated table",
    );
    print_group("isolated object: bulk write, warm point read, scan, and rollback");
    benchmark_isolated(entries, samples, scan_items);
    print_group(&format!(
        "target map with {background_namespaces} populated Small background namespaces"
    ));
    benchmark_mixed(entries, samples, background_namespaces, scan_items);
    print_group("Stage-shaped atomic batches: map updates plus one Cell cursor");
    benchmark_stage_steps(commits, samples);
    print_group("worst-case commit amortization: one overwrite per durable transaction");
    benchmark_durable_overwrite(commits, samples);
}

fn benchmark_isolated(entries: usize, samples: usize, scan_items: usize) {
    report_pair(
        "byte map bulk put + commit",
        entries,
        samples,
        || measure_byte_map_bulk_put::<Small>(entries),
        || measure_byte_map_bulk_put::<Large>(entries),
    );
    report_pair(
        "bulk put + commit",
        entries,
        samples,
        || measure_bulk_put::<Small>(entries),
        || measure_bulk_put::<Large>(entries),
    );

    let mut small_bytes = Fixture::<Small>::populated_bytes(entries);
    let mut large_bytes = Fixture::<Large>::populated_bytes(entries);
    report_pair(
        "hot byte map point get",
        entries,
        samples,
        || measure_byte_map_point_get(&mut small_bytes, entries),
        || measure_byte_map_point_get(&mut large_bytes, entries),
    );
    report_scan_pair(
        ScanWorkload {
            name: "hot byte map asc scan",
            direction: ScanDirection::Ascending,
            kind: ScanKind::ByteMap,
        },
        &mut small_bytes,
        &mut large_bytes,
        entries,
        samples,
        scan_items,
    );
    report_scan_pair(
        ScanWorkload {
            name: "hot byte map desc scan",
            direction: ScanDirection::Descending,
            kind: ScanKind::ByteMap,
        },
        &mut small_bytes,
        &mut large_bytes,
        entries,
        samples,
        scan_items,
    );

    let mut small = Fixture::<Small>::populated_typed(entries);
    let mut large = Fixture::<Large>::populated_typed(entries);
    report_pair(
        "hot point get",
        entries,
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        ScanWorkload {
            name: "hot ascending scan",
            direction: ScanDirection::Ascending,
            kind: ScanKind::TypedMap,
        },
        &mut small,
        &mut large,
        entries,
        samples,
        scan_items,
    );
    report_scan_pair(
        ScanWorkload {
            name: "hot descending scan",
            direction: ScanDirection::Descending,
            kind: ScanKind::TypedMap,
        },
        &mut small,
        &mut large,
        entries,
        samples,
        scan_items,
    );
    report_pair(
        "hot overwrite + rollback",
        entries,
        samples,
        || measure_hot_overwrite_rollback(&mut small, entries),
        || measure_hot_overwrite_rollback(&mut large, entries),
    );
}

fn benchmark_stage_steps(steps: usize, samples: usize) {
    for operations_per_step in [1, 8, 64] {
        let mut small = StageFixture::<Small>::populated();
        let mut large = StageFixture::<Large>::populated();
        report_pair(
            &format!("stage step x{operations_per_step}"),
            steps,
            samples,
            || measure_stage_steps(&mut small, steps, operations_per_step),
            || measure_stage_steps(&mut large, steps, operations_per_step),
        );
    }
}

fn benchmark_durable_overwrite(commits: usize, samples: usize) {
    let mut small = Fixture::<Small>::empty();
    let mut large = Fixture::<Large>::empty();
    report_pair(
        "durable overwrite commit",
        commits,
        samples,
        || measure_single_put_commits(&mut small, commits),
        || measure_single_put_commits(&mut large, commits),
    );
}

fn benchmark_mixed(
    entries: usize,
    samples: usize,
    background_namespaces: usize,
    scan_items: usize,
) {
    let mut small =
        Fixture::<Small>::populated_with_small_background(entries, background_namespaces);
    let mut large =
        Fixture::<Large>::populated_with_small_background(entries, background_namespaces);
    report_pair(
        "mixed hot point get",
        entries,
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        ScanWorkload {
            name: "mixed hot ascending scan",
            direction: ScanDirection::Ascending,
            kind: ScanKind::TypedMap,
        },
        &mut small,
        &mut large,
        entries,
        samples,
        scan_items,
    );
    report_scan_pair(
        ScanWorkload {
            name: "mixed hot descending scan",
            direction: ScanDirection::Descending,
            kind: ScanKind::TypedMap,
        },
        &mut small,
        &mut large,
        entries,
        samples,
        scan_items,
    );
}

fn report_scan_pair<SmallSize, LargeSize>(
    workload: ScanWorkload,
    small: &mut Fixture<SmallSize>,
    large: &mut Fixture<LargeSize>,
    entries: usize,
    samples: usize,
    scan_items: usize,
) {
    report_pair(
        workload.name,
        entries,
        samples,
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(small, entries, scan_items, workload.direction)
            }
            ScanKind::TypedMap => measure_scan(small, entries, scan_items, workload.direction),
        },
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(large, entries, scan_items, workload.direction)
            }
            ScanKind::TypedMap => measure_scan(large, entries, scan_items, workload.direction),
        },
    );
}

fn measure_byte_map_bulk_put<SIZE>(entries: usize) -> Duration
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    let mut fixture = Fixture::<SIZE>::empty();
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map write transaction");
    {
        let mut bytes = fixture
            .bytes
            .access(transaction.access())
            .expect("access byte map");
        for key in 0..entries {
            bytes
                .put(&(key as u64).to_be_bytes().to_vec(), &value)
                .expect("write byte map benchmark item");
        }
    }
    transaction
        .commit()
        .expect("commit byte map benchmark writes");
    started.elapsed()
}

fn measure_bulk_put<SIZE>(entries: usize) -> Duration
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    let mut fixture = Fixture::<SIZE>::empty();
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin write transaction");
    {
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access write map");
        for key in 0..entries {
            map.put(&(key as u64), &value)
                .expect("write benchmark item");
        }
    }
    transaction.commit().expect("commit benchmark writes");
    started.elapsed()
}

fn measure_point_get<SIZE>(fixture: &mut Fixture<SIZE>, entries: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin read transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access read map");
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

fn measure_byte_map_point_get<SIZE>(fixture: &mut Fixture<SIZE>, entries: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map read transaction");
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map");
    let mut state = RANDOM_SEED;
    let mut checksum = 0_usize;
    for _ in 0..entries {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = (state % entries as u64).to_be_bytes().to_vec();
        let value = bytes.get(&key).expect("read byte map item").unwrap();
        checksum = checksum.wrapping_add(usize::from(value[0]));
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish byte map read transaction");
    started.elapsed()
}

fn measure_byte_map_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map scan transaction");
    let limit = ScanLimit::new(scan_items, 4 * 1_024 * 1_024).unwrap();
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map scan");
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    loop {
        let batch = bytes
            .scan(.., direction, continuation.as_ref(), limit)
            .expect("scan byte map benchmark page");
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
    transaction
        .commit()
        .expect("finish byte map scan transaction");
    started.elapsed()
}

fn measure_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin scan transaction");
    let limit = ScanLimit::new(scan_items, 4 * 1_024 * 1_024).unwrap();
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access scan map");
    let mut continuation = None;
    loop {
        let batch = map
            .scan(.., direction, continuation.as_ref(), limit)
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

fn measure_stage_steps<SIZE>(
    fixture: &mut StageFixture<SIZE>,
    steps: usize,
    operations_per_step: usize,
) -> Duration {
    let operations_per_step =
        u64::try_from(operations_per_step).expect("stage batch size fits in u64");
    let stage_keys = u64::try_from(STAGE_KEYS).expect("stage key count fits in u64");
    let started = std::time::Instant::now();
    for _ in 0..steps {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin stage transaction");
        let cursor = fixture
            .cursor
            .access(transaction.access())
            .expect("access stage cursor")
            .get()
            .expect("read stage cursor")
            .expect("seeded stage cursor");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access stage map");
            for offset in 0..operations_per_step {
                let key = cursor
                    .wrapping_mul(operations_per_step)
                    .wrapping_add(offset)
                    % stage_keys;
                let mut value = map
                    .get(&key)
                    .expect("read stage item")
                    .expect("seeded stage item");
                value[0] = value[0].wrapping_add(1);
                map.put(&key, &value).expect("write stage item");
            }
        }
        fixture
            .cursor
            .access(transaction.access())
            .expect("access stage cursor")
            .set(&cursor.wrapping_add(1))
            .expect("advance stage cursor");
        transaction.commit().expect("commit stage transaction");
    }
    started.elapsed()
}

fn measure_hot_overwrite_rollback<SIZE>(fixture: &mut Fixture<SIZE>, entries: usize) -> Duration {
    let value = vec![0xa5; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin overwrite transaction");
    {
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access overwrite map");
        for key in 0..entries {
            map.put(&(key as u64), &value)
                .expect("overwrite benchmark item");
        }
    }
    drop(transaction);
    started.elapsed()
}

fn measure_single_put_commits<SIZE>(fixture: &mut Fixture<SIZE>, commits: usize) -> Duration {
    let started = std::time::Instant::now();
    let mut encoded = vec![0x5a; VALUE_BYTES];
    for value in 0..commits {
        encoded[..std::mem::size_of::<u64>()].copy_from_slice(&(value as u64).to_be_bytes());
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin single-put transaction");
        fixture
            .map
            .access(transaction.access())
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
    mut small: impl FnMut() -> Duration,
    mut large: impl FnMut() -> Duration,
) {
    small();
    large();
    let mut small_durations = Vec::with_capacity(samples);
    let mut large_durations = Vec::with_capacity(samples);
    for sample in 0..samples {
        if sample % 2 == 0 {
            small_durations.push(small());
            large_durations.push(large());
        } else {
            large_durations.push(large());
            small_durations.push(small());
        }
    }
    let mut ratios = small_durations
        .iter()
        .zip(&large_durations)
        .map(|(small, large)| small.as_secs_f64() / large.as_secs_f64())
        .collect::<Vec<_>>();
    ratios.sort_by(f64::total_cmp);
    let median_ratio = ratios[ratios.len() / 2];
    let small_wins = small_durations
        .iter()
        .zip(&large_durations)
        .filter(|(small, large)| small < large)
        .count();
    report(workload, "Small", operations, small_durations);
    report(workload, "Large", operations, large_durations);
    println!("  paired Small/Large median={median_ratio:.3}x; Small wins {small_wins}/{samples}");
}

fn report(workload: &str, size: &str, operations: usize, mut durations: Vec<Duration>) {
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let rate = operations as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_operation = average_duration(median, operations);
    println!(
        "{workload:<28} {size:<10} {operations:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        duration(min),
        duration(median),
        duration(max),
    );
}

fn print_section(name: &str, description: &str) {
    println!();
    println!("=== {name} ===");
    println!("{description}");
    println!(
        "{:<28} {:<10} {:>12} {:>12} {:>12} {:>12} {:>12} {:>14}",
        "workload", "data", "operations", "min", "median", "max", "median/op", "median ops/s"
    );
}

fn print_group(description: &str) {
    println!();
    println!("-- {description} --");
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
