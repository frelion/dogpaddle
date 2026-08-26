//! Scenario benchmarks for both physical forms of `OrderedMap`.

use std::{hint::black_box, time::Duration};

use dogpaddle_store::{
    Cell, CodecError, Large, OrderedMap, ScanDirection, ScanLimit, Small, Store, StoreData,
    StoreError, StoreValue, Transactions,
};
use tempfile::TempDir;

mod support;

use support::{
    SampleWork, average_duration, emit_configuration, emit_pair_summary, emit_samples,
    emit_summary, format_duration, initialize, sample_dir, setting,
};

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_BACKGROUND_NAMESPACES: usize = 8;
const DEFAULT_SCAN_ITEMS: usize = 1_024;
const DEFAULT_SCAN_BYTES: usize = 4 * 1_024 * 1_024;
const DEFAULT_WIDE_SCAN_ENTRIES: usize = 10_000;
const VALUE_BYTES: usize = 64;
const WIDE_VALUE_BYTES: usize = 8 * 1_024;
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

struct ScanFixture<V, SIZE> {
    transactions: Transactions,
    map: OrderedMap<u64, V, SIZE>,
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
        let root = sample_dir("ordered-map");
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
        let root = sample_dir("ordered-map-background");
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
        let root = sample_dir("ordered-map-multi-collection");
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

impl<V: StoreValue, SIZE> ScanFixture<V, SIZE>
where
    OrderedMap<u64, V, SIZE>: StoreData,
{
    fn populated(entries: usize, value: &V) -> Self {
        let root = sample_dir("ordered-map-scan");
        let mut store =
            Store::create(root.path().join("store")).expect("create scan benchmark store");
        let map = store
            .create_data::<OrderedMap<u64, V, SIZE>>("map")
            .expect("create scan benchmark map");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin scan benchmark seed transaction");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access scan benchmark seed map");
            for key in 0..entries {
                map.put(&(key as u64), value)
                    .expect("seed scan benchmark map");
            }
        }
        transaction
            .commit()
            .expect("commit scan benchmark seed transaction");
        fixture
    }
}

fn main() {
    initialize("store_ordered_map");

    let entries = setting("DOGPADDLE_BENCH_ENTRIES", DEFAULT_ENTRIES);
    let commits = setting("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS);
    let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
    let background_namespaces = setting(
        "DOGPADDLE_BENCH_BACKGROUND_NAMESPACES",
        DEFAULT_BACKGROUND_NAMESPACES,
    );
    let scan_items = setting("DOGPADDLE_BENCH_SCAN_ITEMS", DEFAULT_SCAN_ITEMS);
    let scan_bytes = setting("DOGPADDLE_BENCH_SCAN_BYTES", DEFAULT_SCAN_BYTES);
    let wide_scan_entries = setting(
        "DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES",
        DEFAULT_WIDE_SCAN_ENTRIES,
    );
    assert!(
        entries > 0
            && commits > 0
            && samples > 0
            && background_namespaces > 0
            && scan_items > 0
            && scan_bytes > 0
            && wide_scan_entries > 0
    );
    emit_configuration(
        "store_ordered_map",
        &format!(
            "\"entries\":{entries},\"value_bytes\":{VALUE_BYTES},\"wide_scan_entries\":{wide_scan_entries},\"wide_value_bytes\":{WIDE_VALUE_BYTES},\"commits\":{commits},\"samples\":{samples},\"background_namespaces\":{background_namespaces},\"scan_items\":{scan_items},\"scan_bytes\":{scan_bytes},\"random_seed\":{RANDOM_SEED}"
        ),
    );

    println!("DogPaddle OrderedMap benchmark");
    println!(
        "entries={entries} value_bytes={VALUE_BYTES} wide_scan_entries={wide_scan_entries} wide_value_bytes={WIDE_VALUE_BYTES} commits={commits} samples={samples} background_namespaces={background_namespaces} scan_items={scan_items} scan_bytes={scan_bytes}"
    );
    println!(
        "sync=durable execution=single-thread point_scan_cache=warm random_seed={RANDOM_SEED:#x}"
    );

    print_section(
        "OrderedMap<K, V, Small> vs OrderedMap<K, V, Large>",
        "same workloads; Small shares the main table, Large owns a dedicated table",
    );
    print_group("isolated object: bulk write, warm point read, scan, and rollback");
    benchmark_isolated(entries, samples, scan_items, scan_bytes);
    print_group("scan decoding cost: primitive value, wide full decode, and wide projection");
    benchmark_scan_decoding(entries, wide_scan_entries, samples, scan_items, scan_bytes);
    print_group(&format!(
        "target map with {background_namespaces} populated Small background namespaces"
    ));
    benchmark_mixed(
        entries,
        samples,
        background_namespaces,
        scan_items,
        scan_bytes,
    );
    print_group("Stage-shaped atomic batches: map updates plus one Cell cursor");
    benchmark_stage_steps(commits, samples);
    print_group("worst-case commit amortization: one overwrite per durable transaction");
    benchmark_durable_overwrite(commits, samples);
}

fn benchmark_scan_decoding(
    entries: usize,
    wide_entries: usize,
    samples: usize,
    scan_items: usize,
    scan_bytes: usize,
) {
    let mut small_primitive = ScanFixture::<u64, Small>::populated(entries, &0x5a);
    let mut large_primitive = ScanFixture::<u64, Large>::populated(entries, &0x5a);
    report_pair(
        "primitive asc full scan",
        entries,
        samples,
        || measure_primitive_scan(&mut small_primitive, entries, scan_items, scan_bytes),
        || measure_primitive_scan(&mut large_primitive, entries, scan_items, scan_bytes),
    );

    let wide_value = vec![0x5a; WIDE_VALUE_BYTES];
    let mut small_wide = ScanFixture::<Vec<u8>, Small>::populated(wide_entries, &wide_value);
    let mut large_wide = ScanFixture::<Vec<u8>, Large>::populated(wide_entries, &wide_value);
    report_mode_pair(
        "wide asc scan Small",
        wide_entries,
        samples,
        &mut small_wide,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan_items,
                scan_bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan_items,
                scan_bytes,
                true,
            )
        },
    );
    report_mode_pair(
        "wide asc scan Large",
        wide_entries,
        samples,
        &mut large_wide,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan_items,
                scan_bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan_items,
                scan_bytes,
                true,
            )
        },
    );
}

fn benchmark_isolated(entries: usize, samples: usize, scan_items: usize, scan_bytes: usize) {
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
        scan_bytes,
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
        scan_bytes,
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
        scan_bytes,
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
        scan_bytes,
    );
    benchmark_narrow_scan_modes(
        &mut small, &mut large, entries, samples, scan_items, scan_bytes,
    );
    report_pair(
        "hot overwrite + rollback",
        entries,
        samples,
        || {
            let mut fixture = Fixture::<Small>::populated_typed(entries);
            measure_hot_overwrite_rollback(&mut fixture, entries)
        },
        || {
            let mut fixture = Fixture::<Large>::populated_typed(entries);
            measure_hot_overwrite_rollback(&mut fixture, entries)
        },
    );
}

fn benchmark_narrow_scan_modes(
    small: &mut Fixture<Small>,
    large: &mut Fixture<Large>,
    entries: usize,
    samples: usize,
    scan_items: usize,
    scan_bytes: usize,
) {
    report_mode_pair(
        "narrow asc scan Small",
        entries,
        samples,
        small,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan_items,
                scan_bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan_items,
                scan_bytes,
                true,
            )
        },
    );
    report_mode_pair(
        "narrow asc scan Large",
        entries,
        samples,
        large,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan_items,
                scan_bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan_items,
                scan_bytes,
                true,
            )
        },
    );
}

fn benchmark_stage_steps(steps: usize, samples: usize) {
    for operations_per_step in [1, 8, 64] {
        report_pair(
            &format!("stage step x{operations_per_step}"),
            steps,
            samples,
            || {
                let mut fixture = StageFixture::<Small>::populated();
                measure_stage_steps(&mut fixture, steps, operations_per_step)
            },
            || {
                let mut fixture = StageFixture::<Large>::populated();
                measure_stage_steps(&mut fixture, steps, operations_per_step)
            },
        );
    }
}

fn benchmark_durable_overwrite(commits: usize, samples: usize) {
    report_pair(
        "durable overwrite commit",
        commits,
        samples,
        || {
            let mut fixture = Fixture::<Small>::empty();
            measure_single_put_commits(&mut fixture, commits)
        },
        || {
            let mut fixture = Fixture::<Large>::empty();
            measure_single_put_commits(&mut fixture, commits)
        },
    );
}

fn benchmark_mixed(
    entries: usize,
    samples: usize,
    background_namespaces: usize,
    scan_items: usize,
    scan_bytes: usize,
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
        scan_bytes,
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
        scan_bytes,
    );
}

fn report_scan_pair<SmallSize, LargeSize>(
    workload: ScanWorkload,
    small: &mut Fixture<SmallSize>,
    large: &mut Fixture<LargeSize>,
    entries: usize,
    samples: usize,
    scan_items: usize,
    scan_bytes: usize,
) {
    report_pair(
        workload.name,
        entries,
        samples,
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(small, entries, scan_items, scan_bytes, workload.direction)
            }
            ScanKind::TypedMap => {
                measure_scan(small, entries, scan_items, scan_bytes, workload.direction)
            }
        },
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(large, entries, scan_items, scan_bytes, workload.direction)
            }
            ScanKind::TypedMap => {
                measure_scan(large, entries, scan_items, scan_bytes, workload.direction)
            }
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
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map write validation transaction");
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map for write validation");
    let first = 0_u64.to_be_bytes().to_vec();
    let last = u64::try_from(entries - 1)
        .expect("entry count fits u64")
        .to_be_bytes()
        .to_vec();
    assert_eq!(
        bytes.get(&first).unwrap().as_deref(),
        Some(value.as_slice())
    );
    assert_eq!(bytes.get(&last).unwrap().as_deref(), Some(value.as_slice()));
    transaction
        .commit()
        .expect("finish byte map write validation transaction");
    elapsed
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
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin map write validation transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access map for write validation");
    assert_eq!(map.get(&0).unwrap().as_deref(), Some(value.as_slice()));
    assert_eq!(
        map.get(&u64::try_from(entries - 1).expect("entry count fits u64"))
            .unwrap()
            .as_deref(),
        Some(value.as_slice())
    );
    transaction
        .commit()
        .expect("finish map write validation transaction");
    elapsed
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
    let elapsed = started.elapsed();
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
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
    let elapsed = started.elapsed();
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

fn measure_byte_map_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map scan transaction");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map scan");
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    loop {
        let next = bytes
            .scan(.., direction, continuation.as_ref(), limit, |entry| {
                let (_, value) = entry.decode_owned()?;
                count += 1;
                checksum = checksum.wrapping_add(usize::from(value[0]));
                Ok::<(), StoreError>(())
            })
            .expect("scan byte map benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish byte map scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

fn measure_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin scan transaction");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access scan map");
    let mut continuation = None;
    loop {
        let next = map
            .scan(.., direction, continuation.as_ref(), limit, |entry| {
                let (_, value) = entry.decode_owned()?;
                count += 1;
                checksum = checksum.wrapping_add(usize::from(value[0]));
                Ok::<(), StoreError>(())
            })
            .expect("scan benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction.commit().expect("finish scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

fn measure_primitive_scan<SIZE>(
    fixture: &mut ScanFixture<u64, SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin primitive scan transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access primitive scan map");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_u64;
    loop {
        let next = map
            .scan(
                ..,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
                |entry| {
                    let (key, value) = entry.decode_owned()?;
                    count += 1;
                    checksum = checksum.wrapping_add(key ^ value);
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan primitive benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish primitive scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, expected_scan_checksum(entries, 0x5a));
    elapsed
}

fn measure_vec_scan<SIZE>(
    transactions: &mut Transactions,
    map: &OrderedMap<u64, Vec<u8>, SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    project: bool,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = transactions.begin().expect("begin vector scan transaction");
    let map = map
        .access(transaction.access())
        .expect("access vector scan map");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_u64;
    loop {
        let next = map
            .scan(
                ..,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
                |entry| {
                    let value = if project {
                        entry.project(project_vec_checksum)?
                    } else {
                        let (key, value) = entry.decode_owned()?;
                        key ^ u64::from(value[0])
                    };
                    count += 1;
                    checksum = checksum.wrapping_add(value);
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan wide benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish vector scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, expected_scan_checksum(entries, 0x5a));
    elapsed
}

fn project_vec_checksum(key: &[u8], value: &[u8]) -> Result<u64, CodecError> {
    let key = u64::from_be_bytes(
        key.try_into()
            .map_err(|_| CodecError::new("invalid benchmark key"))?,
    );
    Ok(key ^ u64::from(value[0]))
}

fn measure_stage_steps<SIZE>(
    fixture: &mut StageFixture<SIZE>,
    steps: usize,
    operations_per_step: usize,
) -> Duration
where
    TypedMap<SIZE>: StoreData,
{
    let operations_per_step_u64 =
        u64::try_from(operations_per_step).expect("stage batch size fits in u64");
    let stage_keys = u64::try_from(STAGE_KEYS).expect("stage key count fits in u64");
    let initial_cursor = read_stage_cursor(fixture);
    let expected_first_bytes =
        expected_stage_first_bytes(initial_cursor, steps, operations_per_step_u64, stage_keys);
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
            for offset in 0..operations_per_step_u64 {
                let key = cursor
                    .wrapping_mul(operations_per_step_u64)
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
    let elapsed = started.elapsed();
    assert_eq!(
        read_stage_cursor(fixture),
        initial_cursor.wrapping_add(u64::try_from(steps).expect("step count fits u64"))
    );
    assert_stage_map(fixture, &expected_first_bytes);
    elapsed
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
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin rollback validation transaction");
    let value = fixture
        .map
        .access(transaction.access())
        .expect("access map for rollback validation")
        .get(&0)
        .expect("read rollback validation value")
        .expect("seeded rollback validation value");
    assert_eq!(value[0], 0x5a);
    transaction
        .commit()
        .expect("finish rollback validation transaction");
    elapsed
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
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin durable overwrite validation transaction");
    let actual = fixture
        .map
        .access(transaction.access())
        .expect("access durable overwrite validation map")
        .get(&0)
        .expect("read durable overwrite validation value")
        .expect("durable overwrite value exists");
    assert_eq!(
        &actual[..size_of::<u64>()],
        &u64::try_from(commits - 1)
            .expect("commit count fits u64")
            .to_be_bytes()
    );
    transaction
        .commit()
        .expect("finish durable overwrite validation transaction");
    elapsed
}

fn read_stage_cursor<SIZE>(fixture: &mut StageFixture<SIZE>) -> u64
where
    TypedMap<SIZE>: StoreData,
{
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage cursor validation transaction");
    let cursor = fixture
        .cursor
        .access(transaction.access())
        .expect("access stage cursor for validation")
        .get()
        .expect("read stage cursor for validation")
        .expect("seeded stage cursor exists");
    transaction
        .commit()
        .expect("finish stage cursor validation transaction");
    cursor
}

fn expected_stage_first_bytes(
    initial_cursor: u64,
    steps: usize,
    operations_per_step: u64,
    stage_keys: u64,
) -> Vec<u8> {
    let mut expected = vec![0x5a_u8; STAGE_KEYS];
    for step in 0..steps {
        let cursor =
            initial_cursor.wrapping_add(u64::try_from(step).expect("stage step fits in u64"));
        for offset in 0..operations_per_step {
            let key = cursor
                .wrapping_mul(operations_per_step)
                .wrapping_add(offset)
                % stage_keys;
            let key = usize::try_from(key).expect("stage key fits in usize");
            expected[key] = expected[key].wrapping_add(1);
        }
    }
    expected
}

fn assert_stage_map<SIZE>(fixture: &mut StageFixture<SIZE>, expected_first_bytes: &[u8])
where
    TypedMap<SIZE>: StoreData,
{
    assert_eq!(expected_first_bytes.len(), STAGE_KEYS);
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage map validation transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access stage map for validation");
    for (key, expected_first) in expected_first_bytes.iter().copied().enumerate() {
        let value = map
            .get(&u64::try_from(key).expect("stage validation key fits in u64"))
            .expect("read stage map validation value")
            .expect("seeded stage map value exists");
        assert_eq!(value.len(), VALUE_BYTES);
        assert_eq!(value[0], expected_first);
        assert!(value[1..].iter().all(|byte| *byte == 0x5a));
    }
    transaction
        .commit()
        .expect("finish stage map validation transaction");
}

fn expected_scan_checksum(entries: usize, value: u64) -> u64 {
    (0..entries).fold(0_u64, |checksum, key| {
        checksum.wrapping_add(u64::try_from(key).expect("benchmark key fits u64") ^ value)
    })
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
    emit_pair_summary(
        "store_ordered_map",
        workload,
        "Small",
        "Large",
        &small_durations,
        &large_durations,
    );
    report(workload, "Small", operations, small_durations);
    report(workload, "Large", operations, large_durations);
    println!("  paired Small/Large median={median_ratio:.3}x; Small wins {small_wins}/{samples}");
}

fn report_mode_pair<T>(
    workload: &str,
    operations: usize,
    samples: usize,
    fixture: &mut T,
    mut full: impl FnMut(&mut T) -> Duration,
    mut projected: impl FnMut(&mut T) -> Duration,
) {
    full(fixture);
    projected(fixture);
    let mut full_durations = Vec::with_capacity(samples);
    let mut projected_durations = Vec::with_capacity(samples);
    for sample in 0..samples {
        if sample % 2 == 0 {
            full_durations.push(full(fixture));
            projected_durations.push(projected(fixture));
        } else {
            projected_durations.push(projected(fixture));
            full_durations.push(full(fixture));
        }
    }
    let mut ratios = full_durations
        .iter()
        .zip(&projected_durations)
        .map(|(full, projected)| full.as_secs_f64() / projected.as_secs_f64())
        .collect::<Vec<_>>();
    ratios.sort_by(f64::total_cmp);
    let median_ratio = ratios[ratios.len() / 2];
    let projected_wins = full_durations
        .iter()
        .zip(&projected_durations)
        .filter(|(full, projected)| projected < full)
        .count();
    emit_pair_summary(
        "store_ordered_map",
        workload,
        "Full",
        "Projected",
        &full_durations,
        &projected_durations,
    );
    report(workload, "Full", operations, full_durations);
    report(workload, "Projected", operations, projected_durations);
    println!(
        "  paired Full/Projected median={median_ratio:.3}x; projection wins {projected_wins}/{samples}"
    );
}

fn report(workload: &str, size: &str, operations: usize, mut durations: Vec<Duration>) {
    let transactions =
        if workload.starts_with("stage step") || workload == "durable overwrite commit" {
            operations
        } else {
            1
        };
    let logical_bytes = if let Some(operations_per_step) = workload
        .strip_prefix("stage step x")
        .and_then(|value| value.parse::<usize>().ok())
    {
        let bytes_per_step = operations_per_step
            .checked_mul(2 * (size_of::<u64>() + VALUE_BYTES))
            .and_then(|value| value.checked_add(2 * size_of::<u64>()))
            .unwrap();
        operations.checked_mul(bytes_per_step).unwrap()
    } else if workload.contains("wide") {
        operations
            .checked_mul(size_of::<u64>() + WIDE_VALUE_BYTES)
            .unwrap()
    } else if workload.contains("primitive") {
        operations.checked_mul(size_of::<u64>() * 2).unwrap()
    } else {
        operations
            .checked_mul(size_of::<u64>() + VALUE_BYTES)
            .unwrap()
    };
    let work = SampleWork {
        operations,
        transactions,
        logical_bytes,
    };
    emit_samples("store_ordered_map", workload, size, &durations, work);
    emit_summary("store_ordered_map", workload, size, &durations, work);
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let rate = operations as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_operation = average_duration(median, operations);
    println!(
        "{workload:<28} {size:<10} {operations:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        format_duration(min),
        format_duration(median),
        format_duration(max),
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
