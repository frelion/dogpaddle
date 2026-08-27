//! Scenario benchmarks for both physical forms of `OrderedMap`.

use dogpaddle_bench_protocol::{ConfigurationRecord, Fields, positive_usize};
use dogpaddle_store::{Large, ScanDirection, Small};

mod support;

#[path = "ordered_map/fixture.rs"]
mod fixture;
#[path = "ordered_map/measure.rs"]
mod measure;
#[path = "ordered_map/oracle.rs"]
mod oracle;
#[path = "ordered_map/report.rs"]
mod report;

use fixture::{Fixture, ScanFixture, StationFixture};
use measure::{
    measure_bulk_put, measure_byte_map_bulk_put, measure_byte_map_point_get, measure_byte_map_scan,
    measure_hot_overwrite_rollback, measure_point_get, measure_primitive_scan, measure_scan,
    measure_single_put_commits, measure_station_steps, measure_vec_scan,
};
use report::{BenchmarkCase, print_group, print_section, report_mode_pair, report_size_pair};
use support::{initialize, write_record};

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_BACKGROUND_NAMESPACES: usize = 8;
const DEFAULT_SCAN_ITEMS: usize = 1_024;
const DEFAULT_SCAN_BYTES: usize = 4 * 1_024 * 1_024;
const DEFAULT_WIDE_SCAN_ENTRIES: usize = 10_000;
const VALUE_BYTES: usize = 64;
const WIDE_VALUE_BYTES: usize = 8 * 1_024;
const STATION_KEYS: usize = 1_024;
const RANDOM_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

struct ScanWorkload {
    case: BenchmarkCase,
    direction: ScanDirection,
    kind: ScanKind,
}

#[derive(Clone, Copy)]
enum ScanKind {
    ByteMap,
    TypedMap,
}

fn main() {
    let _bench_root = initialize("store_ordered_map");

    let entries = positive_usize("DOGPADDLE_BENCH_ENTRIES", DEFAULT_ENTRIES)
        .expect("parse OrderedMap entry count");
    let commits = positive_usize("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS)
        .expect("parse OrderedMap commit count");
    let samples = positive_usize("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES)
        .expect("parse OrderedMap sample count");
    let background_namespaces = positive_usize(
        "DOGPADDLE_BENCH_BACKGROUND_NAMESPACES",
        DEFAULT_BACKGROUND_NAMESPACES,
    )
    .expect("parse OrderedMap background namespace count");
    let scan_items = positive_usize("DOGPADDLE_BENCH_SCAN_ITEMS", DEFAULT_SCAN_ITEMS)
        .expect("parse OrderedMap scan item limit");
    let scan_bytes = positive_usize("DOGPADDLE_BENCH_SCAN_BYTES", DEFAULT_SCAN_BYTES)
        .expect("parse OrderedMap scan byte limit");
    let wide_scan_entries = positive_usize(
        "DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES",
        DEFAULT_WIDE_SCAN_ENTRIES,
    )
    .expect("parse OrderedMap wide scan entry count");
    assert!(
        entries > 0
            && commits > 0
            && samples > 0
            && background_namespaces > 0
            && scan_items > 0
            && scan_bytes > 0
            && wide_scan_entries > 0
    );
    let mut fields = Fields::new();
    for (name, value) in [
        ("entries", entries),
        ("value_bytes", VALUE_BYTES),
        ("wide_scan_entries", wide_scan_entries),
        ("wide_value_bytes", WIDE_VALUE_BYTES),
        ("commits", commits),
        ("samples", samples),
        ("background_namespaces", background_namespaces),
        ("scan_items", scan_items),
        ("scan_bytes", scan_bytes),
    ] {
        fields
            .insert(name, value)
            .expect("construct OrderedMap configuration fields");
    }
    fields
        .insert("random_seed", RANDOM_SEED)
        .expect("construct OrderedMap random seed field");
    let record = ConfigurationRecord::new("store_ordered_map", fields)
        .expect("construct OrderedMap configuration record");
    write_record(&record);

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
    print_group("Station-shaped atomic batches: map updates plus one Cell cursor");
    benchmark_station_steps(commits, samples);
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
    report_size_pair(
        &primitive_case("primitive asc full scan", entries),
        samples,
        || measure_primitive_scan(&mut small_primitive, entries, scan_items, scan_bytes),
        || measure_primitive_scan(&mut large_primitive, entries, scan_items, scan_bytes),
    );

    let wide_value = vec![0x5a; WIDE_VALUE_BYTES];
    let mut small_wide = ScanFixture::<Vec<u8>, Small>::populated(wide_entries, &wide_value);
    let mut large_wide = ScanFixture::<Vec<u8>, Large>::populated(wide_entries, &wide_value);
    report_mode_pair(
        &wide_case("wide asc scan Small", wide_entries),
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
        &wide_case("wide asc scan Large", wide_entries),
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
    report_size_pair(
        &map_case("byte map bulk put + commit", entries),
        samples,
        || measure_byte_map_bulk_put::<Small>(entries),
        || measure_byte_map_bulk_put::<Large>(entries),
    );
    report_size_pair(
        &map_case("bulk put + commit", entries),
        samples,
        || measure_bulk_put::<Small>(entries),
        || measure_bulk_put::<Large>(entries),
    );

    let mut small_bytes = Fixture::<Small>::populated_bytes(entries);
    let mut large_bytes = Fixture::<Large>::populated_bytes(entries);
    report_size_pair(
        &map_case("hot byte map point get", entries),
        samples,
        || measure_byte_map_point_get(&mut small_bytes, entries),
        || measure_byte_map_point_get(&mut large_bytes, entries),
    );
    report_scan_pair(
        &ScanWorkload {
            case: map_case("hot byte map asc scan", entries),
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
        &ScanWorkload {
            case: map_case("hot byte map desc scan", entries),
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
    report_size_pair(
        &map_case("hot point get", entries),
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        &ScanWorkload {
            case: map_case("hot ascending scan", entries),
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
        &ScanWorkload {
            case: map_case("hot descending scan", entries),
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
    report_size_pair(
        &map_case("hot overwrite + rollback", entries),
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
        &map_case("narrow asc scan Small", entries),
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
        &map_case("narrow asc scan Large", entries),
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

fn benchmark_station_steps(steps: usize, samples: usize) {
    for operations_per_step in [1, 8, 64] {
        report_size_pair(
            &station_case(steps, operations_per_step),
            samples,
            || {
                let mut fixture = StationFixture::<Small>::populated();
                measure_station_steps(&mut fixture, steps, operations_per_step)
            },
            || {
                let mut fixture = StationFixture::<Large>::populated();
                measure_station_steps(&mut fixture, steps, operations_per_step)
            },
        );
    }
}

fn benchmark_durable_overwrite(commits: usize, samples: usize) {
    report_size_pair(
        &transactional_map_case("durable overwrite commit", commits),
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
    report_size_pair(
        &map_case("mixed hot point get", entries),
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        &ScanWorkload {
            case: map_case("mixed hot ascending scan", entries),
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
        &ScanWorkload {
            case: map_case("mixed hot descending scan", entries),
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
    workload: &ScanWorkload,
    small: &mut Fixture<SmallSize>,
    large: &mut Fixture<LargeSize>,
    entries: usize,
    samples: usize,
    scan_items: usize,
    scan_bytes: usize,
) {
    report_size_pair(
        &workload.case,
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

fn map_case(workload: impl Into<String>, operations: usize) -> BenchmarkCase {
    BenchmarkCase::per_operation(workload, operations, 1, size_of::<u64>() + VALUE_BYTES)
}

fn transactional_map_case(workload: impl Into<String>, operations: usize) -> BenchmarkCase {
    BenchmarkCase::per_operation(
        workload,
        operations,
        operations,
        size_of::<u64>() + VALUE_BYTES,
    )
}

fn primitive_case(workload: impl Into<String>, operations: usize) -> BenchmarkCase {
    BenchmarkCase::per_operation(workload, operations, 1, size_of::<u64>() * 2)
}

fn wide_case(workload: impl Into<String>, operations: usize) -> BenchmarkCase {
    BenchmarkCase::per_operation(workload, operations, 1, size_of::<u64>() + WIDE_VALUE_BYTES)
}

fn station_case(steps: usize, operations_per_step: usize) -> BenchmarkCase {
    let bytes_per_step = operations_per_step
        .checked_mul(2 * (size_of::<u64>() + VALUE_BYTES))
        .and_then(|bytes| bytes.checked_add(2 * size_of::<u64>()))
        .expect("station logical byte count fits in usize");
    BenchmarkCase::per_operation(
        format!("station step x{operations_per_step}"),
        steps,
        steps,
        bytes_per_step,
    )
}
