//! Scenario benchmarks for both physical forms of `OrderedMap`.

use dogpaddle_bench_protocol::{BenchmarkProfile, Fields, Plan, Run};
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
use report::{BenchmarkCase, FrozenCases, report_mode_pair, report_size_pair};
use support::BenchRoot;

const BENCHMARK: &str = "ordered_map";
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
struct ScanLimits {
    items: usize,
    bytes: usize,
}

#[derive(Clone, Copy)]
struct Config {
    entries: usize,
    commits: usize,
    samples: usize,
    background_namespaces: usize,
    scan_items: usize,
    scan_bytes: usize,
    wide_scan_entries: usize,
}

#[derive(Clone, Copy)]
enum ScanKind {
    ByteMap,
    TypedMap,
}

impl Config {
    const fn for_profile(profile: BenchmarkProfile) -> Self {
        match profile {
            BenchmarkProfile::Smoke => Self {
                entries: 4,
                commits: 1,
                samples: 1,
                background_namespaces: 1,
                scan_items: 2,
                scan_bytes: 16_384,
                wide_scan_entries: 2,
            },
            BenchmarkProfile::Reference => Self {
                entries: DEFAULT_ENTRIES,
                commits: DEFAULT_COMMITS,
                samples: DEFAULT_SAMPLES,
                background_namespaces: DEFAULT_BACKGROUND_NAMESPACES,
                scan_items: DEFAULT_SCAN_ITEMS,
                scan_bytes: DEFAULT_SCAN_BYTES,
                wide_scan_entries: DEFAULT_WIDE_SCAN_ENTRIES,
            },
        }
    }
}

fn main() {
    let profile = BenchmarkProfile::from_environment();
    let config = Config::for_profile(profile);
    let Config {
        entries,
        commits,
        samples,
        background_namespaces,
        scan_items,
        scan_bytes,
        wide_scan_entries,
    } = config;
    assert!(
        entries > 0
            && commits > 0
            && samples > 0
            && background_namespaces > 0
            && scan_items > 0
            && scan_bytes > 0
            && wide_scan_entries > 0
    );
    let mut plan = Plan::new(profile, configuration_fields(&config));
    let mut cases = benchmark_plan(&mut plan, &config);
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    let bench_root = BenchRoot::new(&run);
    let scan = ScanLimits {
        items: scan_items,
        bytes: scan_bytes,
    };

    benchmark_isolated(&mut run, &mut cases, &bench_root, entries, samples, scan);
    benchmark_scan_decoding(
        &mut run,
        &mut cases,
        &bench_root,
        entries,
        wide_scan_entries,
        samples,
        scan,
    );
    benchmark_mixed(
        &mut run,
        &mut cases,
        &bench_root,
        entries,
        samples,
        background_namespaces,
        scan,
    );
    benchmark_station_steps(&mut run, &mut cases, &bench_root, commits, samples);
    benchmark_durable_overwrite(&mut run, &mut cases, &bench_root, commits, samples);
    cases.finish();
    run.finish(|| {});
}

fn configuration_fields(config: &Config) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in [
        ("entries", config.entries),
        ("value_bytes", VALUE_BYTES),
        ("wide_scan_entries", config.wide_scan_entries),
        ("wide_value_bytes", WIDE_VALUE_BYTES),
        ("commits", config.commits),
        ("samples", config.samples),
        ("background_namespaces", config.background_namespaces),
        ("scan_items", config.scan_items),
        ("scan_bytes", config.scan_bytes),
    ] {
        fields.insert(name, value);
    }
    fields.insert("random_seed", RANDOM_SEED);
    fields
        .with("execution", "single_thread")
        .with("cache", "warm")
        .with("mdbx_sync_mode", "durable")
}

fn benchmark_plan(plan: &mut Plan, config: &Config) -> FrozenCases {
    let mut cases = FrozenCases::new();
    let entries = config.entries;
    let samples = config.samples;
    cases.size(
        plan,
        map_case("byte map bulk put + commit", entries),
        samples,
    );
    cases.size(plan, map_case("bulk put + commit", entries), samples);
    cases.size(plan, map_case("hot byte map point get", entries), samples);
    cases.size(plan, map_case("hot byte map asc scan", entries), samples);
    cases.size(plan, map_case("hot byte map desc scan", entries), samples);
    cases.size(plan, map_case("hot point get", entries), samples);
    cases.size(plan, map_case("hot ascending scan", entries), samples);
    cases.size(plan, map_case("hot descending scan", entries), samples);
    cases.mode(plan, map_case("narrow asc scan Small", entries), samples);
    cases.mode(plan, map_case("narrow asc scan Large", entries), samples);
    cases.size(plan, map_case("hot overwrite + rollback", entries), samples);
    cases.size(
        plan,
        primitive_case("primitive asc full scan", entries),
        samples,
    );
    cases.mode(
        plan,
        wide_case("wide asc scan Small", config.wide_scan_entries),
        samples,
    );
    cases.mode(
        plan,
        wide_case("wide asc scan Large", config.wide_scan_entries),
        samples,
    );
    cases.size(plan, map_case("mixed hot point get", entries), samples);
    cases.size(plan, map_case("mixed hot ascending scan", entries), samples);
    cases.size(
        plan,
        map_case("mixed hot descending scan", entries),
        samples,
    );
    for operations_per_step in [1, 8, 64] {
        cases.size(
            plan,
            station_case(config.commits, operations_per_step),
            samples,
        );
    }
    cases.size(
        plan,
        transactional_map_case("durable overwrite commit", config.commits),
        samples,
    );
    cases
}

fn benchmark_scan_decoding(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    entries: usize,
    wide_entries: usize,
    samples: usize,
    scan: ScanLimits,
) {
    let mut small_primitive = ScanFixture::<u64, Small>::populated(bench_root, entries, &0x5a);
    let mut large_primitive = ScanFixture::<u64, Large>::populated(bench_root, entries, &0x5a);
    report_size_pair(
        run,
        plan,
        &primitive_case("primitive asc full scan", entries),
        samples,
        || measure_primitive_scan(&mut small_primitive, entries, scan.items, scan.bytes),
        || measure_primitive_scan(&mut large_primitive, entries, scan.items, scan.bytes),
    );

    let wide_value = vec![0x5a; WIDE_VALUE_BYTES];
    let mut small_wide =
        ScanFixture::<Vec<u8>, Small>::populated(bench_root, wide_entries, &wide_value);
    let mut large_wide =
        ScanFixture::<Vec<u8>, Large>::populated(bench_root, wide_entries, &wide_value);
    report_mode_pair(
        run,
        plan,
        &wide_case("wide asc scan Small", wide_entries),
        samples,
        &mut small_wide,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan.items,
                scan.bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan.items,
                scan.bytes,
                true,
            )
        },
    );
    report_mode_pair(
        run,
        plan,
        &wide_case("wide asc scan Large", wide_entries),
        samples,
        &mut large_wide,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan.items,
                scan.bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                wide_entries,
                scan.items,
                scan.bytes,
                true,
            )
        },
    );
}

fn benchmark_isolated(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    entries: usize,
    samples: usize,
    scan: ScanLimits,
) {
    report_size_pair(
        run,
        plan,
        &map_case("byte map bulk put + commit", entries),
        samples,
        || measure_byte_map_bulk_put::<Small>(bench_root, entries),
        || measure_byte_map_bulk_put::<Large>(bench_root, entries),
    );
    report_size_pair(
        run,
        plan,
        &map_case("bulk put + commit", entries),
        samples,
        || measure_bulk_put::<Small>(bench_root, entries),
        || measure_bulk_put::<Large>(bench_root, entries),
    );

    benchmark_byte_map(run, plan, bench_root, entries, samples, scan);

    let mut small = Fixture::<Small>::populated_typed(bench_root, entries);
    let mut large = Fixture::<Large>::populated_typed(bench_root, entries);
    report_size_pair(
        run,
        plan,
        &map_case("hot point get", entries),
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        run,
        plan,
        &ScanWorkload {
            case: map_case("hot ascending scan", entries),
            direction: ScanDirection::Ascending,
            kind: ScanKind::TypedMap,
        },
        (&mut small, &mut large),
        entries,
        samples,
        scan,
    );
    report_scan_pair(
        run,
        plan,
        &ScanWorkload {
            case: map_case("hot descending scan", entries),
            direction: ScanDirection::Descending,
            kind: ScanKind::TypedMap,
        },
        (&mut small, &mut large),
        entries,
        samples,
        scan,
    );
    benchmark_narrow_scan_modes(run, plan, &mut small, &mut large, entries, samples, scan);
    report_size_pair(
        run,
        plan,
        &map_case("hot overwrite + rollback", entries),
        samples,
        || {
            let mut fixture = Fixture::<Small>::populated_typed(bench_root, entries);
            measure_hot_overwrite_rollback(&mut fixture, entries)
        },
        || {
            let mut fixture = Fixture::<Large>::populated_typed(bench_root, entries);
            measure_hot_overwrite_rollback(&mut fixture, entries)
        },
    );
}

fn benchmark_byte_map(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    entries: usize,
    samples: usize,
    scan: ScanLimits,
) {
    let mut small = Fixture::<Small>::populated_bytes(bench_root, entries);
    let mut large = Fixture::<Large>::populated_bytes(bench_root, entries);
    report_size_pair(
        run,
        plan,
        &map_case("hot byte map point get", entries),
        samples,
        || measure_byte_map_point_get(&mut small, entries),
        || measure_byte_map_point_get(&mut large, entries),
    );
    for (name, direction) in [
        ("hot byte map asc scan", ScanDirection::Ascending),
        ("hot byte map desc scan", ScanDirection::Descending),
    ] {
        report_scan_pair(
            run,
            plan,
            &ScanWorkload {
                case: map_case(name, entries),
                direction,
                kind: ScanKind::ByteMap,
            },
            (&mut small, &mut large),
            entries,
            samples,
            scan,
        );
    }
}

fn benchmark_narrow_scan_modes(
    run: &mut Run,
    plan: &mut FrozenCases,
    small: &mut Fixture<Small>,
    large: &mut Fixture<Large>,
    entries: usize,
    samples: usize,
    scan: ScanLimits,
) {
    report_mode_pair(
        run,
        plan,
        &map_case("narrow asc scan Small", entries),
        samples,
        small,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan.items,
                scan.bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan.items,
                scan.bytes,
                true,
            )
        },
    );
    report_mode_pair(
        run,
        plan,
        &map_case("narrow asc scan Large", entries),
        samples,
        large,
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan.items,
                scan.bytes,
                false,
            )
        },
        |fixture| {
            measure_vec_scan(
                &mut fixture.transactions,
                &fixture.map,
                entries,
                scan.items,
                scan.bytes,
                true,
            )
        },
    );
}

fn benchmark_station_steps(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    steps: usize,
    samples: usize,
) {
    for operations_per_step in [1, 8, 64] {
        report_size_pair(
            run,
            plan,
            &station_case(steps, operations_per_step),
            samples,
            || {
                let mut fixture = StationFixture::<Small>::populated(bench_root);
                measure_station_steps(&mut fixture, steps, operations_per_step)
            },
            || {
                let mut fixture = StationFixture::<Large>::populated(bench_root);
                measure_station_steps(&mut fixture, steps, operations_per_step)
            },
        );
    }
}

fn benchmark_durable_overwrite(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    commits: usize,
    samples: usize,
) {
    report_size_pair(
        run,
        plan,
        &transactional_map_case("durable overwrite commit", commits),
        samples,
        || {
            let mut fixture = Fixture::<Small>::empty(bench_root);
            measure_single_put_commits(&mut fixture, commits)
        },
        || {
            let mut fixture = Fixture::<Large>::empty(bench_root);
            measure_single_put_commits(&mut fixture, commits)
        },
    );
}

fn benchmark_mixed(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    entries: usize,
    samples: usize,
    background_namespaces: usize,
    scan: ScanLimits,
) {
    let mut small = Fixture::<Small>::populated_with_small_background(
        bench_root,
        entries,
        background_namespaces,
    );
    let mut large = Fixture::<Large>::populated_with_small_background(
        bench_root,
        entries,
        background_namespaces,
    );
    report_size_pair(
        run,
        plan,
        &map_case("mixed hot point get", entries),
        samples,
        || measure_point_get(&mut small, entries),
        || measure_point_get(&mut large, entries),
    );
    report_scan_pair(
        run,
        plan,
        &ScanWorkload {
            case: map_case("mixed hot ascending scan", entries),
            direction: ScanDirection::Ascending,
            kind: ScanKind::TypedMap,
        },
        (&mut small, &mut large),
        entries,
        samples,
        scan,
    );
    report_scan_pair(
        run,
        plan,
        &ScanWorkload {
            case: map_case("mixed hot descending scan", entries),
            direction: ScanDirection::Descending,
            kind: ScanKind::TypedMap,
        },
        (&mut small, &mut large),
        entries,
        samples,
        scan,
    );
}

fn report_scan_pair<SmallSize, LargeSize>(
    run: &mut Run,
    plan: &mut FrozenCases,
    workload: &ScanWorkload,
    fixtures: (&mut Fixture<SmallSize>, &mut Fixture<LargeSize>),
    entries: usize,
    samples: usize,
    scan: ScanLimits,
) {
    let (small, large) = fixtures;
    report_size_pair(
        run,
        plan,
        &workload.case,
        samples,
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(small, entries, scan.items, scan.bytes, workload.direction)
            }
            ScanKind::TypedMap => {
                measure_scan(small, entries, scan.items, scan.bytes, workload.direction)
            }
        },
        || match workload.kind {
            ScanKind::ByteMap => {
                measure_byte_map_scan(large, entries, scan.items, scan.bytes, workload.direction)
            }
            ScanKind::TypedMap => {
                measure_scan(large, entries, scan.items, scan.bytes, workload.direction)
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
