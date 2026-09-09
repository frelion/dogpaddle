//! Representative `OrderedMap` costs on `RocksDB`.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{ScanDirection, ScanLimit};
use serde_json::json;

#[path = "ordered_map/fixture.rs"]
mod fixture;
#[path = "ordered_map/measure.rs"]
mod measure;

use fixture::{MapFixture, StationFixture};
use measure::{
    measure_bulk_put, measure_point_get, measure_scan, measure_single_put_commits,
    measure_station_steps,
};

const BENCHMARK: &str = "ordered_map";
const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SCAN_ITEMS: usize = 1_024;
const DEFAULT_SCAN_BYTES: usize = 4 * 1_024 * 1_024;
const DEFAULT_WIDE_ENTRIES: usize = 10_000;
const VALUE_BYTES: usize = 64;
const WIDE_VALUE_BYTES: usize = 8 * 1_024;
const STATION_KEYS: usize = 1_024;
const STATION_OPERATIONS: usize = 8;
const RANDOM_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Copy)]
struct Config {
    entries: usize,
    commits: usize,
    scan_items: usize,
    scan_bytes: usize,
    wide_entries: usize,
    samples: usize,
    warm_up_time: Duration,
    measurement_time: Duration,
}

impl Config {
    const fn for_profile(profile: PerformanceProfile) -> Self {
        match profile {
            PerformanceProfile::Smoke => Self {
                entries: 4,
                commits: 1,
                scan_items: 2,
                scan_bytes: 16_384,
                wide_entries: 2,
                samples: 10,
                warm_up_time: Duration::from_millis(20),
                measurement_time: Duration::from_millis(50),
            },
            PerformanceProfile::Reference => Self {
                entries: DEFAULT_ENTRIES,
                commits: DEFAULT_COMMITS,
                scan_items: DEFAULT_SCAN_ITEMS,
                scan_bytes: DEFAULT_SCAN_BYTES,
                wide_entries: DEFAULT_WIDE_ENTRIES,
                samples: 15,
                warm_up_time: Duration::from_secs(3),
                measurement_time: Duration::from_secs(5),
            },
        }
    }
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if is_cargo_bench() {
        require_release_build(BENCHMARK);
    }
    let config = Config::for_profile(profile);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    write_context(&root, profile, config);
    let mut criterion = Criterion::default()
        .sample_size(config.samples)
        .warm_up_time(config.warm_up_time)
        .measurement_time(config.measurement_time)
        .without_plots()
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    benchmark(&mut criterion, &root, config);
    criterion.final_summary();
}

fn benchmark(criterion: &mut Criterion, root: &RunRoot, config: Config) {
    assert!(config.entries > 0 && config.commits > 0 && config.wide_entries > 0);
    let scan_limit = ScanLimit::new(config.scan_items, config.scan_bytes).unwrap();
    let mut group = criterion.benchmark_group(BENCHMARK);

    group.throughput(elements(config.entries));
    group.bench_function(
        BenchmarkId::new("bulk_put_commit", config.entries),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || {
                    let mut fixture = MapFixture::empty(root, "bulk-put");
                    measure_bulk_put(&mut fixture, config.entries)
                })
            });
        },
    );

    let fixture = MapFixture::populated(root, "hot", config.entries, VALUE_BYTES);
    group.bench_function(BenchmarkId::new("point_get", config.entries), |bencher| {
        bencher.iter_custom(|iterations| {
            measure_iterations(iterations, || measure_point_get(&fixture, config.entries))
        });
    });
    for (name, direction) in [
        ("ascending_scan", ScanDirection::Ascending),
        ("descending_scan", ScanDirection::Descending),
    ] {
        group.bench_function(BenchmarkId::new(name, config.entries), |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || {
                    measure_scan(&fixture, config.entries, direction, scan_limit)
                })
            });
        });
    }

    group.throughput(elements(config.wide_entries));
    let wide = MapFixture::populated(root, "wide", config.wide_entries, WIDE_VALUE_BYTES);
    group.bench_function(
        BenchmarkId::new("wide_scan", config.wide_entries),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || {
                    measure_scan(
                        &wide,
                        config.wide_entries,
                        ScanDirection::Ascending,
                        scan_limit,
                    )
                })
            });
        },
    );

    group.throughput(elements(config.commits));
    group.bench_function(
        BenchmarkId::new("station_step", config.commits),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || {
                    let mut station = StationFixture::populated(root);
                    measure_station_steps(&mut station, config.commits, STATION_OPERATIONS)
                })
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("durable_hot_overwrite", config.commits),
        |bencher| {
            bencher.iter_custom(|iterations| {
                measure_iterations(iterations, || {
                    let mut fixture = MapFixture::empty(root, "durable-overwrite");
                    measure_single_put_commits(&mut fixture, config.commits)
                })
            });
        },
    );
    group.finish();
}

fn measure_iterations(iterations: u64, mut measure: impl FnMut() -> Duration) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        elapsed = elapsed
            .checked_add(measure())
            .expect("benchmark elapsed duration fits Duration");
    }
    elapsed
}

fn elements(count: usize) -> Throughput {
    Throughput::Elements(u64::try_from(count).expect("operation count fits u64"))
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, config: Config) {
    let context = json!({
        "benchmark": BENCHMARK,
        "runner": "criterion",
        "criterion_version": "0.8.2",
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "entries": config.entries,
            "value_bytes": VALUE_BYTES,
            "wide_entries": config.wide_entries,
            "wide_value_bytes": WIDE_VALUE_BYTES,
            "commits": config.commits,
            "scan_items": config.scan_items,
            "scan_bytes": config.scan_bytes,
            "station_keys": STATION_KEYS,
            "station_operations": STATION_OPERATIONS,
            "random_seed": RANDOM_SEED,
            "samples": config.samples,
            "warm_up_time_ns": nanos(config.warm_up_time),
            "measurement_time_ns": nanos(config.measurement_time),
            "store": {
                "engine": "RocksDB",
                "collection": "OrderedMap<u64, Vec<u8>>",
                "write_mode": "WAL enabled, sync=true"
            },
            "read_transactions": "read-only snapshots",
            "read_cache": "warm",
            "validation": "outside_timing",
            "execution": "single_thread",
            "scenarios": [
                "bulk_put_commit",
                "point_get",
                "ascending_scan",
                "descending_scan",
                "wide_scan",
                "station_step",
                "durable_hot_overwrite"
            ]
        },
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("serialize OrderedMap performance context"),
    )
    .expect("write OrderedMap performance context");
}

fn is_cargo_bench() -> bool {
    std::env::args_os().any(|argument| argument == "--bench")
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("benchmark duration fits u64 nanoseconds")
}
