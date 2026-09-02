//! CDC-oriented append, replay, projection, forwarding, fan-out, and GC scenarios.

use dogpaddle_bench_protocol::{BenchmarkProfile, Fields, Plan, Run};

mod support;

#[path = "append_log/fixture.rs"]
mod fixture;
#[path = "append_log/measure.rs"]
mod measure;
#[path = "append_log/oracle.rs"]
mod oracle;
#[path = "append_log/report.rs"]
mod report;

use fixture::{CdcRecord, FilterMode, LogFixture};
use measure::{
    measure_append, measure_append_body, measure_batch_append, measure_count_station,
    measure_decode_scan, measure_durable_append, measure_filter_station, measure_gc,
    measure_project_scan, measure_readers, measure_steady_window,
};
use oracle::{chunked_gc_transactions, make_records};
use report::{FrozenCases, LogCase, LogPair, report_log, report_log_mode_pair, report_log_pair};
use support::BenchRoot;

const BENCHMARK: &str = "append_log";
const DEFAULT_ENTRIES: usize = 10_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_RECORD_BYTES: &[usize] = &[128, 1_024, 8_192];
const DEFAULT_SOURCE_BATCH_ITEMS: &[usize] = &[1, 64, 1_024];
const DEFAULT_STATION_RECORD_BYTES: usize = 1_024;
const DEFAULT_STATION_BATCH_ITEMS: usize = 1_024;
const DEFAULT_GC_ITEMS: usize = 1_024;
const DEFAULT_READERS: &[usize] = &[1, 4];
const RECORD_HEADER_BYTES: usize = 16;
const SEED_BATCH_ITEMS: usize = 4_096;
const CURSOR_KEY: &[u8] = b"input/00000000/cursor";

#[derive(Clone, Copy)]
struct AppendConfiguration<'a> {
    entries: usize,
    commits: usize,
    samples: usize,
    record_sizes: &'a [usize],
    source_batches: &'a [usize],
    station_record_bytes: usize,
    station_batch_items: usize,
    gc_items: usize,
    readers: &'a [usize],
}

impl AppendConfiguration<'static> {
    const fn for_profile(profile: BenchmarkProfile) -> Self {
        match profile {
            BenchmarkProfile::Smoke => Self {
                entries: 3,
                commits: 1,
                samples: 1,
                record_sizes: &[16, 64],
                source_batches: &[1, 2],
                station_record_bytes: 16,
                station_batch_items: 1,
                gc_items: 1,
                readers: &[1, 2],
            },
            BenchmarkProfile::Reference => Self {
                entries: DEFAULT_ENTRIES,
                commits: DEFAULT_COMMITS,
                samples: DEFAULT_SAMPLES,
                record_sizes: DEFAULT_RECORD_BYTES,
                source_batches: DEFAULT_SOURCE_BATCH_ITEMS,
                station_record_bytes: DEFAULT_STATION_RECORD_BYTES,
                station_batch_items: DEFAULT_STATION_BATCH_ITEMS,
                gc_items: DEFAULT_GC_ITEMS,
                readers: DEFAULT_READERS,
            },
        }
    }
}

fn main() {
    let profile = BenchmarkProfile::from_environment();
    let config = AppendConfiguration::for_profile(profile);
    let AppendConfiguration {
        entries,
        commits,
        samples,
        record_sizes,
        source_batches,
        station_record_bytes,
        station_batch_items,
        gc_items,
        readers,
    } = config;

    assert!(entries > 0 && commits > 0 && samples > 0);
    assert!(station_batch_items > 0 && gc_items > 0);
    assert!(station_record_bytes >= RECORD_HEADER_BYTES);
    assert!(record_sizes.iter().all(|size| *size >= RECORD_HEADER_BYTES));
    assert!(source_batches.iter().all(|size| *size > 0));
    assert!(readers.iter().all(|count| *count > 0));
    let mut plan = Plan::new(profile, configuration_fields(&config));
    let mut cases = benchmark_plan(&mut plan, &config);
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    let bench_root = BenchRoot::new(&run);
    benchmark_record_widths(
        &mut run,
        &mut cases,
        &bench_root,
        entries,
        samples,
        station_batch_items,
        record_sizes,
    );
    benchmark_durable_source(&mut run, &mut cases, &bench_root, &config);
    benchmark_station_transactions(&mut run, &mut cases, &bench_root, &config);
    cases.finish();
    run.finish(|| {});
}

fn configuration_fields(config: &AppendConfiguration<'_>) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in [
        ("entries", config.entries),
        ("commits_cap", config.commits),
        ("samples", config.samples),
        ("station_record_bytes", config.station_record_bytes),
        ("station_batch_items", config.station_batch_items),
        ("gc_items", config.gc_items),
    ] {
        fields.insert(name, value);
    }
    fields.insert("record_bytes", config.record_sizes);
    fields.insert("source_batch_items", config.source_batches);
    fields.insert("readers", config.readers);
    fields
        .with("execution", "single_thread")
        .with("cache", "warm")
        .with("mdbx_sync_mode", "durable")
}

fn benchmark_plan(plan: &mut Plan, config: &AppendConfiguration<'_>) -> FrozenCases {
    let mut cases = FrozenCases::new();
    plan_record_widths(plan, &mut cases, config);
    plan_durable_source(plan, &mut cases, config);
    plan_station_transactions(plan, &mut cases, config);
    cases
}

fn plan_record_widths(plan: &mut Plan, cases: &mut FrozenCases, config: &AppendConfiguration<'_>) {
    for &record_bytes in config.record_sizes {
        cases.single(
            plan,
            LogCase::new(
                "bulk append pre-encoded, one tx",
                config.entries,
                record_bytes,
                1,
            ),
            config.samples,
        );
        cases.pair(
            plan,
            LogPair::variants(
                format!("record_bytes={record_bytes}"),
                LogCase::new(
                    "append scalar body, rollback",
                    config.entries,
                    record_bytes,
                    1,
                ),
                "append batch body, rollback",
            ),
            config.samples,
        );
        cases.pair(
            plan,
            LogPair::variants(
                format!("record_bytes={record_bytes}"),
                LogCase::new(
                    "append scalar, one durable tx",
                    config.entries,
                    record_bytes,
                    1,
                ),
                "append batch, one durable tx",
            ),
            config.samples,
        );
        cases.pair(
            plan,
            LogPair::modes(
                format!("scan decode record_bytes={record_bytes}"),
                LogCase::new("scan project diff", config.entries, record_bytes, 1),
                "scan full decode",
            ),
            config.samples,
        );
    }
}

fn plan_durable_source(plan: &mut Plan, cases: &mut FrozenCases, config: &AppendConfiguration<'_>) {
    for &batch_items in config.source_batches {
        let measured_entries = config
            .entries
            .min(config.commits.saturating_mul(batch_items));
        let transactions = measured_entries.div_ceil(batch_items);
        cases.single(
            plan,
            LogCase::new(
                format!("source append b{batch_items} ({transactions} tx)"),
                measured_entries,
                config.station_record_bytes,
                transactions,
            ),
            config.samples,
        );
    }
}

fn plan_station_transactions(
    plan: &mut Plan,
    cases: &mut FrozenCases,
    config: &AppendConfiguration<'_>,
) {
    let transactions = config.entries.div_ceil(config.station_batch_items);
    cases.single(
        plan,
        LogCase::new(
            format!("station count project ({transactions} tx)"),
            config.entries,
            config.station_record_bytes,
            transactions,
        ),
        config.samples,
    );
    cases.single(
        plan,
        LogCase::new(
            format!("station raw pass-through ({transactions} tx)"),
            config.entries,
            config.station_record_bytes,
            transactions,
        ),
        config.samples,
    );
    cases.pair(
        plan,
        LogPair::modes(
            format!(
                "station filter 50% record_bytes={}",
                config.station_record_bytes
            ),
            LogCase::new(
                format!("station filter 50% project ({transactions} tx)"),
                config.entries,
                config.station_record_bytes,
                transactions,
            ),
            format!("station filter 50% decode ({transactions} tx)"),
        ),
        config.samples,
    );
    let steady_transactions = config.entries.div_ceil(config.station_batch_items)
        + chunked_gc_transactions(config.entries, config.station_batch_items, config.gc_items);
    cases.single(
        plan,
        LogCase::new(
            format!("steady append + GC ({steady_transactions} tx)"),
            config.entries,
            config.station_record_bytes,
            steady_transactions,
        ),
        config.samples,
    );
    for &reader_count in config.readers {
        let deliveries = config
            .entries
            .checked_mul(reader_count)
            .expect("benchmark delivery count fits in usize");
        let reader_transactions = transactions
            .checked_mul(reader_count)
            .expect("benchmark transaction count fits in usize");
        cases.single(
            plan,
            LogCase::new(
                format!("downstream replay x{reader_count} ({reader_transactions} tx)"),
                deliveries,
                config.station_record_bytes,
                reader_transactions,
            ),
            config.samples,
        );
    }
    let gc_transactions = config.entries.div_ceil(config.gc_items);
    cases.single(
        plan,
        LogCase::new(
            format!("prefix GC b{} ({gc_transactions} tx)", config.gc_items),
            config.entries,
            config.station_record_bytes,
            gc_transactions,
        ),
        config.samples,
    );
}

fn benchmark_record_widths(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    entries: usize,
    samples: usize,
    scan_items: usize,
    record_sizes: &[usize],
) {
    for &record_bytes in record_sizes {
        let records = make_records(entries, record_bytes);
        let encoded = records.iter().map(CdcRecord::encode).collect::<Vec<_>>();

        report_log(
            run,
            plan,
            &LogCase::new("bulk append pre-encoded, one tx", entries, record_bytes, 1),
            samples,
            || measure_append(bench_root, &encoded, entries),
        );
        report_log_pair(
            run,
            plan,
            &LogPair::variants(
                format!("record_bytes={record_bytes}"),
                LogCase::new("append scalar body, rollback", entries, record_bytes, 1),
                "append batch body, rollback",
            ),
            samples,
            || measure_append_body(bench_root, &records, false),
            || measure_append_body(bench_root, &records, true),
        );
        report_log_pair(
            run,
            plan,
            &LogPair::variants(
                format!("record_bytes={record_bytes}"),
                LogCase::new("append scalar, one durable tx", entries, record_bytes, 1),
                "append batch, one durable tx",
            ),
            samples,
            || measure_append(bench_root, &records, entries),
            || measure_batch_append(bench_root, &records, entries),
        );

        let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, 0);
        report_log_mode_pair(
            run,
            plan,
            &LogPair::modes(
                format!("scan decode record_bytes={record_bytes}"),
                LogCase::new("scan project diff", entries, record_bytes, 1),
                "scan full decode",
            ),
            samples,
            |full| {
                if full {
                    measure_decode_scan(&mut fixture, entries, record_bytes, scan_items)
                } else {
                    measure_project_scan(&mut fixture, entries, record_bytes, scan_items)
                }
            },
        );
    }
}

fn benchmark_durable_source(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    config: &AppendConfiguration<'_>,
) {
    let entries = config.entries;
    let record_bytes = config.station_record_bytes;
    let records = make_records(entries, record_bytes);
    for &batch_items in config.source_batches {
        let measured_entries = entries.min(config.commits.saturating_mul(batch_items));
        let transactions = measured_entries.div_ceil(batch_items);
        report_log(
            run,
            plan,
            &LogCase::new(
                format!("source append b{batch_items} ({transactions} tx)"),
                measured_entries,
                record_bytes,
                transactions,
            ),
            config.samples,
            || {
                measure_durable_append(
                    bench_root,
                    &records[..measured_entries],
                    measured_entries,
                    batch_items,
                )
            },
        );
    }
}

fn benchmark_station_transactions(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    config: &AppendConfiguration<'_>,
) {
    let AppendConfiguration {
        entries,
        samples,
        station_record_bytes: record_bytes,
        station_batch_items: batch_items,
        gc_items,
        readers,
        ..
    } = *config;
    let transactions = entries.div_ceil(batch_items);
    benchmark_station_filters(run, plan, bench_root, config, transactions);

    let steady_transactions =
        entries.div_ceil(batch_items) + chunked_gc_transactions(entries, batch_items, gc_items);
    report_log(
        run,
        plan,
        &LogCase::new(
            format!("steady append + GC ({steady_transactions} tx)"),
            entries,
            record_bytes,
            steady_transactions,
        ),
        samples,
        || measure_steady_window(bench_root, entries, record_bytes, batch_items, gc_items),
    );

    for &reader_count in readers {
        let deliveries = entries
            .checked_mul(reader_count)
            .expect("benchmark delivery count fits in usize");
        let reader_transactions = transactions
            .checked_mul(reader_count)
            .expect("benchmark transaction count fits in usize");
        report_log(
            run,
            plan,
            &LogCase::new(
                format!("downstream replay x{reader_count} ({reader_transactions} tx)"),
                deliveries,
                record_bytes,
                reader_transactions,
            ),
            samples,
            || measure_readers(bench_root, entries, record_bytes, batch_items, reader_count),
        );
    }

    let gc_transactions = entries.div_ceil(gc_items);
    report_log(
        run,
        plan,
        &LogCase::new(
            format!("prefix GC b{gc_items} ({gc_transactions} tx)"),
            entries,
            record_bytes,
            gc_transactions,
        ),
        samples,
        || measure_gc(bench_root, entries, record_bytes, gc_items),
    );
}

fn benchmark_station_filters(
    run: &mut Run,
    plan: &mut FrozenCases,
    bench_root: &BenchRoot,
    config: &AppendConfiguration<'_>,
    transactions: usize,
) {
    let entries = config.entries;
    let samples = config.samples;
    let record_bytes = config.station_record_bytes;
    let batch_items = config.station_batch_items;
    report_log(
        run,
        plan,
        &LogCase::new(
            format!("station count project ({transactions} tx)"),
            entries,
            record_bytes,
            transactions,
        ),
        samples,
        || measure_count_station(bench_root, entries, record_bytes, batch_items),
    );
    report_log(
        run,
        plan,
        &LogCase::new(
            format!("station raw pass-through ({transactions} tx)"),
            entries,
            record_bytes,
            transactions,
        ),
        samples,
        || {
            measure_filter_station(
                bench_root,
                entries,
                record_bytes,
                batch_items,
                FilterMode::PassThrough,
            )
        },
    );
    let projected = format!("station filter 50% project ({transactions} tx)");
    let decoded = format!("station filter 50% decode ({transactions} tx)");
    report_log_mode_pair(
        run,
        plan,
        &LogPair::modes(
            format!("station filter 50% record_bytes={record_bytes}"),
            LogCase::new(projected, entries, record_bytes, transactions),
            decoded,
        ),
        samples,
        |decode| {
            measure_filter_station(
                bench_root,
                entries,
                record_bytes,
                batch_items,
                if decode {
                    FilterMode::DecodedHalf
                } else {
                    FilterMode::ProjectedHalf
                },
            )
        },
    );
}
