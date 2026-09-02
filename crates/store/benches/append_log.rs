//! CDC-oriented append, replay, projection, forwarding, fan-out, and GC scenarios.

use std::num::NonZeroUsize;

use dogpaddle_bench_protocol::{BenchmarkProfile, ConfigurationRecord, Fields};

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
use report::{
    LogCase, LogPair, print_log_section, report_log, report_log_mode_pair, report_log_pair,
};
use support::{BenchRoot, complete, initialize, write_record};

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
const MEBIBYTE_BYTES: u128 = 1_048_576;
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
    let bench_root = initialize(BENCHMARK);
    let config = AppendConfiguration::for_profile(bench_root.profile());
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
    emit_configuration(&config);

    println!("DogPaddle AppendLog benchmark");
    println!(
        "entries={entries} commits_cap={commits} samples={samples} record_bytes={record_sizes:?}"
    );
    println!(
        "source_batch_items={source_batches:?} station_record_bytes={station_record_bytes} station_batch_items={station_batch_items} gc_items={gc_items} readers={readers:?}"
    );
    println!(
        "sync=durable execution=single-thread cache=warm seed=outside-timing validation=outside-timing"
    );
    print_log_section(
        "AppendLog<T>: encoded width and read strategy",
        "one durable bulk append transaction; warm scans stay in one transaction",
    );
    benchmark_record_widths(
        &bench_root,
        entries,
        samples,
        station_batch_items,
        record_sizes,
    );
    print_log_section(
        "AppendLog<T>: Source commit amortization",
        "each batch is one begin -> append -> durable commit transaction",
    );
    benchmark_durable_source(
        &bench_root,
        entries,
        commits,
        samples,
        station_record_bytes,
        source_batches,
    );
    print_log_section(
        "AppendLog<T>: Station, fan-out, and GC",
        "Station state cursor, log work, output/state writes, and durable commit are timed together",
    );
    benchmark_station_transactions(
        &bench_root,
        entries,
        samples,
        station_record_bytes,
        station_batch_items,
        gc_items,
        readers,
    );
    complete(BENCHMARK);
}

fn emit_configuration(config: &AppendConfiguration<'_>) {
    let mut fields = Fields::new();
    for (name, value) in [
        ("entries", config.entries),
        ("commits_cap", config.commits),
        ("samples", config.samples),
        ("station_record_bytes", config.station_record_bytes),
        ("station_batch_items", config.station_batch_items),
        ("gc_items", config.gc_items),
    ] {
        fields
            .insert(name, value)
            .expect("construct AppendLog configuration fields");
    }
    fields
        .insert("record_bytes", config.record_sizes)
        .expect("construct AppendLog record-size field");
    fields
        .insert("source_batch_items", config.source_batches)
        .expect("construct AppendLog batch-size field");
    fields
        .insert("readers", config.readers)
        .expect("construct AppendLog reader field");
    let samples_and_summary = config.samples + 1;
    let records = config.record_sizes.len() * (7 * config.samples + 10)
        + config.source_batches.len() * samples_and_summary
        + (4 + config.readers.len()) * samples_and_summary
        + (2 * config.samples + 3);
    let record = ConfigurationRecord::new(BENCHMARK, NonZeroUsize::new(records).unwrap(), fields)
        .expect("construct AppendLog configuration record");
    write_record(&record);
}

fn benchmark_record_widths(
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
            &LogCase::new("bulk append pre-encoded, one tx", entries, record_bytes, 1),
            samples,
            || measure_append(bench_root, &encoded, entries),
        );
        report_log_pair(
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
    bench_root: &BenchRoot,
    entries: usize,
    commits: usize,
    samples: usize,
    record_bytes: usize,
    batches: &[usize],
) {
    let records = make_records(entries, record_bytes);
    for &batch_items in batches {
        let measured_entries = entries.min(commits.saturating_mul(batch_items));
        let transactions = measured_entries.div_ceil(batch_items);
        report_log(
            &LogCase::new(
                format!("source append b{batch_items} ({transactions} tx)"),
                measured_entries,
                record_bytes,
                transactions,
            ),
            samples,
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
    bench_root: &BenchRoot,
    entries: usize,
    samples: usize,
    record_bytes: usize,
    batch_items: usize,
    gc_items: usize,
    readers: &[usize],
) {
    let transactions = entries.div_ceil(batch_items);
    report_log(
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

    let steady_transactions =
        entries.div_ceil(batch_items) + chunked_gc_transactions(entries, batch_items, gc_items);
    report_log(
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
