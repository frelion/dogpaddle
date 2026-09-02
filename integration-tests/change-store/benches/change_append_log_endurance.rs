#[path = "support/mod.rs"]
mod support;

use std::{collections::VecDeque, fs, num::NonZeroUsize, path::Path, time::Duration};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, ConfigurationRecord, ExtensionRecord, Fields, LatencySummary,
    require_benchmark_build,
};
use dogpaddle_change::encode_change;
use dogpaddle_change_store_integration::{order_checksum, wide_change};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

use support::{BenchStoreRoot, complete, decode_entry, emit_record};

const BENCHMARK: &str = "change_append_log_endurance";
const MODE: &str = "fixed_wide_window";
const MDBX_DATA_FILE: &str = "mdbx.dat";

#[derive(Clone, Copy)]
struct FileSize {
    logical: u64,
    allocated: u64,
}

struct Config {
    rows_per_change: usize,
    changes_per_cycle: usize,
    cycles: usize,
    payload_bytes: usize,
    retained_bytes: usize,
    truncate_items: NonZeroUsize,
    page_items: usize,
    page_bytes: usize,
    validation_interval: NonZeroUsize,
    max_working_set_bytes: usize,
    max_total_written_bytes: usize,
}

fn main() {
    require_benchmark_build(BENCHMARK);
    let stores = BenchStoreRoot::from_environment(BENCHMARK);
    let config = Config::for_profile(stores.profile());
    stores.emit_environment(BENCHMARK);
    emit_configuration(&config);
    run(&stores, &config);
    complete(BENCHMARK);
}

impl Config {
    fn for_profile(profile: BenchmarkProfile) -> Self {
        let config = match profile {
            BenchmarkProfile::Smoke => Self {
                rows_per_change: 8,
                changes_per_cycle: 2,
                cycles: 3,
                payload_bytes: 16,
                retained_bytes: 64 * 1_024,
                truncate_items: NonZeroUsize::new(8).unwrap(),
                page_items: 2,
                page_bytes: 1024 * 1024,
                validation_interval: NonZeroUsize::new(1).unwrap(),
                max_working_set_bytes: 64 * 1_024 * 1_024,
                max_total_written_bytes: 64 * 1_024 * 1_024,
            },
            BenchmarkProfile::Reference => Self {
                rows_per_change: 4_096,
                changes_per_cycle: 32,
                cycles: 500,
                payload_bytes: 1_024,
                retained_bytes: 512 * 1_024 * 1_024,
                truncate_items: NonZeroUsize::new(4_096).unwrap(),
                page_items: 16,
                page_bytes: 128 * 1_024 * 1_024,
                validation_interval: NonZeroUsize::new(25).unwrap(),
                max_working_set_bytes: 1_024 * 1_024 * 1_024,
                max_total_written_bytes: 1_024 * 1_024 * 1_024 * 1_024,
            },
        };
        assert!(
            config.retained_bytes <= config.max_working_set_bytes,
            "retained window exceeds working-set budget"
        );
        let payload = config
            .rows_per_change
            .checked_mul(config.payload_bytes)
            .expect("fixture payload size fits usize");
        assert!(
            i32::try_from(payload).is_ok(),
            "rows * payload bytes must fit Arrow Binary offsets"
        );
        config
    }
}

fn emit_configuration(config: &Config) {
    let fields = Fields::new()
        .with("workload_mode", MODE)
        .unwrap()
        .with("rows_per_change", config.rows_per_change)
        .unwrap()
        .with("changes_per_cycle", config.changes_per_cycle)
        .unwrap()
        .with("cycles", config.cycles)
        .unwrap()
        .with("payload_bytes", config.payload_bytes)
        .unwrap()
        .with("retained_target_bytes", config.retained_bytes)
        .unwrap()
        .with("truncate_items", config.truncate_items.get())
        .unwrap()
        .with("validation_page_items", config.page_items)
        .unwrap()
        .with("validation_page_bytes", config.page_bytes)
        .unwrap()
        .with(
            "validation_interval_cycles",
            config.validation_interval.get(),
        )
        .unwrap()
        .with("max_working_set_bytes", config.max_working_set_bytes)
        .unwrap()
        .with("max_total_written_bytes", config.max_total_written_bytes)
        .unwrap()
        .with("encoding", "outside_timing")
        .unwrap()
        .with("producer_timing", "begin_append_commit")
        .unwrap()
        .with("truncate_timing", "begin_bounded_truncate_commit")
        .unwrap()
        .with("validation", "paged_decode_exact_bytes_order_checksum")
        .unwrap();
    emit_record(
        &ConfigurationRecord::new(
            BENCHMARK,
            NonZeroUsize::new(config.cycles + 1).unwrap(),
            fields,
        )
        .unwrap(),
    );
}

#[allow(clippy::too_many_lines)]
fn run(stores: &BenchStoreRoot, config: &Config) {
    let sample = stores.sample("fixed-wide-window");
    let mut next_id = 0_u64;
    let seed = seed_window(config, &mut next_id);
    let seed_entries = seed.len();
    let mut retained_bytes = scan_charge(seed.iter());
    let mut total_written = seed.iter().map(Vec::len).sum::<usize>();
    assert!(total_written <= config.max_total_written_bytes);

    let mut store = Store::create(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(&seed)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);
    let mut expected = seed.into_iter().collect::<VecDeque<_>>();
    let seed_file = data_file_size(sample.path());
    let mut peak_file = seed_file;

    let mut head = 0_u64;
    let mut tail = u64::try_from(seed_entries).unwrap();
    let mut producer_durations = Vec::with_capacity(config.cycles);
    let mut truncate_durations = Vec::with_capacity(config.cycles);
    let mut final_checksum = validate_reopen(
        sample.path(),
        head,
        tail,
        &expected,
        config.page_items,
        config.page_bytes,
    );

    println!(
        "Change + AppendLog endurance: profile={} mode={MODE} seed_entries={} retained_target={} cycles={}",
        stores.profile(),
        seed_entries,
        config.retained_bytes,
        config.cycles
    );

    for cycle in 0..config.cycles {
        let batch = encoded_batch(config, &mut next_id);
        let pending_bytes = scan_charge(batch.iter());
        assert!(
            retained_bytes
                .checked_add(pending_bytes)
                .is_some_and(|bytes| bytes <= config.max_working_set_bytes),
            "live encoded fixture exceeds working-set budget"
        );
        total_written = total_written
            .checked_add(batch.iter().map(Vec::len).sum::<usize>())
            .expect("total written bytes fit usize");
        assert!(
            total_written <= config.max_total_written_bytes,
            "endurance run exceeds total-write budget"
        );

        let store = Store::open(sample.path()).unwrap();
        let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
        let mut transactions = store.into_transactions();
        let producer_started = std::time::Instant::now();
        {
            let transaction = transactions.begin().unwrap();
            log.access(transaction.access())
                .unwrap()
                .append_batch(&batch)
                .unwrap();
            transaction.commit().unwrap();
        }
        let producer = producer_started.elapsed();
        producer_durations.push(producer);
        peak_file = peak_file.max(data_file_size(sample.path()));

        for entry in batch {
            retained_bytes = retained_bytes
                .checked_add(entry.len() + size_of::<u64>())
                .expect("retained byte count fits usize");
            expected.push_back(entry);
            tail = tail.checked_add(1).expect("log tail fits u64");
        }
        let head_before = head;
        while retained_bytes > config.retained_bytes && expected.len() > 1 {
            let removed = expected.pop_front().expect("non-empty retained window");
            retained_bytes -= removed.len() + size_of::<u64>();
        }
        let target = tail
            .checked_sub(u64::try_from(expected.len()).unwrap())
            .expect("retained window starts before tail");
        assert!(target > head_before, "each cycle must exercise truncation");

        let truncate_started = std::time::Instant::now();
        {
            let transaction = transactions.begin().unwrap();
            let mut access = log.access(transaction.access()).unwrap();
            while head < target {
                head = access
                    .truncate_before(target, config.truncate_items)
                    .unwrap();
            }
            transaction.commit().unwrap();
        }
        let truncate = truncate_started.elapsed();
        truncate_durations.push(truncate);
        drop(transactions);
        let cycle_file = data_file_size(sample.path());
        peak_file = peak_file.max(cycle_file);

        let validated = (cycle + 1).is_multiple_of(config.validation_interval.get())
            || cycle + 1 == config.cycles;
        if validated {
            final_checksum = validate_reopen(
                sample.path(),
                head,
                tail,
                &expected,
                config.page_items,
                config.page_bytes,
            );
        }
        emit_cycle(
            cycle,
            producer,
            truncate,
            head_before,
            head,
            tail,
            retained_bytes,
            cycle_file,
            validated,
            final_checksum,
        );
    }

    final_checksum = validate_reopen(
        sample.path(),
        head,
        tail,
        &expected,
        config.page_items,
        config.page_bytes,
    );
    let final_file = data_file_size(sample.path());
    emit_summary(
        config,
        seed_entries,
        expected.len(),
        retained_bytes,
        total_written,
        seed_file,
        final_file,
        peak_file,
        final_checksum,
        &producer_durations,
        &truncate_durations,
    );
}

fn seed_window(config: &Config, next_id: &mut u64) -> Vec<Vec<u8>> {
    let mut entries = Vec::new();
    let mut retained = 0_usize;
    loop {
        let entry = encoded_change(config, *next_id);
        let charge = entry.len() + size_of::<u64>();
        if !entries.is_empty() && retained.checked_add(charge).unwrap() > config.retained_bytes {
            break;
        }
        retained = retained.checked_add(charge).unwrap();
        entries.push(entry);
        *next_id = next_id
            .checked_add(u64::try_from(config.rows_per_change).unwrap())
            .expect("fixture ID fits u64");
        if retained >= config.retained_bytes {
            break;
        }
    }
    assert!(!entries.is_empty());
    assert!(retained <= config.max_working_set_bytes);
    entries
}

fn encoded_batch(config: &Config, next_id: &mut u64) -> Vec<Vec<u8>> {
    (0..config.changes_per_cycle)
        .map(|_| {
            let encoded = encoded_change(config, *next_id);
            *next_id = next_id
                .checked_add(u64::try_from(config.rows_per_change).unwrap())
                .expect("fixture ID fits u64");
            encoded
        })
        .collect()
}

fn encoded_change(config: &Config, start: u64) -> Vec<u8> {
    encode_change(&wide_change(
        start,
        config.rows_per_change,
        config.payload_bytes,
    ))
    .expect("encode fixed-wide endurance Change")
}

fn validate_reopen(
    path: &Path,
    head: u64,
    tail: u64,
    expected: &VecDeque<Vec<u8>>,
    page_items: usize,
    page_bytes: usize,
) -> u64 {
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(access.bounds().unwrap(), head..tail);
    assert_eq!(
        access.retained_bytes().unwrap(),
        u64::try_from(scan_charge(expected.iter())).unwrap()
    );
    let mut offset = head;
    let mut actual_index = 0_usize;
    while offset < tail {
        let progress = access
            .scan(
                offset,
                ScanLimit::new(page_items, page_bytes).unwrap(),
                |entry| {
                    entry.project(decode_entry)?;
                    entry.project(|bytes| {
                        assert_eq!(bytes, expected[actual_index]);
                        Ok(())
                    })?;
                    actual_index = actual_index.checked_add(1).unwrap();
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up || progress.next_offset > offset);
        offset = progress.next_offset;
    }
    assert_eq!(actual_index, expected.len());
    order_checksum(expected)
}

#[allow(clippy::too_many_arguments)]
fn emit_cycle(
    cycle: usize,
    producer: Duration,
    truncate: Duration,
    head_before: u64,
    head: u64,
    tail: u64,
    retained_bytes: usize,
    file_size: FileSize,
    validated: bool,
    checksum: u64,
) {
    let fields = Fields::new()
        .with("workload_mode", MODE)
        .unwrap()
        .with("cycle", cycle)
        .unwrap()
        .with("producer_ns", producer.as_nanos())
        .unwrap()
        .with("truncate_ns", truncate.as_nanos())
        .unwrap()
        .with("head_before", head_before)
        .unwrap()
        .with("head", head)
        .unwrap()
        .with("tail", tail)
        .unwrap()
        .with("retained_bytes", retained_bytes)
        .unwrap()
        .with("file_logical_bytes", file_size.logical)
        .unwrap()
        .with("file_allocated_bytes", file_size.allocated)
        .unwrap()
        .with("reopened", true)
        .unwrap()
        .with("validated", validated)
        .unwrap()
        .with("validation_checksum", checksum)
        .unwrap();
    emit_record(&ExtensionRecord::new("cycle_sample", BENCHMARK, fields).unwrap());
}

#[allow(clippy::too_many_arguments)]
fn emit_summary(
    config: &Config,
    seed_entries: usize,
    retained_entries: usize,
    retained_bytes: usize,
    total_written: usize,
    seed_file: FileSize,
    final_file: FileSize,
    peak_file: FileSize,
    checksum: u64,
    producer_samples: &[Duration],
    truncate_samples: &[Duration],
) {
    let producer = LatencySummary::from_samples(producer_samples).unwrap();
    let truncate = LatencySummary::from_samples(truncate_samples).unwrap();
    let mut fields = Fields::new()
        .with("workload_mode", MODE)
        .unwrap()
        .with("cycles", config.cycles)
        .unwrap()
        .with("rows_per_change", config.rows_per_change)
        .unwrap()
        .with("changes_per_cycle", config.changes_per_cycle)
        .unwrap()
        .with("payload_bytes", config.payload_bytes)
        .unwrap()
        .with("retained_target_bytes", config.retained_bytes)
        .unwrap()
        .with("seed_entries", seed_entries)
        .unwrap()
        .with("retained_entries", retained_entries)
        .unwrap()
        .with("retained_bytes", retained_bytes)
        .unwrap()
        .with("actual_written_bytes", total_written)
        .unwrap()
        .with("seed_file_logical_bytes", seed_file.logical)
        .unwrap()
        .with("seed_file_allocated_bytes", seed_file.allocated)
        .unwrap()
        .with("final_file_logical_bytes", final_file.logical)
        .unwrap()
        .with("final_file_allocated_bytes", final_file.allocated)
        .unwrap()
        .with("peak_file_logical_bytes", peak_file.logical)
        .unwrap()
        .with("peak_file_allocated_bytes", peak_file.allocated)
        .unwrap()
        .with("reopens", config.cycles)
        .unwrap()
        .with("validation_checksum", checksum)
        .unwrap();
    insert_latency(&mut fields, "producer", producer);
    insert_latency(&mut fields, "truncate", truncate);
    emit_record(&ExtensionRecord::new("endurance_summary", BENCHMARK, fields).unwrap());
    println!(
        "{MODE}: retained_entries={retained_entries} retained_bytes={retained_bytes} written={total_written} checksum={checksum:#018x}"
    );
}

fn insert_latency(fields: &mut Fields, prefix: &str, summary: LatencySummary) {
    for (suffix, value) in [
        ("p50_ns", summary.p50().as_nanos()),
        ("p95_ns", summary.p95().as_nanos()),
        ("p99_ns", summary.p99().as_nanos()),
        ("max_ns", summary.max().as_nanos()),
    ] {
        fields
            .insert(format!("{prefix}_{suffix}"), value)
            .expect("encode latency summary");
    }
}

fn scan_charge<'a>(entries: impl Iterator<Item = &'a Vec<u8>>) -> usize {
    entries.map(|entry| entry.len() + size_of::<u64>()).sum()
}

impl FileSize {
    fn max(self, other: Self) -> Self {
        Self {
            logical: self.logical.max(other.logical),
            allocated: self.allocated.max(other.allocated),
        }
    }
}

fn data_file_size(store_path: &Path) -> FileSize {
    let metadata = fs::metadata(store_path.join(MDBX_DATA_FILE))
        .expect("read endurance MDBX data-file metadata");
    FileSize {
        logical: metadata.len(),
        allocated: allocated_bytes(&metadata),
    }
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}
