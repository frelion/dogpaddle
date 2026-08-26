use std::{
    collections::VecDeque,
    fs,
    hint::black_box,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use dogpaddle_bench_protocol::{
    ConfigurationRecord, ExtensionRecord, Fields, LatencySummary, require_benchmark_build, string,
};
use dogpaddle_change::{decode_change, encode_change};
use dogpaddle_change_store_integration::{assert_change_eq, checksum_change, wide_change};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError};

#[path = "support/mod.rs"]
mod support;

use support::{BenchStoreRoot, decode_entry, emit_host_environment, emit_record, setting};

const MDBX_DATA_FILE: &str = "mdbx.dat";
const DEFAULT_MAX_WORKING_SET_BYTES: usize = 1_073_741_824;
const DEFAULT_MAX_TOTAL_WRITTEN_BYTES: usize = 1_099_511_627_776;

macro_rules! json_fields {
    ($($name:literal => $value:expr),+ $(,)?) => {{
        let mut fields = Fields::new();
        $(
            fields
                .insert($name, $value)
                .expect(concat!("encode ", $name));
        )+
        fields
    }};
}

struct Config {
    profile: String,
    rows_per_change: usize,
    changes_per_cycle: usize,
    cycles: usize,
    payload_bytes: usize,
    retained_encoded_bytes: usize,
    truncate_items: NonZeroUsize,
    max_working_set_bytes: usize,
    max_total_written_bytes: usize,
}

#[derive(Clone, Copy)]
struct FileSize {
    logical: u64,
    allocated: u64,
}

#[derive(Default)]
struct FilePeaks {
    logical: u64,
    allocated: u64,
}

struct RetainedEntry {
    offset: u64,
    event_start: u64,
    encoded_len: usize,
    checksum: u64,
}

struct WorkloadPlan {
    encoded_bytes_per_change: usize,
    cycle_encoded_bytes: usize,
    seed_entries: usize,
    seed_encoded_bytes: usize,
    measured_changes: usize,
    measured_rows: usize,
    measured_encoded_bytes: usize,
    estimated_working_set_bytes: usize,
    total_written_bytes: usize,
}

struct PreparedBatch {
    metadata: Vec<(u64, u64)>,
    encoded: Vec<Vec<u8>>,
    encoded_bytes: usize,
}

struct CycleSample {
    cycle: usize,
    append_duration: Duration,
    truncate_duration: Duration,
    head_before: u64,
    target: u64,
    tail: u64,
    removed_entries: usize,
    removed_bytes: usize,
    retained_entries: usize,
    retained_encoded_bytes: usize,
    append_file: FileSize,
    truncate_file: FileSize,
}

struct EnduranceSummary<'summary> {
    plan: &'summary WorkloadPlan,
    actual_written_bytes: usize,
    protocol_elapsed: Duration,
    wall_elapsed: Duration,
    changes_per_second: u128,
    rows_per_second: u128,
    encoded_bytes_per_second: u128,
    append: LatencySummary,
    truncate: LatencySummary,
    initial_file: FileSize,
    seed_file: FileSize,
    final_file: FileSize,
    reopened_file: FileSize,
    file_peaks: &'summary FilePeaks,
    allocated_amplification_hundredths: u128,
    validation_checksum: u64,
}

fn emit_configuration(config: &Config, plan: &WorkloadPlan) {
    let fields = json_fields! {
        "endurance_profile" => &config.profile,
        "rows_per_change" => config.rows_per_change,
        "changes_per_cycle" => config.changes_per_cycle,
        "cycles" => config.cycles,
        "payload_bytes" => config.payload_bytes,
        "encoded_bytes_per_change" => plan.encoded_bytes_per_change,
        "seed_entries" => plan.seed_entries,
        "seed_encoded_bytes" => plan.seed_encoded_bytes,
        "retained_encoded_bytes" => config.retained_encoded_bytes,
        "truncate_items" => config.truncate_items.get(),
        "max_working_set_bytes" => config.max_working_set_bytes,
        "estimated_working_set_bytes" => plan.estimated_working_set_bytes,
        "max_total_written_bytes" => config.max_total_written_bytes,
        "estimated_total_written_bytes" => plan.total_written_bytes,
    };
    emit_record(
        &ConfigurationRecord::new("change_append_log_endurance", fields)
            .expect("build endurance configuration record"),
    );
}

fn emit_cycle_sample(sample: &CycleSample) {
    let fields = json_fields! {
        "cycle" => sample.cycle,
        "append_ns" => sample.append_duration.as_nanos(),
        "truncate_ns" => sample.truncate_duration.as_nanos(),
        "head_before" => sample.head_before,
        "target" => sample.target,
        "tail" => sample.tail,
        "removed_entries" => sample.removed_entries,
        "removed_bytes" => sample.removed_bytes,
        "retained_entries" => sample.retained_entries,
        "retained_encoded_bytes" => sample.retained_encoded_bytes,
        "append_file_logical_bytes" => sample.append_file.logical,
        "append_file_allocated_bytes" => sample.append_file.allocated,
        "truncate_file_logical_bytes" => sample.truncate_file.logical,
        "truncate_file_allocated_bytes" => sample.truncate_file.allocated,
    };
    emit_record(
        &ExtensionRecord::new("cycle_sample", "change_append_log_endurance", fields)
            .expect("build endurance sample record"),
    );
}

fn emit_endurance_summary(summary: &EnduranceSummary<'_>) {
    let mut fields = json_fields! {
        "seed_entries" => summary.plan.seed_entries,
        "seed_encoded_bytes" => summary.plan.seed_encoded_bytes,
        "measured_changes" => summary.plan.measured_changes,
        "measured_rows" => summary.plan.measured_rows,
        "measured_encoded_bytes" => summary.plan.measured_encoded_bytes,
        "actual_written_bytes" => summary.actual_written_bytes,
        "protocol_ns" => summary.protocol_elapsed.as_nanos(),
        "wall_ns" => summary.wall_elapsed.as_nanos(),
        "changes_per_second" => summary.changes_per_second,
        "rows_per_second" => summary.rows_per_second,
        "encoded_bytes_per_second" => summary.encoded_bytes_per_second,
    };
    insert_latency_fields(&mut fields, "append", summary.append);
    insert_latency_fields(&mut fields, "truncate", summary.truncate);
    insert_file_fields(&mut fields, "initial", summary.initial_file);
    insert_file_fields(&mut fields, "seed", summary.seed_file);
    insert_file_fields(&mut fields, "final", summary.final_file);
    insert_file_fields(&mut fields, "reopened", summary.reopened_file);
    insert_file_fields(
        &mut fields,
        "peak",
        FileSize {
            logical: summary.file_peaks.logical,
            allocated: summary.file_peaks.allocated,
        },
    );
    fields
        .insert(
            "allocated_amplification_hundredths",
            summary.allocated_amplification_hundredths,
        )
        .expect("encode allocation amplification");
    fields
        .insert(
            "validation_checksum",
            format!("{:#018x}", summary.validation_checksum),
        )
        .expect("encode validation checksum");
    emit_record(
        &ExtensionRecord::new("endurance_summary", "change_append_log_endurance", fields)
            .expect("build endurance summary record"),
    );
}

fn insert_latency_fields(fields: &mut Fields, prefix: &str, latency: LatencySummary) {
    for (suffix, value) in [
        ("p50_ns", latency.p50().as_nanos()),
        ("p95_ns", latency.p95().as_nanos()),
        ("p99_ns", latency.p99().as_nanos()),
        ("max_ns", latency.max().as_nanos()),
    ] {
        fields
            .insert(format!("{prefix}_{suffix}"), value)
            .expect("encode endurance latency");
    }
}

fn insert_file_fields(fields: &mut Fields, prefix: &str, size: FileSize) {
    fields
        .insert(format!("{prefix}_file_logical_bytes"), size.logical)
        .expect("encode logical file bytes");
    fields
        .insert(format!("{prefix}_file_allocated_bytes"), size.allocated)
        .expect("encode allocated file bytes");
}

fn main() {
    require_benchmark_build("change_append_log_endurance");

    let config = config();
    let batch_working_set_bytes = preflight_dimensions(&config);
    let representative = encode_change(&wide_change(
        0,
        config.rows_per_change,
        config.payload_bytes,
    ))
    .expect("encode representative endurance Change");
    let plan = WorkloadPlan::new(&config, representative.len(), batch_working_set_bytes);
    drop(representative);

    let stores = BenchStoreRoot::from_environment();
    if config.profile == "full" {
        assert_eq!(
            stores.profile(),
            "reference",
            "the full endurance workload requires DOGPADDLE_CHANGE_STORE_BENCH_PROFILE=reference and DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR"
        );
    }
    let sample_store = stores.sample("change-append-log-endurance");
    emit_host_environment(&stores, "change_append_log_endurance");
    emit_configuration(&config, &plan);
    println!(
        "Change + AppendLog endurance: profile={} rows/change={} changes/cycle={} cycles={} payload_bytes={} retained_encoded_bytes={} truncate_items={}",
        config.profile,
        config.rows_per_change,
        config.changes_per_cycle,
        config.cycles,
        config.payload_bytes,
        config.retained_encoded_bytes,
        config.truncate_items
    );
    println!(
        "estimated: encoded/change={} seed_entries={} seed_encoded={} cycle_encoded={} working_set={} total_written={} max_working_set={} max_total_written={}",
        bytes_usize(plan.encoded_bytes_per_change),
        plan.seed_entries,
        bytes_usize(plan.seed_encoded_bytes),
        bytes_usize(plan.cycle_encoded_bytes),
        bytes_usize(plan.estimated_working_set_bytes),
        bytes_usize(plan.total_written_bytes),
        bytes_usize(config.max_working_set_bytes),
        bytes_usize(config.max_total_written_bytes)
    );
    println!(
        "environment: store_path={} filesystem_base={} mdbx_sync_mode=durable",
        sample_store.path().display(),
        stores.base().display()
    );
    println!(
        "controls: DOGPADDLE_CHANGE_STORE_BENCH_PROFILE/_STORE_DIR select the filesystem; DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE plus _ROWS_PER_CHANGE, _CHANGES_PER_CYCLE, _CYCLES, _PAYLOAD_BYTES, _RETAINED_BYTES, _TRUNCATE_ITEMS, _MAX_WORKING_SET_BYTES, _MAX_TOTAL_WRITTEN_BYTES select the workload"
    );

    run(&config, &plan, sample_store.path());
}

#[expect(
    clippy::too_many_lines,
    reason = "the endurance protocol is kept linear so timing and durability boundaries remain auditable"
)]
fn run(config: &Config, plan: &WorkloadPlan, store_path: &Path) {
    let mut store = Store::create(store_path).expect("create endurance Store");
    let log: AppendLog<Vec<u8>> = store.create_data("changes").expect("create endurance log");
    let initial_file = data_file_size(store_path);
    let mut transactions = store.into_transactions();
    let mut retained = VecDeque::<RetainedEntry>::new();
    let mut retained_bytes = 0_usize;
    let mut next_offset = 0_u64;
    let mut next_event = 0_u64;
    let mut actual_written_bytes = 0_usize;
    let mut append_durations = Vec::with_capacity(config.cycles);
    let mut truncate_durations = Vec::with_capacity(config.cycles);
    let mut file_peaks = FilePeaks::default();
    file_peaks.observe(initial_file);

    let mut remaining_seed = plan.seed_entries;
    while remaining_seed > 0 {
        let entries = remaining_seed.min(config.changes_per_cycle);
        let batch = prepare_batch(
            config,
            entries,
            &mut next_event,
            plan.encoded_bytes_per_change,
        );
        actual_written_bytes = actual_written_bytes
            .checked_add(batch.encoded_bytes)
            .expect("actual written byte count fits usize");
        assert!(actual_written_bytes <= config.max_total_written_bytes);
        let expected_start = next_offset;
        let expected_end = expected_start
            .checked_add(u64::try_from(entries).expect("seed entry count fits u64"))
            .expect("seed offset fits u64");
        let transaction = transactions.begin().expect("begin endurance seed");
        let assigned = log
            .access(transaction.access())
            .expect("access endurance seed log")
            .append_batch(&batch.encoded)
            .expect("append endurance seed Changes");
        transaction.commit().expect("durably commit endurance seed");
        assert_eq!(assigned, expected_start..expected_end);
        remember_batch(batch, &mut retained, &mut retained_bytes, &mut next_offset);
        remaining_seed -= entries;
    }
    assert_eq!(retained.len(), plan.seed_entries);
    assert_eq!(retained_bytes, plan.seed_encoded_bytes);
    assert!(retained_bytes <= config.retained_encoded_bytes);
    let seed_file = data_file_size(store_path);
    file_peaks.observe(seed_file);

    let wall_started = Instant::now();
    for cycle in 0..config.cycles {
        let head_before = retained
            .front()
            .expect("seeded endurance window is non-empty")
            .offset;
        let batch = prepare_batch(
            config,
            config.changes_per_cycle,
            &mut next_event,
            plan.encoded_bytes_per_change,
        );
        assert_eq!(batch.encoded_bytes, plan.cycle_encoded_bytes);
        actual_written_bytes = actual_written_bytes
            .checked_add(batch.encoded_bytes)
            .expect("actual written byte count fits usize");
        assert!(
            actual_written_bytes <= config.max_total_written_bytes,
            "actual encoded writes exceeded the configured total budget"
        );

        let started = Instant::now();
        let transaction = transactions.begin().expect("begin endurance append");
        let assigned = log
            .access(transaction.access())
            .expect("access endurance append log")
            .append_batch(&batch.encoded)
            .expect("append endurance Changes");
        transaction
            .commit()
            .expect("durably commit endurance append");
        let append_duration = started.elapsed();
        append_durations.push(append_duration);
        let append_file = data_file_size(store_path);
        file_peaks.observe(append_file);

        let expected_end = next_offset
            .checked_add(u64::try_from(config.changes_per_cycle).expect("entry count fits u64"))
            .expect("endurance offset fits u64");
        assert_eq!(assigned, next_offset..expected_end);
        remember_batch(batch, &mut retained, &mut retained_bytes, &mut next_offset);

        let mut removed_entries = 0_usize;
        let mut removed_bytes = 0_usize;
        while retained_bytes > config.retained_encoded_bytes {
            let removed = retained.pop_front().expect("non-empty retained queue");
            retained_bytes -= removed.encoded_len;
            removed_entries = removed_entries
                .checked_add(1)
                .expect("removed entry count fits usize");
            removed_bytes = removed_bytes
                .checked_add(removed.encoded_len)
                .expect("removed byte count fits usize");
        }
        assert_eq!(
            removed_entries, config.changes_per_cycle,
            "every measured cycle must replace exactly one appended cycle"
        );
        assert_eq!(removed_bytes, plan.cycle_encoded_bytes);
        assert_eq!(retained.len(), plan.seed_entries);
        assert_eq!(retained_bytes, plan.seed_encoded_bytes);
        let target = retained
            .front()
            .expect("endurance keeps at least one entry")
            .offset;
        assert!(target > head_before);

        let started = Instant::now();
        let transaction = transactions.begin().expect("begin endurance truncate");
        let mut access = log
            .access(transaction.access())
            .expect("access endurance truncate log");
        let mut head = head_before;
        while head < target {
            head = access
                .truncate_before(target, config.truncate_items)
                .expect("truncate endurance prefix");
        }
        transaction
            .commit()
            .expect("durably commit endurance truncation");
        let truncate_duration = started.elapsed();
        assert_eq!(head, target);
        {
            let transaction = transactions
                .begin()
                .expect("begin post-truncate validation");
            let access = log
                .access(transaction.access())
                .expect("access post-truncate validation log");
            assert_eq!(
                access.bounds().expect("read post-truncate bounds"),
                target..next_offset
            );
        }
        truncate_durations.push(truncate_duration);
        let file = data_file_size(store_path);
        file_peaks.observe(file);
        emit_cycle_sample(&CycleSample {
            cycle,
            append_duration,
            truncate_duration,
            head_before,
            target,
            tail: next_offset,
            removed_entries,
            removed_bytes,
            retained_entries: retained.len(),
            retained_encoded_bytes: retained_bytes,
            append_file,
            truncate_file: file,
        });
    }
    let wall_elapsed = wall_started.elapsed();
    let protocol_elapsed = total_duration(&append_durations)
        .checked_add(total_duration(&truncate_durations))
        .expect("protocol duration fits Duration");
    assert!(!protocol_elapsed.is_zero());
    assert_eq!(actual_written_bytes, plan.total_written_bytes);
    drop(transactions);

    let final_file = data_file_size(store_path);
    let validation_checksum = verify_after_reopen(store_path, &retained, retained_bytes, config);
    let reopened_file = data_file_size(store_path);
    file_peaks.observe(final_file);
    file_peaks.observe(reopened_file);

    let append = LatencySummary::from_samples(&append_durations).expect("summarize append latency");
    let truncate =
        LatencySummary::from_samples(&truncate_durations).expect("summarize truncate latency");
    let rows_per_second = rate_per_second(plan.measured_rows, protocol_elapsed);
    let changes_per_second = rate_per_second(plan.measured_changes, protocol_elapsed);
    let encoded_bytes_per_second = rate_per_second(plan.measured_encoded_bytes, protocol_elapsed);
    let encoded_mib_hundredths_per_second = encoded_bytes_per_second
        .checked_mul(100)
        .expect("encoded MiB/s numerator fits u128")
        / 1_048_576;
    let allocated_amplification_hundredths = if retained_bytes == 0 {
        0
    } else {
        u128::from(reopened_file.allocated)
            .checked_mul(100)
            .expect("file amplification numerator fits u128")
            / u128::try_from(retained_bytes).expect("retained bytes fit u128")
    };

    black_box(validation_checksum);
    emit_endurance_summary(&EnduranceSummary {
        plan,
        actual_written_bytes,
        protocol_elapsed,
        wall_elapsed,
        changes_per_second,
        rows_per_second,
        encoded_bytes_per_second,
        append,
        truncate,
        initial_file,
        seed_file,
        final_file,
        reopened_file,
        file_peaks: &file_peaks,
        allocated_amplification_hundredths,
        validation_checksum,
    });
    println!(
        "completed: seed_entries={} measured_changes={} measured_rows={} measured_encoded_bytes={} retained_entries={} retained_encoded_bytes={} protocol={} wall={} changes/s={changes_per_second} rows/s={rows_per_second} encoded_MiB/s={}.{:02}",
        plan.seed_entries,
        plan.measured_changes,
        plan.measured_rows,
        plan.measured_encoded_bytes,
        retained.len(),
        retained_bytes,
        duration(protocol_elapsed),
        duration(wall_elapsed),
        encoded_mib_hundredths_per_second / 100,
        encoded_mib_hundredths_per_second % 100,
    );
    println!(
        "  append tx   p50={} p95={} p99={} max={}",
        duration(append.p50()),
        duration(append.p95()),
        duration(append.p99()),
        duration(append.max())
    );
    println!(
        "  truncate tx p50={} p95={} p99={} max={}",
        duration(truncate.p50()),
        duration(truncate.p95()),
        duration(truncate.p99()),
        duration(truncate.max())
    );
    println!(
        "  file initial(logical/allocated)={}/{} seed={}/{} final={}/{} reopened={}/{} peak={}/{} allocated_amplification={}.{:02}x",
        bytes(initial_file.logical),
        bytes(initial_file.allocated),
        bytes(seed_file.logical),
        bytes(seed_file.allocated),
        bytes(final_file.logical),
        bytes(final_file.allocated),
        bytes(reopened_file.logical),
        bytes(reopened_file.allocated),
        bytes(file_peaks.logical),
        bytes(file_peaks.allocated),
        allocated_amplification_hundredths / 100,
        allocated_amplification_hundredths % 100
    );
    println!(
        "  validation=reopen+raw-byte-equality+full-decode checksum={validation_checksum:#018x}"
    );
}

fn config() -> Config {
    let profile = string("DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE", "smoke")
        .expect("load Change + Store endurance workload profile");
    let defaults = match profile.as_str() {
        "smoke" => (256, 8, 16, 128, 4 * 1024 * 1024, 64),
        "full" => (4_096, 32, 500, 1_024, 512 * 1024 * 1024, 4_096),
        _ => panic!("DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE must be smoke or full"),
    };
    Config {
        profile,
        rows_per_change: setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_ROWS_PER_CHANGE",
            defaults.0,
        ),
        changes_per_cycle: setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_CHANGES_PER_CYCLE",
            defaults.1,
        ),
        cycles: setting("DOGPADDLE_CHANGE_STORE_ENDURANCE_CYCLES", defaults.2),
        payload_bytes: setting("DOGPADDLE_CHANGE_STORE_ENDURANCE_PAYLOAD_BYTES", defaults.3),
        retained_encoded_bytes: setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_RETAINED_BYTES",
            defaults.4,
        ),
        truncate_items: NonZeroUsize::new(setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_TRUNCATE_ITEMS",
            defaults.5,
        ))
        .expect("setting rejects zero"),
        max_working_set_bytes: setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES",
            DEFAULT_MAX_WORKING_SET_BYTES,
        ),
        max_total_written_bytes: setting(
            "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_TOTAL_WRITTEN_BYTES",
            DEFAULT_MAX_TOTAL_WRITTEN_BYTES,
        ),
    }
}

impl WorkloadPlan {
    fn new(
        config: &Config,
        encoded_bytes_per_change: usize,
        batch_working_set_bytes: usize,
    ) -> Self {
        assert!(encoded_bytes_per_change > 0);
        assert!(
            config.retained_encoded_bytes >= encoded_bytes_per_change,
            "retained encoded-byte target must hold at least one complete Change entry"
        );
        let cycle_encoded_bytes = encoded_bytes_per_change
            .checked_mul(config.changes_per_cycle)
            .expect("cycle encoded bytes fit usize");
        assert!(
            cycle_encoded_bytes <= config.max_working_set_bytes,
            "one encoded cycle requires {cycle_encoded_bytes} bytes, above the configured {} byte working-set budget",
            config.max_working_set_bytes
        );
        let seed_entries = config.retained_encoded_bytes / encoded_bytes_per_change;
        let seed_encoded_bytes = seed_entries
            .checked_mul(encoded_bytes_per_change)
            .expect("seed encoded bytes fit usize");
        let seed_metadata_bytes = seed_entries
            .checked_mul(size_of::<RetainedEntry>())
            .and_then(|value| value.checked_mul(2))
            .expect("seed metadata bytes fit usize");
        let latency_sample_bytes = config
            .cycles
            .checked_mul(size_of::<Duration>())
            .and_then(|value| value.checked_mul(2))
            .expect("latency sample bytes fit usize");
        let estimated_working_set_bytes = batch_working_set_bytes
            .checked_add(seed_metadata_bytes)
            .and_then(|value| value.checked_add(latency_sample_bytes))
            .expect("total working-set estimate fits usize");
        assert!(
            estimated_working_set_bytes <= config.max_working_set_bytes,
            "combined prepared batch, retained metadata, and latency samples require an estimated {estimated_working_set_bytes} bytes, above the configured {} byte working-set budget",
            config.max_working_set_bytes
        );
        let measured_changes = config
            .cycles
            .checked_mul(config.changes_per_cycle)
            .expect("measured Change count fits usize");
        let measured_rows = measured_changes
            .checked_mul(config.rows_per_change)
            .expect("measured row count fits usize");
        let measured_written_bytes = cycle_encoded_bytes
            .checked_mul(config.cycles)
            .expect("measured encoded bytes fit usize");
        let total_written_bytes = seed_encoded_bytes
            .checked_add(measured_written_bytes)
            .expect("total encoded bytes fit usize");
        assert!(
            total_written_bytes <= config.max_total_written_bytes,
            "seed plus measured writes require {total_written_bytes} bytes, above the configured {} byte total-write budget",
            config.max_total_written_bytes
        );
        let total_changes = seed_entries
            .checked_add(measured_changes)
            .expect("total Change count fits usize");
        let total_rows = total_changes
            .checked_mul(config.rows_per_change)
            .expect("total event count fits usize");
        u64::try_from(total_rows).expect("endurance event ids must fit u64");

        Self {
            encoded_bytes_per_change,
            cycle_encoded_bytes,
            seed_entries,
            seed_encoded_bytes,
            measured_changes,
            measured_rows,
            measured_encoded_bytes: measured_written_bytes,
            estimated_working_set_bytes,
            total_written_bytes,
        }
    }
}

fn preflight_dimensions(config: &Config) -> usize {
    let binary_bytes_per_change = config
        .rows_per_change
        .checked_mul(config.payload_bytes)
        .expect("Binary payload bytes fit usize");
    assert!(
        i32::try_from(binary_bytes_per_change).is_ok(),
        "rows/change * payload bytes must fit Arrow Binary's i32 offsets"
    );
    assert!(
        i32::try_from(config.rows_per_change).is_ok(),
        "rows/change must fit Arrow Binary's i32 offset count"
    );
    let cycle_rows = config
        .rows_per_change
        .checked_mul(config.changes_per_cycle)
        .expect("cycle row count fits usize");
    let cycle_payload = binary_bytes_per_change
        .checked_mul(config.changes_per_cycle)
        .expect("cycle payload bytes fit usize");
    let payload_working_set = cycle_payload
        .checked_mul(4)
        .expect("payload working-set estimate fits usize");
    let row_working_set = cycle_rows
        .checked_mul(128)
        .expect("row working-set estimate fits usize");
    let entry_working_set = config
        .changes_per_cycle
        .checked_mul(8 * 1_024)
        .expect("entry working-set estimate fits usize");
    let estimated_working_set = payload_working_set
        .checked_add(row_working_set)
        .and_then(|value| value.checked_add(entry_working_set))
        .expect("cycle working-set estimate fits usize");
    assert!(
        estimated_working_set <= config.max_working_set_bytes,
        "estimated cycle working set {estimated_working_set} exceeds the configured {} byte budget",
        config.max_working_set_bytes
    );

    let measured_changes = config
        .cycles
        .checked_mul(config.changes_per_cycle)
        .expect("measured Change count fits usize");
    let measured_rows = measured_changes
        .checked_mul(config.rows_per_change)
        .expect("measured row count fits usize");
    u64::try_from(measured_rows).expect("measured event ids must fit u64");
    estimated_working_set
}

fn prepare_batch(
    config: &Config,
    entries: usize,
    next_event: &mut u64,
    expected_encoded_len: usize,
) -> PreparedBatch {
    assert!(entries > 0);
    let mut metadata = Vec::with_capacity(entries);
    let mut encoded = Vec::with_capacity(entries);
    let mut encoded_bytes = 0_usize;
    for _ in 0..entries {
        let event_start = *next_event;
        let change = wide_change(event_start, config.rows_per_change, config.payload_bytes);
        let checksum = checksum_change(&change);
        let entry = encode_change(&change).expect("encode endurance Change");
        assert_eq!(
            entry.len(),
            expected_encoded_len,
            "endurance budget requires stable encoded entry lengths"
        );
        encoded_bytes = encoded_bytes
            .checked_add(entry.len())
            .expect("batch encoded bytes fit usize");
        encoded.push(entry);
        metadata.push((event_start, checksum));
        *next_event = next_event
            .checked_add(u64::try_from(config.rows_per_change).expect("rows per Change fit u64"))
            .expect("endurance event id fits u64");
    }
    assert!(
        encoded_bytes <= config.max_working_set_bytes,
        "actual encoded batch exceeded the configured cycle budget"
    );
    PreparedBatch {
        metadata,
        encoded,
        encoded_bytes,
    }
}

fn remember_batch(
    batch: PreparedBatch,
    retained: &mut VecDeque<RetainedEntry>,
    retained_bytes: &mut usize,
    next_offset: &mut u64,
) {
    assert_eq!(batch.metadata.len(), batch.encoded.len());
    for ((event_start, checksum), encoded) in batch.metadata.into_iter().zip(batch.encoded) {
        *retained_bytes = retained_bytes
            .checked_add(encoded.len())
            .expect("retained encoded bytes fit usize");
        retained.push_back(RetainedEntry {
            offset: *next_offset,
            event_start,
            encoded_len: encoded.len(),
            checksum,
        });
        *next_offset = next_offset
            .checked_add(1)
            .expect("endurance offset fits u64");
    }
}

fn total_duration(samples: &[Duration]) -> Duration {
    samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |total, value| {
            total
                .checked_add(value)
                .expect("sample durations fit Duration")
        })
}

fn fold_checksum(state: u64, value: u64) -> u64 {
    state.rotate_left(11) ^ value.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn rate_per_second(value: usize, elapsed: Duration) -> u128 {
    assert!(!elapsed.is_zero());
    u128::try_from(value)
        .expect("throughput value fits u128")
        .checked_mul(1_000_000_000)
        .expect("throughput numerator fits u128")
        / elapsed.as_nanos()
}

fn verify_after_reopen(
    path: &Path,
    retained: &VecDeque<RetainedEntry>,
    retained_bytes: usize,
    config: &Config,
) -> u64 {
    let store = Store::open(path).expect("reopen endurance Store");
    let log: AppendLog<Vec<u8>> = store.open_data("changes").expect("reopen endurance log");
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().expect("begin endurance verification");
    let access = log
        .access(transaction.access())
        .expect("access reopened endurance log");
    let expected_start = retained.front().expect("retained entry").offset;
    let expected_end = retained
        .back()
        .expect("retained entry")
        .offset
        .checked_add(1)
        .expect("endurance tail fits u64");
    assert_eq!(
        access.bounds().expect("read reopened endurance bounds"),
        expected_start..expected_end
    );

    let scan_bytes = retained_bytes
        .checked_add(
            retained
                .len()
                .checked_mul(size_of::<u64>())
                .expect("retained offset bytes fit usize"),
        )
        .expect("retained scan bytes fit usize");
    let mut index = 0_usize;
    let mut actual_checksum = 0_u64;
    let progress = access
        .scan(
            expected_start,
            ScanLimit::new(retained.len(), scan_bytes).expect("valid endurance scan limit"),
            |entry| {
                let expected = &retained[index];
                assert_eq!(entry.offset(), expected.offset);
                let expected_change = wide_change(
                    expected.event_start,
                    config.rows_per_change,
                    config.payload_bytes,
                );
                let expected_encoded =
                    encode_change(&expected_change).expect("re-encode retained Change oracle");
                assert_eq!(expected_encoded.len(), expected.encoded_len);
                let raw = entry.project(|encoded| Ok(encoded.to_vec()))?;
                assert_eq!(raw, expected_encoded);
                let decoded = entry.project(decode_entry)?;
                assert_change_eq(&decoded, &expected_change);
                let checksum = checksum_change(&decoded);
                assert_eq!(checksum, expected.checksum);
                actual_checksum = fold_checksum(actual_checksum, checksum);
                index += 1;
                Ok::<(), StoreError>(())
            },
        )
        .expect("scan reopened endurance log");
    assert!(progress.caught_up);
    assert_eq!(index, retained.len());
    let expected_checksum = retained
        .iter()
        .fold(0_u64, |state, entry| fold_checksum(state, entry.checksum));
    assert_eq!(actual_checksum, expected_checksum);

    // Also prove the last entry is an independently decodable complete stream.
    let last = retained.back().expect("retained entry");
    let last_encoded = encode_change(&wide_change(
        last.event_start,
        config.rows_per_change,
        config.payload_bytes,
    ))
    .expect("encode final retained Change");
    assert!(decode_change(&last_encoded).is_ok());
    actual_checksum
}

impl FilePeaks {
    fn observe(&mut self, size: FileSize) {
        self.logical = self.logical.max(size.logical);
        self.allocated = self.allocated.max(size.allocated);
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

fn bytes_usize(value: usize) -> String {
    bytes(u64::try_from(value).expect("byte count fits u64"))
}

fn bytes(value: u64) -> String {
    const GIBIBYTE_BYTES: u64 = 1_073_741_824;
    const MEBIBYTE_BYTES: u64 = 1_048_576;

    if value >= GIBIBYTE_BYTES {
        scaled_bytes(value, GIBIBYTE_BYTES, "GiB")
    } else {
        scaled_bytes(value, MEBIBYTE_BYTES, "MiB")
    }
}

fn scaled_bytes(value: u64, unit_bytes: u64, unit: &str) -> String {
    let hundredths = u128::from(value)
        .checked_mul(100)
        .expect("byte formatting calculation fits u128")
        / u128::from(unit_bytes);
    format!("{}.{:02} {unit}", hundredths / 100, hundredths % 100)
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
