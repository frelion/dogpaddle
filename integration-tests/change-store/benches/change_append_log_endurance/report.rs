use std::{fs, path::Path, time::Duration};

use dogpaddle_bench_protocol::{ConfigurationRecord, ExtensionRecord, Fields, LatencySummary};
use dogpaddle_change_store_integration::WorkloadPersona;

use crate::support::emit_record;

use super::config::{BENCHMARK, Config, MODE_FILTER_ENV, WorkloadMode};
use super::workload::projection_metadata;

const MDBX_DATA_FILE: &str = "mdbx.dat";

macro_rules! fields {
    ($($name:literal => $value:expr),+ $(,)?) => {{
        let mut fields = Fields::new();
        $(
            fields.insert($name, $value).expect(concat!("encode ", $name));
        )+
        fields
    }};
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct FileSize {
    pub(super) logical: u64,
    pub(super) allocated: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct FilePeaks {
    pub(super) logical: u64,
    pub(super) allocated: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NumberSummary {
    pub(super) min: usize,
    pub(super) p50: usize,
    pub(super) p95: usize,
    pub(super) p99: usize,
    pub(super) max: usize,
    pub(super) mean: usize,
}

pub(super) struct CycleSample {
    pub(super) mode: WorkloadMode,
    pub(super) cycle: usize,
    pub(super) producer: Duration,
    pub(super) full_consumer: Duration,
    pub(super) projected_consumer: Duration,
    pub(super) truncate: Duration,
    pub(super) full_pages: usize,
    pub(super) projected_pages: usize,
    pub(super) head_before: u64,
    pub(super) target: u64,
    pub(super) tail: u64,
    pub(super) removed_entries: usize,
    pub(super) removed_bytes: usize,
    pub(super) retained_entries: usize,
    pub(super) retained_encoded_bytes: usize,
    pub(super) full_cursor: u64,
    pub(super) projected_cursor: u64,
    pub(super) reopened: bool,
    pub(super) append_file: FileSize,
    pub(super) truncate_file: FileSize,
}

pub(super) struct ScenarioResult {
    pub(super) profile: String,
    pub(super) mode: WorkloadMode,
    pub(super) requested_persona: &'static str,
    pub(super) diff_model: &'static str,
    pub(super) schema_names: Vec<&'static str>,
    pub(super) schema_business_columns: Vec<usize>,
    pub(super) schema_physical_columns: Vec<usize>,
    pub(super) schema_leaf_columns: Vec<usize>,
    pub(super) schema_top_level_nullable_columns: Vec<usize>,
    pub(super) schema_top_level_variable_width_columns: Vec<usize>,
    pub(super) schema_top_level_nested_columns: Vec<usize>,
    pub(super) schema_total_nullable_fields: Vec<usize>,
    pub(super) schema_variable_width_leaf_columns: Vec<usize>,
    pub(super) schema_total_nested_fields: Vec<usize>,
    pub(super) schema_type_summaries: Vec<&'static str>,
    pub(super) schema_projection_profiles: Vec<String>,
    pub(super) base_rows_per_change: usize,
    pub(super) base_payload_bytes: usize,
    pub(super) rows: NumberSummary,
    pub(super) payload_bytes: NumberSummary,
    pub(super) entry_lengths: NumberSummary,
    pub(super) projection_selected_columns: NumberSummary,
    pub(super) projection_total_columns: NumberSummary,
    pub(super) projection_column_selectivity_basis_points: NumberSummary,
    pub(super) projection_selected_array_bytes: NumberSummary,
    pub(super) projection_total_array_bytes: NumberSummary,
    pub(super) projection_array_bytes_selectivity_basis_points: NumberSummary,
    pub(super) changes_per_cycle: usize,
    pub(super) cycles: usize,
    pub(super) retained_target_bytes: usize,
    pub(super) consumer_page_items: usize,
    pub(super) consumer_page_bytes: usize,
    pub(super) truncate_items: usize,
    pub(super) reopen_interval_cycles: usize,
    pub(super) reopens: usize,
    pub(super) seed_entries: usize,
    pub(super) seed_encoded_bytes: usize,
    pub(super) measured_entries: usize,
    pub(super) measured_rows: usize,
    pub(super) measured_encoded_bytes: usize,
    pub(super) actual_written_bytes: usize,
    pub(super) retained_entries: usize,
    pub(super) retained_encoded_bytes: usize,
    pub(super) producer: LatencySummary,
    pub(super) full_consumer: LatencySummary,
    pub(super) projected_consumer: LatencySummary,
    pub(super) truncate: LatencySummary,
    pub(super) protocol_elapsed: Duration,
    pub(super) wall_elapsed: Duration,
    pub(super) initial_file: FileSize,
    pub(super) seed_file: FileSize,
    pub(super) final_file: FileSize,
    pub(super) reopened_file: FileSize,
    pub(super) file_peaks: FilePeaks,
    pub(super) allocated_amplification_hundredths: u128,
    pub(super) validation_checksum: u64,
}

pub(super) fn emit_configuration(config: &Config) {
    let modes = config
        .workload_modes
        .iter()
        .map(|mode| mode.as_str())
        .collect::<Vec<_>>();
    let heterogeneous = WorkloadPersona::Heterogeneous.descriptor();
    let homogeneous = WorkloadPersona::BlobEvent4.descriptor();
    let fields = fields! {
        "endurance_profile" => &config.profile,
        "workload_modes" => modes,
        "workload_mode_filter_env" => MODE_FILTER_ENV,
        "rows_per_change" => config.rows_per_change,
        "changes_per_cycle" => config.changes_per_cycle,
        "cycles" => config.cycles,
        "payload_bytes" => config.payload_bytes,
        "retained_encoded_bytes" => config.retained_encoded_bytes,
        "truncate_items" => config.truncate_items.get(),
        "consumer_page_items" => config.consumer_page_items,
        "consumer_page_bytes" => config.consumer_page_bytes,
        "reopen_interval_cycles" => config.reopen_interval_cycles.get(),
        "max_working_set_bytes" => config.max_working_set_bytes,
        "max_total_written_bytes" => config.max_total_written_bytes,
        "producer_scope" => "cycle_transaction",
        "producer_timing_boundary" => "change_encode+transaction_begin+append_batch+durable_commit",
        "consumer_scope" => "page_transaction",
        "consumer_timing_boundary" => "transaction_begin+page_scan_decode+cursor_set+durable_commit",
        "cycle_sample_consumer_ns_scope" => "sum_of_page_transactions",
        "truncate_scope" => "cycle_transaction",
        "truncate_timing_boundary" => "transaction_begin+truncate_before_loop+durable_commit",
        "distribution_population" => "seed_and_measured_entries",
        "heterogeneous_persona" => heterogeneous.name,
        "heterogeneous_diff_model" => heterogeneous.diff_model.as_str(),
        "heterogeneous_schema_names" => heterogeneous.schemas.iter().map(|schema| schema.name).collect::<Vec<_>>(),
        "heterogeneous_schema_business_columns" => heterogeneous.schemas.iter().map(|schema| schema.business_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_physical_columns" => heterogeneous.schemas.iter().map(|schema| schema.physical_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_leaf_columns" => heterogeneous.schemas.iter().map(|schema| schema.leaf_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_top_level_nullable_columns" => heterogeneous.schemas.iter().map(|schema| schema.top_level_nullable_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_top_level_variable_width_columns" => heterogeneous.schemas.iter().map(|schema| schema.top_level_variable_width_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_top_level_nested_columns" => heterogeneous.schemas.iter().map(|schema| schema.top_level_nested_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_total_nullable_fields" => heterogeneous.schemas.iter().map(|schema| schema.total_nullable_fields).collect::<Vec<_>>(),
        "heterogeneous_schema_variable_width_leaf_columns" => heterogeneous.schemas.iter().map(|schema| schema.variable_width_leaf_columns).collect::<Vec<_>>(),
        "heterogeneous_schema_total_nested_fields" => heterogeneous.schemas.iter().map(|schema| schema.total_nested_fields).collect::<Vec<_>>(),
        "heterogeneous_schema_type_summaries" => heterogeneous.schemas.iter().map(|schema| schema.type_summary).collect::<Vec<_>>(),
        "heterogeneous_projection_profiles" => projection_metadata(WorkloadPersona::Heterogeneous),
        "homogeneous_persona" => homogeneous.name,
        "homogeneous_diff_model" => homogeneous.diff_model.as_str(),
        "homogeneous_schema_names" => homogeneous.schemas.iter().map(|schema| schema.name).collect::<Vec<_>>(),
        "homogeneous_projection_profiles" => projection_metadata(WorkloadPersona::BlobEvent4),
    };
    emit_record(
        &ConfigurationRecord::new(BENCHMARK, fields).expect("build endurance configuration record"),
    );
}

pub(super) fn emit_cycle_sample(sample: &CycleSample) {
    let fields = fields! {
        "workload_mode" => sample.mode.as_str(),
        "cycle" => sample.cycle,
        "producer_ns" => sample.producer.as_nanos(),
        "full_consumer_ns" => sample.full_consumer.as_nanos(),
        "projected_consumer_ns" => sample.projected_consumer.as_nanos(),
        "truncate_ns" => sample.truncate.as_nanos(),
        "full_consumer_pages" => sample.full_pages,
        "projected_consumer_pages" => sample.projected_pages,
        "consumer_ns_scope" => "sum_of_page_transactions",
        "head_before" => sample.head_before,
        "target" => sample.target,
        "tail" => sample.tail,
        "removed_entries" => sample.removed_entries,
        "removed_bytes" => sample.removed_bytes,
        "retained_entries" => sample.retained_entries,
        "retained_encoded_bytes" => sample.retained_encoded_bytes,
        "full_cursor" => sample.full_cursor,
        "projected_cursor" => sample.projected_cursor,
        "reopened" => sample.reopened,
        "append_file_logical_bytes" => sample.append_file.logical,
        "append_file_allocated_bytes" => sample.append_file.allocated,
        "truncate_file_logical_bytes" => sample.truncate_file.logical,
        "truncate_file_allocated_bytes" => sample.truncate_file.allocated,
    };
    emit_record(
        &ExtensionRecord::new("cycle_sample", BENCHMARK, fields)
            .expect("build endurance cycle record"),
    );
}

pub(super) fn emit_summary(result: &ScenarioResult) {
    let mut fields = fields! {
        "endurance_profile" => &result.profile,
        "workload_mode" => result.mode.as_str(),
        "requested_persona" => result.requested_persona,
        "diff_model" => result.diff_model,
        "schema_names" => &result.schema_names,
        "schema_business_columns" => &result.schema_business_columns,
        "schema_physical_columns" => &result.schema_physical_columns,
        "schema_leaf_columns" => &result.schema_leaf_columns,
        "schema_top_level_nullable_columns" => &result.schema_top_level_nullable_columns,
        "schema_top_level_variable_width_columns" => &result.schema_top_level_variable_width_columns,
        "schema_top_level_nested_columns" => &result.schema_top_level_nested_columns,
        "schema_total_nullable_fields" => &result.schema_total_nullable_fields,
        "schema_variable_width_leaf_columns" => &result.schema_variable_width_leaf_columns,
        "schema_total_nested_fields" => &result.schema_total_nested_fields,
        "schema_type_summaries" => &result.schema_type_summaries,
        "schema_projection_profiles" => &result.schema_projection_profiles,
        "base_rows_per_change" => result.base_rows_per_change,
        "base_payload_bytes" => result.base_payload_bytes,
        "changes_per_cycle" => result.changes_per_cycle,
        "cycles" => result.cycles,
        "retained_target_bytes" => result.retained_target_bytes,
        "consumer_page_items" => result.consumer_page_items,
        "consumer_page_bytes" => result.consumer_page_bytes,
        "truncate_items" => result.truncate_items,
        "reopen_interval_cycles" => result.reopen_interval_cycles,
        "reopens" => result.reopens,
        "seed_entries" => result.seed_entries,
        "seed_encoded_bytes" => result.seed_encoded_bytes,
        "measured_entries" => result.measured_entries,
        "measured_rows" => result.measured_rows,
        "measured_encoded_bytes" => result.measured_encoded_bytes,
        "actual_written_bytes" => result.actual_written_bytes,
        "retained_entries" => result.retained_entries,
        "retained_encoded_bytes" => result.retained_encoded_bytes,
        "protocol_ns" => result.protocol_elapsed.as_nanos(),
        "wall_ns" => result.wall_elapsed.as_nanos(),
        "allocated_amplification_hundredths" => result.allocated_amplification_hundredths,
        "validation_checksum" => format!("{:#018x}", result.validation_checksum),
        "distribution_population" => "seed_and_measured_entries",
        "wall_scope" => "measured_cycles_including_validation_and_periodic_reopen",
        "reopen_count_scope" => "measured_cycle_checkpoints_excluding_final_validation_reopen",
        "producer_scope" => "cycle_transaction",
        "producer_timing_boundary" => "change_encode+transaction_begin+append_batch+durable_commit",
        "consumer_scope" => "page_transaction",
        "consumer_timing_boundary" => "transaction_begin+page_scan_decode+cursor_set+durable_commit",
        "truncate_scope" => "cycle_transaction",
        "truncate_timing_boundary" => "transaction_begin+truncate_before_loop+durable_commit",
    };
    insert_summary_statistics(&mut fields, result);
    emit_record(
        &ExtensionRecord::new("endurance_summary", BENCHMARK, fields)
            .expect("build endurance summary record"),
    );
}

fn insert_summary_statistics(fields: &mut Fields, result: &ScenarioResult) {
    insert_number_summary(fields, "rows_per_entry", result.rows);
    insert_number_summary(
        fields,
        "payload_target_bytes_per_value",
        result.payload_bytes,
    );
    insert_number_summary(fields, "entry_length_bytes", result.entry_lengths);
    for (prefix, summary) in [
        (
            "projection_selected_columns",
            result.projection_selected_columns,
        ),
        ("projection_total_columns", result.projection_total_columns),
        (
            "projection_column_selectivity_basis_points",
            result.projection_column_selectivity_basis_points,
        ),
        (
            "projection_selected_array_bytes",
            result.projection_selected_array_bytes,
        ),
        (
            "projection_total_array_bytes",
            result.projection_total_array_bytes,
        ),
        (
            "projection_array_bytes_selectivity_basis_points",
            result.projection_array_bytes_selectivity_basis_points,
        ),
    ] {
        insert_number_summary(fields, prefix, summary);
    }
    for (prefix, latency) in [
        ("producer", result.producer),
        ("full_consumer", result.full_consumer),
        ("projected_consumer", result.projected_consumer),
        ("truncate", result.truncate),
    ] {
        insert_latency(fields, prefix, latency);
    }
    for (prefix, size) in [
        ("initial", result.initial_file),
        ("seed", result.seed_file),
        ("final", result.final_file),
        ("reopened", result.reopened_file),
        (
            "peak",
            FileSize {
                logical: result.file_peaks.logical,
                allocated: result.file_peaks.allocated,
            },
        ),
    ] {
        insert_file(fields, prefix, size);
    }
}

pub(super) fn print_configuration(config: &Config, store_base: &Path) {
    println!("Change + AppendLog endurance v2");
    println!(
        "profile={} modes={:?} store_base={} rows/change={} changes/cycle={} cycles={} payload_bytes={} retained_bytes={}",
        config.profile,
        config
            .workload_modes
            .iter()
            .map(|mode| mode.as_str())
            .collect::<Vec<_>>(),
        store_base.display(),
        config.rows_per_change,
        config.changes_per_cycle,
        config.cycles,
        config.payload_bytes,
        config.retained_encoded_bytes,
    );
    println!(
        "protocol=encode+append+durable producer; paged full/projected decode+durable cursors; committed-cursor byte-window GC; periodic close/reopen; final raw/full/order/relation oracle"
    );
}

pub(super) fn print_summary(result: &ScenarioResult) {
    println!(
        "{}: measured_entries={} rows={} encoded={} retained={}/{} protocol={} wall={} reopens={} checksum={:#018x}",
        result.mode.as_str(),
        result.measured_entries,
        result.measured_rows,
        bytes_usize(result.measured_encoded_bytes),
        result.retained_entries,
        bytes_usize(result.retained_encoded_bytes),
        duration(result.protocol_elapsed),
        duration(result.wall_elapsed),
        result.reopens,
        result.validation_checksum,
    );
    for (name, summary) in [
        ("producer", result.producer),
        ("consumer/full", result.full_consumer),
        ("consumer/projected", result.projected_consumer),
        ("truncate", result.truncate),
    ] {
        println!(
            "  {name:<18} p50={} p95={} p99={} max={}",
            duration(summary.p50()),
            duration(summary.p95()),
            duration(summary.p99()),
            duration(summary.max()),
        );
    }
    println!(
        "  entry bytes min/p50/p95/p99/max/mean={}/{}/{}/{}/{}/{} file peak(logical/allocated)={}/{} amplification={}.{:02}x",
        result.entry_lengths.min,
        result.entry_lengths.p50,
        result.entry_lengths.p95,
        result.entry_lengths.p99,
        result.entry_lengths.max,
        result.entry_lengths.mean,
        bytes(result.file_peaks.logical),
        bytes(result.file_peaks.allocated),
        result.allocated_amplification_hundredths / 100,
        result.allocated_amplification_hundredths % 100,
    );
    println!(
        "  projected columns p50={}/{} ({}.{:02}%) array bytes p50={}/{} ({}.{:02}%)",
        result.projection_selected_columns.p50,
        result.projection_total_columns.p50,
        result.projection_column_selectivity_basis_points.p50 / 100,
        result.projection_column_selectivity_basis_points.p50 % 100,
        result.projection_selected_array_bytes.p50,
        result.projection_total_array_bytes.p50,
        result.projection_array_bytes_selectivity_basis_points.p50 / 100,
        result.projection_array_bytes_selectivity_basis_points.p50 % 100,
    );
}

pub(super) fn number_summary(values: &[usize]) -> NumberSummary {
    assert!(!values.is_empty(), "number summary requires samples");
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let total = sorted
        .iter()
        .try_fold(0_u128, |sum, value| {
            sum.checked_add(u128::try_from(*value).expect("sample fits u128"))
        })
        .expect("number summary total fits u128");
    NumberSummary {
        min: sorted[0],
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        max: *sorted.last().expect("non-empty samples"),
        mean: usize::try_from(
            total / u128::try_from(sorted.len()).expect("sample count fits u128"),
        )
        .expect("mean fits usize"),
    }
}

pub(super) fn data_file_size(store_path: &Path) -> FileSize {
    let metadata = fs::metadata(store_path.join(MDBX_DATA_FILE))
        .expect("read endurance MDBX data-file metadata");
    FileSize {
        logical: metadata.len(),
        allocated: allocated_bytes(&metadata),
    }
}

impl FilePeaks {
    pub(super) fn observe(&mut self, size: FileSize) {
        self.logical = self.logical.max(size.logical);
        self.allocated = self.allocated.max(size.allocated);
    }
}

fn percentile(sorted: &[usize], percentile: usize) -> usize {
    let rank = sorted
        .len()
        .checked_mul(percentile)
        .and_then(|value| value.checked_add(99))
        .expect("percentile rank fits usize")
        / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn insert_number_summary(fields: &mut Fields, prefix: &str, summary: NumberSummary) {
    for (suffix, value) in [
        ("min", summary.min),
        ("p50", summary.p50),
        ("p95", summary.p95),
        ("p99", summary.p99),
        ("max", summary.max),
        ("mean", summary.mean),
    ] {
        fields
            .insert(format!("{prefix}_{suffix}"), value)
            .expect("encode numeric summary");
    }
}

fn insert_latency(fields: &mut Fields, prefix: &str, latency: LatencySummary) {
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

fn insert_file(fields: &mut Fields, prefix: &str, size: FileSize) {
    fields
        .insert(format!("{prefix}_file_logical_bytes"), size.logical)
        .expect("encode logical file bytes");
    fields
        .insert(format!("{prefix}_file_allocated_bytes"), size.allocated)
        .expect("encode allocated file bytes");
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

    let (unit_bytes, unit) = if value >= GIBIBYTE_BYTES {
        (GIBIBYTE_BYTES, "GiB")
    } else {
        (MEBIBYTE_BYTES, "MiB")
    };
    let hundredths = u128::from(value) * 100 / u128::from(unit_bytes);
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
