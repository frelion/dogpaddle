mod consumer;
mod gc;
mod lifecycle;
mod producer;

use std::{collections::VecDeque, hint::black_box, path::Path, time::Instant};

use dogpaddle_bench_protocol::LatencySummary;

use super::{
    config::{Config, WorkloadMode},
    oracle::StreamOracle,
    report::{
        CycleSample, FilePeaks, ScenarioResult, data_file_size, emit_cycle_sample, number_summary,
    },
    workload::{EntryOracle, WorkloadStream, projection_metadata},
};
use consumer::{ConsumerKind, consume_to_tail};
use gc::truncate_to_window;
use lifecycle::{Session, assert_byte_window, preflight, total_duration, verify_final_reopen};
use producer::{ProductionLedger, append_measured, append_seed, retain};

#[expect(
    clippy::too_many_lines,
    reason = "the top-level endurance lifecycle stays linear while phase internals live in focused modules"
)]
pub(super) fn run_scenario(
    config: &Config,
    mode: WorkloadMode,
    store_path: &Path,
) -> ScenarioResult {
    let preflight = preflight(config, mode);
    let mut stream = WorkloadStream::new(config, mode);
    let mut session = Session::create(store_path);
    let initial_file = data_file_size(store_path);
    let mut file_peaks = FilePeaks::default();
    file_peaks.observe(initial_file);

    let mut retained = VecDeque::<EntryOracle>::new();
    let mut retained_bytes = 0_usize;
    let mut next_offset = 0_u64;
    let mut production = ProductionLedger::new();
    let mut full_consumer_oracle = StreamOracle::new(0);

    let mut pending = Some(loop {
        let candidate = stream.prepare(1);
        assert!(
            candidate.encoded_bytes <= config.retained_encoded_bytes,
            "retained byte target must hold every complete encoded Change"
        );
        if retained_bytes
            .checked_add(candidate.encoded_bytes)
            .expect("seed bytes fit usize")
            > config.retained_encoded_bytes
        {
            assert!(
                !retained.is_empty(),
                "endurance seed keeps at least one entry"
            );
            break candidate;
        }
        let outcome = append_seed(
            &mut session,
            candidate,
            next_offset,
            &mut production,
            config,
        );
        retain(
            outcome,
            &mut retained,
            &mut retained_bytes,
            &mut next_offset,
        );
    });
    let seed_entries = retained.len();
    let seed_encoded_bytes = retained_bytes;
    assert_byte_window(
        retained_bytes,
        config.retained_encoded_bytes,
        preflight.max_entry_bytes,
    );

    let seed_full = consume_to_tail(
        &mut session,
        &retained,
        next_offset,
        mode.persona(),
        config,
        ConsumerKind::Full,
        &mut full_consumer_oracle,
    );
    let seed_projected = consume_to_tail(
        &mut session,
        &retained,
        next_offset,
        mode.persona(),
        config,
        ConsumerKind::Projected,
        &mut full_consumer_oracle,
    );
    assert_eq!(seed_full.cursor, next_offset);
    assert_eq!(seed_projected.cursor, next_offset);
    assert_eq!(production.oracle, full_consumer_oracle);
    let seed_file = data_file_size(store_path);
    file_peaks.observe(seed_file);

    let measured_entries = config
        .cycles
        .checked_mul(config.changes_per_cycle)
        .expect("measured entry count fits usize");
    let mut measured_rows = 0_usize;
    let mut measured_encoded_bytes = 0_usize;
    let mut producer_durations = Vec::with_capacity(config.cycles);
    let mut full_consumer_durations = Vec::new();
    let mut projected_consumer_durations = Vec::new();
    let mut truncate_durations = Vec::with_capacity(config.cycles);
    let mut reopens = 0_usize;
    let wall_started = Instant::now();

    for cycle in 0..config.cycles {
        let head_before = retained.front().expect("seeded byte window").offset;
        let mut batch = if cycle == 0 {
            pending.take().expect("the seed leaves one pending entry")
        } else {
            stream.prepare(config.changes_per_cycle)
        };
        if cycle == 0 && config.changes_per_cycle > 1 {
            batch.extend(stream.prepare(config.changes_per_cycle - 1));
        }
        assert_eq!(batch.entries.len(), config.changes_per_cycle);
        let cycle_rows = batch.rows;
        let cycle_encoded_bytes = batch.encoded_bytes;

        let produced = append_measured(&mut session, batch, next_offset, &mut production, config);
        let producer_duration = produced.duration;
        producer_durations.push(producer_duration);
        measured_rows = measured_rows
            .checked_add(cycle_rows)
            .expect("measured rows fit usize");
        measured_encoded_bytes = measured_encoded_bytes
            .checked_add(cycle_encoded_bytes)
            .expect("measured encoded bytes fit usize");
        retain(
            produced,
            &mut retained,
            &mut retained_bytes,
            &mut next_offset,
        );
        let append_file = data_file_size(store_path);
        file_peaks.observe(append_file);

        let full = consume_to_tail(
            &mut session,
            &retained,
            next_offset,
            mode.persona(),
            config,
            ConsumerKind::Full,
            &mut full_consumer_oracle,
        );
        let projected = consume_to_tail(
            &mut session,
            &retained,
            next_offset,
            mode.persona(),
            config,
            ConsumerKind::Projected,
            &mut full_consumer_oracle,
        );
        full_consumer_durations.extend(full.durations.iter().copied());
        projected_consumer_durations.extend(projected.durations.iter().copied());
        assert_eq!(full.cursor, next_offset);
        assert_eq!(projected.cursor, next_offset);

        let gc = truncate_to_window(
            &mut session,
            &mut retained,
            &mut retained_bytes,
            head_before,
            next_offset,
            preflight.max_entry_bytes,
            config,
        );
        assert_eq!(gc.durable_full_cursor, full.cursor);
        assert_eq!(gc.durable_projected_cursor, projected.cursor);
        truncate_durations.push(gc.duration);
        let truncate_file = data_file_size(store_path);
        file_peaks.observe(truncate_file);

        let reopened = (cycle + 1).is_multiple_of(config.reopen_interval_cycles.get())
            || cycle + 1 == config.cycles;
        if reopened {
            drop(session);
            session = Session::open(store_path);
            assert_eq!(session.bounds(), gc.target..next_offset);
            assert_eq!(session.read_cursors(), (next_offset, next_offset));
            reopens = reopens.checked_add(1).expect("reopen count fits usize");
            file_peaks.observe(data_file_size(store_path));
        }

        emit_cycle_sample(&CycleSample {
            mode,
            cycle,
            producer: producer_duration,
            full_consumer: full.elapsed(),
            projected_consumer: projected.elapsed(),
            truncate: gc.duration,
            full_pages: full.pages,
            projected_pages: projected.pages,
            head_before,
            target: gc.target,
            tail: next_offset,
            removed_entries: gc.removed_entries,
            removed_bytes: gc.removed_bytes,
            retained_entries: retained.len(),
            retained_encoded_bytes: retained_bytes,
            full_cursor: gc.durable_full_cursor,
            projected_cursor: gc.durable_projected_cursor,
            reopened,
            append_file,
            truncate_file,
        });
    }

    let wall_elapsed = wall_started.elapsed();
    assert_eq!(production.oracle, full_consumer_oracle);
    assert_eq!(production.oracle.entries(), seed_entries + measured_entries);
    assert_eq!(
        production.oracle.rows(),
        production.row_counts.iter().sum::<usize>()
    );
    assert_eq!(production.oracle.next_offset(), next_offset);
    let protocol_elapsed = total_duration(&producer_durations)
        .checked_add(total_duration(&full_consumer_durations))
        .and_then(|duration| duration.checked_add(total_duration(&projected_consumer_durations)))
        .and_then(|duration| duration.checked_add(total_duration(&truncate_durations)))
        .expect("endurance protocol duration fits Duration");
    assert!(!protocol_elapsed.is_zero());

    drop(session);
    let final_file = data_file_size(store_path);
    let retained_checksum = verify_final_reopen(
        store_path,
        &retained,
        retained_bytes,
        mode,
        config,
        next_offset,
        preflight.max_entry_bytes,
    );
    black_box(retained_checksum);
    let reopened_file = data_file_size(store_path);
    file_peaks.observe(final_file);
    file_peaks.observe(reopened_file);
    let allocated_amplification_hundredths = u128::from(reopened_file.allocated)
        .checked_mul(100)
        .expect("file amplification numerator fits u128")
        / u128::try_from(retained_bytes).expect("retained bytes fit u128");

    let descriptor = mode.persona().descriptor();
    ScenarioResult {
        profile: config.profile.clone(),
        mode,
        requested_persona: descriptor.name,
        diff_model: mode.diff_model().as_str(),
        schema_names: descriptor
            .schemas
            .iter()
            .map(|schema| schema.name)
            .collect(),
        schema_business_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.business_columns)
            .collect(),
        schema_physical_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.physical_columns)
            .collect(),
        schema_leaf_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.leaf_columns)
            .collect(),
        schema_top_level_nullable_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.top_level_nullable_columns)
            .collect(),
        schema_top_level_variable_width_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.top_level_variable_width_columns)
            .collect(),
        schema_top_level_nested_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.top_level_nested_columns)
            .collect(),
        schema_total_nullable_fields: descriptor
            .schemas
            .iter()
            .map(|schema| schema.total_nullable_fields)
            .collect(),
        schema_variable_width_leaf_columns: descriptor
            .schemas
            .iter()
            .map(|schema| schema.variable_width_leaf_columns)
            .collect(),
        schema_total_nested_fields: descriptor
            .schemas
            .iter()
            .map(|schema| schema.total_nested_fields)
            .collect(),
        schema_type_summaries: descriptor
            .schemas
            .iter()
            .map(|schema| schema.type_summary)
            .collect(),
        schema_projection_profiles: projection_metadata(mode.persona()),
        base_rows_per_change: config.rows_per_change,
        base_payload_bytes: config.payload_bytes,
        rows: number_summary(&production.row_counts),
        payload_bytes: number_summary(&production.payload_widths),
        entry_lengths: number_summary(&production.entry_lengths),
        projection_selected_columns: number_summary(&production.projection.selected_columns),
        projection_total_columns: number_summary(&production.projection.total_columns),
        projection_column_selectivity_basis_points: number_summary(
            &production.projection.column_selectivity_basis_points,
        ),
        projection_selected_array_bytes: number_summary(
            &production.projection.selected_array_bytes,
        ),
        projection_total_array_bytes: number_summary(&production.projection.total_array_bytes),
        projection_array_bytes_selectivity_basis_points: number_summary(
            &production.projection.array_bytes_selectivity_basis_points,
        ),
        changes_per_cycle: config.changes_per_cycle,
        cycles: config.cycles,
        retained_target_bytes: config.retained_encoded_bytes,
        consumer_page_items: config.consumer_page_items,
        consumer_page_bytes: config.consumer_page_bytes,
        truncate_items: config.truncate_items.get(),
        reopen_interval_cycles: config.reopen_interval_cycles.get(),
        reopens,
        seed_entries,
        seed_encoded_bytes,
        measured_entries,
        measured_rows,
        measured_encoded_bytes,
        actual_written_bytes: production.actual_written_bytes,
        retained_entries: retained.len(),
        retained_encoded_bytes: retained_bytes,
        producer: LatencySummary::from_samples(&producer_durations)
            .expect("summarize producer latency"),
        full_consumer: LatencySummary::from_samples(&full_consumer_durations)
            .expect("summarize full-consumer latency"),
        projected_consumer: LatencySummary::from_samples(&projected_consumer_durations)
            .expect("summarize projected-consumer latency"),
        truncate: LatencySummary::from_samples(&truncate_durations)
            .expect("summarize truncate latency"),
        protocol_elapsed,
        wall_elapsed,
        initial_file,
        seed_file,
        final_file,
        reopened_file,
        file_peaks,
        allocated_amplification_hundredths,
        validation_checksum: production.oracle.order_checksum(),
    }
}
