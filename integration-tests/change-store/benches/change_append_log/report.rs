use std::{hint::black_box, time::Duration};

use dogpaddle_bench_protocol::{
    ConfigurationRecord, DurationSummary, Fields, PairOrder, PairSchedule, PairSummaryRecord,
    PairedDurationSummary, SampleRecord, SummaryRecord, measure_pair,
};

use crate::{
    case::BenchmarkCase,
    config::Config,
    model::{CaseMetadata, Measurement, NumberSummary, ProjectionMetadata, ScenarioMetadata},
    support::{BenchStoreRoot, emit_host_environment, emit_record},
};

const BENCHMARK: &str = "change_append_log";

pub(crate) fn emit_configuration(root: &BenchStoreRoot, config: &Config, case_count: usize) {
    emit_host_environment(root, BENCHMARK);
    let mut fields = Fields::new();
    for (name, value) in [
        ("anchor_rows_per_change", config.rows_per_change),
        (
            "anchor_changes_per_transaction",
            config.changes_per_transaction,
        ),
        (
            "anchor_transactions_per_sample",
            config.transactions_per_sample,
        ),
        ("anchor_payload_bytes", config.payload_bytes),
        ("samples", config.samples),
        ("warmups", config.warmups),
        ("max_working_set_bytes", config.max_working_set_bytes),
        ("case_count", case_count),
    ] {
        fields
            .insert(name, value)
            .expect("construct benchmark configuration field");
    }
    emit_record(
        &ConfigurationRecord::new(BENCHMARK, fields)
            .expect("build Change + AppendLog configuration record"),
    );
}

pub(crate) fn run_single(
    config: &Config,
    case: &BenchmarkCase,
    scenario: &ScenarioMetadata,
    mut measure: impl FnMut() -> Measurement,
) {
    for _ in 0..config.warmups {
        black_box(measure());
    }
    let mut measurements = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let measurement = measure();
        emit_sample(case, scenario, sample, measurement, None);
        measurements.push(measurement);
    }
    emit_summary(case, scenario, &measurements);
}

pub(crate) fn run_pair(
    config: &Config,
    case: &BenchmarkCase,
    pair_scenario: &str,
    first_scenario: &ScenarioMetadata,
    second_scenario: &ScenarioMetadata,
    mut first: impl FnMut() -> Measurement,
    mut second: impl FnMut() -> Measurement,
) {
    for warmup in 0..config.warmups {
        let pair = measure_pair(
            PairSchedule::Counterbalanced.order(warmup),
            &mut first,
            &mut second,
        );
        black_box(pair);
    }

    let mut first_measurements = Vec::with_capacity(config.samples);
    let mut second_measurements = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let order = PairSchedule::Counterbalanced.order(sample);
        let pair = measure_pair(order, &mut first, &mut second);
        emit_sample(case, first_scenario, sample, pair.first, Some(order));
        emit_sample(case, second_scenario, sample, pair.second, Some(order));
        first_measurements.push(pair.first);
        second_measurements.push(pair.second);
    }
    emit_summary(case, first_scenario, &first_measurements);
    emit_summary(case, second_scenario, &second_measurements);
    emit_pair_summary(
        case,
        pair_scenario,
        first_scenario,
        second_scenario,
        &first_measurements,
        &second_measurements,
    );
}

fn emit_sample(
    case: &BenchmarkCase,
    scenario: &ScenarioMetadata,
    sample: usize,
    measurement: Measurement,
    pair_order: Option<PairOrder>,
) {
    let mut fields = scenario_fields(&case.metadata, scenario);
    fields
        .insert("observed_pages", measurement.pages)
        .expect("construct observed page field");
    fields
        .insert("result_checksum", measurement.checksum)
        .expect("construct result checksum field");
    fields
        .insert("pair_order", pair_order.map_or("unpaired", pair_order_name))
        .expect("construct pair order field");
    emit_record(
        &SampleRecord::new(
            BENCHMARK,
            scenario.scenario,
            sample,
            measurement.elapsed,
            fields,
        )
        .expect("build Change + AppendLog sample record"),
    );
}

fn emit_summary(case: &BenchmarkCase, scenario: &ScenarioMetadata, measurements: &[Measurement]) {
    let durations = measurements
        .iter()
        .map(|measurement| measurement.elapsed)
        .collect::<Vec<_>>();
    let summary = DurationSummary::from_samples(&durations).expect("summarize benchmark durations");
    assert!(!summary.median().is_zero(), "benchmark median is non-zero");
    emit_record(
        &SummaryRecord::new(
            BENCHMARK,
            scenario.scenario,
            summary,
            scenario_fields(&case.metadata, scenario),
        )
        .expect("build Change + AppendLog summary record"),
    );
    let rows_per_second = rate(case.metadata.rows_per_sample, summary.median());
    let changes_per_second = rate(case.metadata.changes_per_sample, summary.median());
    println!(
        "{:<42} {:<38} median={:?} rows/s={} changes/s={}",
        case.metadata.case_id,
        scenario.scenario,
        summary.median(),
        rows_per_second,
        changes_per_second,
    );
}

fn emit_pair_summary(
    case: &BenchmarkCase,
    pair_scenario: &str,
    first_scenario: &ScenarioMetadata,
    second_scenario: &ScenarioMetadata,
    first: &[Measurement],
    second: &[Measurement],
) {
    let first_durations = first
        .iter()
        .map(|measurement| measurement.elapsed)
        .collect::<Vec<_>>();
    let second_durations = second
        .iter()
        .map(|measurement| measurement.elapsed)
        .collect::<Vec<_>>();
    let summary = PairedDurationSummary::from_pairs(&first_durations, &second_durations)
        .expect("summarize paired benchmark durations");
    emit_record(
        &PairSummaryRecord::new(
            BENCHMARK,
            pair_scenario,
            first_scenario.variant,
            second_scenario.variant,
            summary,
            pair_fields(&case.metadata, first_scenario, second_scenario),
        )
        .expect("build Change + AppendLog pair summary record"),
    );
    println!(
        "  paired {}: first/second median={:.3}x; second wins {}/{}",
        case.metadata.case_id,
        summary.median_first_over_second(),
        summary.second_wins(),
        first.len(),
    );
}

fn scenario_fields(case: &CaseMetadata, scenario: &ScenarioMetadata) -> Fields {
    let mut fields = case_fields(case);
    for (name, value) in [
        ("operation", scenario.operation),
        ("variant", scenario.variant),
        ("lifecycle", scenario.lifecycle),
        ("timing_boundary", scenario.timing_boundary),
    ] {
        fields
            .insert(name, value)
            .expect("construct scenario string field");
    }
    fields
        .insert("headline", scenario.headline)
        .expect("construct headline field");
    if let Some(projection) = &scenario.projection {
        insert_projection(&mut fields, "projection", projection);
    } else {
        fields
            .insert("projection_profile", "not_applicable")
            .expect("construct projection applicability field");
        fields
            .insert("projection_applicable", false)
            .expect("construct projection applicability field");
    }
    fields
}

fn case_fields(case: &CaseMetadata) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in [
        ("case_id", case.case_id.as_str()),
        ("family", case.family),
        ("workload", case.workload),
        ("matrix_axis", case.matrix_axis),
        ("diff_model", case.diff_model),
        ("schema_names", case.schema_names.as_str()),
        ("schema_sequence", case.schema_sequence.as_str()),
        ("type_summary", case.type_summary.as_str()),
        ("page_profile", case.page_profile),
    ] {
        fields
            .insert(name, value)
            .expect("construct case string field");
    }
    for (name, value) in [
        ("schema_count", case.schema_count),
        ("rows_per_change", case.rows_per_change),
        ("payload_bytes", case.payload_bytes),
        ("changes_per_transaction", case.changes_per_transaction),
        ("transactions_per_sample", case.transactions_per_sample),
        ("changes_per_sample", case.changes_per_sample),
        ("rows_per_sample", case.rows_per_sample),
        ("encoded_bytes_per_sample", case.encoded_bytes_per_sample),
        (
            "projected_encoded_bytes_per_sample",
            case.projected_encoded_bytes_per_sample,
        ),
        ("scan_bytes_per_sample", case.scan_bytes_per_sample),
        ("page_max_items", case.page_max_items),
        ("page_max_bytes", case.page_max_bytes),
    ] {
        fields
            .insert(name, value)
            .expect("construct case numeric field");
    }
    for (prefix, summary) in [
        ("business_columns", case.business_columns),
        ("physical_columns", case.physical_columns),
        ("leaf_columns", case.leaf_columns),
        (
            "top_level_nullable_columns",
            case.top_level_nullable_columns,
        ),
        (
            "top_level_variable_width_columns",
            case.top_level_variable_width_columns,
        ),
        ("top_level_nested_columns", case.top_level_nested_columns),
        ("total_nullable_fields", case.total_nullable_fields),
        (
            "variable_width_leaf_columns",
            case.variable_width_leaf_columns,
        ),
        ("total_nested_fields", case.total_nested_fields),
        ("encoded_entry_bytes", case.encoded_entry_bytes),
        (
            "encoded_bytes_per_transaction",
            case.encoded_bytes_per_transaction,
        ),
        (
            "projected_encoded_entry_bytes",
            case.projected_encoded_entry_bytes,
        ),
        (
            "projected_encoded_bytes_per_transaction",
            case.projected_encoded_bytes_per_transaction,
        ),
    ] {
        insert_number_summary(&mut fields, prefix, summary);
    }
    insert_projection(&mut fields, "matrix_projection", &case.selected_projection);
    fields
}

fn pair_fields(case: &CaseMetadata, first: &ScenarioMetadata, second: &ScenarioMetadata) -> Fields {
    let mut fields = case_fields(case);
    for (prefix, scenario) in [("first", first), ("second", second)] {
        for (suffix, value) in [
            ("operation", scenario.operation),
            ("lifecycle", scenario.lifecycle),
            ("scenario", scenario.scenario),
            ("timing_boundary", scenario.timing_boundary),
        ] {
            fields
                .insert(format!("{prefix}_{suffix}"), value)
                .expect("construct paired scenario field");
        }
        fields
            .insert(format!("{prefix}_headline"), scenario.headline)
            .expect("construct paired headline field");
        if let Some(projection) = &scenario.projection {
            insert_projection(&mut fields, &format!("{prefix}_projection"), projection);
        } else {
            fields
                .insert(format!("{prefix}_projection_profile"), "not_applicable")
                .expect("construct paired projection applicability field");
        }
    }
    fields
}

fn insert_projection(fields: &mut Fields, prefix: &str, projection: &ProjectionMetadata) {
    fields
        .insert(format!("{prefix}_applicable"), true)
        .expect("construct projection applicability field");
    fields
        .insert(format!("{prefix}_profile"), projection.profile.as_str())
        .expect("construct projection profile field");
    for (suffix, summary) in [
        ("selected_columns", projection.selected_columns),
        ("total_columns", projection.total_columns),
        (
            "column_selectivity_basis_points",
            projection.column_selectivity_basis_points,
        ),
        ("selected_array_bytes", projection.selected_array_bytes),
        ("total_array_bytes", projection.total_array_bytes),
        (
            "array_bytes_selectivity_basis_points",
            projection.array_bytes_selectivity_basis_points,
        ),
    ] {
        insert_number_summary(fields, &format!("{prefix}_{suffix}"), summary);
    }
}

fn insert_number_summary(fields: &mut Fields, prefix: &str, summary: NumberSummary) {
    for (suffix, value) in [
        ("min", summary.min),
        ("p50", summary.p50),
        ("max", summary.max),
    ] {
        fields
            .insert(format!("{prefix}_{suffix}"), value)
            .expect("construct numeric summary field");
    }
}

fn pair_order_name(order: PairOrder) -> &'static str {
    match order {
        PairOrder::Ab => "ab",
        PairOrder::Ba => "ba",
    }
}

fn rate(count: usize, elapsed: Duration) -> u128 {
    u128::try_from(count)
        .expect("work count fits u128")
        .checked_mul(1_000_000_000)
        .expect("rate numerator fits u128")
        / elapsed.as_nanos()
}
