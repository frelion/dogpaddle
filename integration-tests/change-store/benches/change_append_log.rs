use dogpaddle_bench_protocol::require_benchmark_build;

#[path = "change_append_log/case/mod.rs"]
mod case;
#[path = "change_append_log/config.rs"]
mod config;
#[path = "change_append_log/fixture.rs"]
mod fixture;
#[path = "change_append_log/measure.rs"]
mod measure;
#[path = "change_append_log/model.rs"]
mod model;
#[path = "change_append_log/oracle.rs"]
mod oracle;
#[path = "support/regular.rs"]
mod regular_support;
#[path = "change_append_log/report.rs"]
mod report;
#[path = "support/mod.rs"]
mod support;

use case::{BenchmarkCase, CaseFamily, benchmark_specs, build_case};
use config::Config;
use measure::{AppendMode, ReplayMode};
use model::ScenarioMetadata;
use support::BenchStoreRoot;

fn main() {
    require_benchmark_build("change_append_log");

    let stores = BenchStoreRoot::from_environment();
    let config = Config::load(stores.benchmark_profile());
    let specs = benchmark_specs(&config);
    report::emit_configuration(&stores, &config, specs.len());
    println!(
        "Change + AppendLog benchmark: profile={} store_base={} cases={} samples={} warmups={}",
        stores.profile(),
        stores.base().display(),
        specs.len(),
        config.samples,
        config.warmups,
    );
    println!(
        "controls: DOGPADDLE_CHANGE_STORE_BENCH_PROFILE, _STORE_DIR, _ROWS_PER_CHANGE, _CHANGES_PER_TX, _TRANSACTIONS_PER_SAMPLE, _PAYLOAD_BYTES, _SAMPLES, _WARMUPS, _MAX_WORKING_SET_BYTES"
    );

    for (index, spec) in specs.into_iter().enumerate() {
        let case = build_case(&config, &spec, index);
        println!(
            "\n=== case={} family={} axis={} workload={} ===",
            case.metadata.case_id,
            case.metadata.family,
            case.metadata.matrix_axis,
            case.metadata.workload,
        );
        match case.family {
            CaseFamily::Anchor => {
                benchmark_producer_pair(&config, &stores, &case);
                benchmark_replay_pair(&config, &stores, &case);
                benchmark_reopened(&config, &stores, &case);
                benchmark_pipeline(&config, &stores, &case);
            }
            CaseFamily::Producer => benchmark_producer_pair(&config, &stores, &case),
            CaseFamily::ProjectionReplay | CaseFamily::PageReplay => {
                benchmark_replay_pair(&config, &stores, &case);
            }
        }
    }
}

fn benchmark_producer_pair(config: &Config, stores: &BenchStoreRoot, case: &BenchmarkCase) {
    let preencoded = ScenarioMetadata {
        scenario: "preencoded_append_durable_commit",
        operation: "append_preencoded",
        variant: "preencoded_attribution",
        lifecycle: "fresh_store_durable_transactions",
        timing_boundary: "begin_append_commit_without_fixture_or_oracle",
        headline: false,
        projection: None,
    };
    let integrated = ScenarioMetadata {
        scenario: "encode_append_durable_commit",
        operation: "encode_change_then_append",
        variant: "integrated_encode_append",
        lifecycle: "fresh_store_durable_transactions",
        timing_boundary: "begin_encode_append_commit_without_fixture_or_oracle_or_drop",
        headline: true,
        projection: None,
    };
    let preencoded_label = sample_label(case, preencoded.scenario);
    let integrated_label = sample_label(case, integrated.scenario);
    report::run_pair(
        config,
        case,
        "producer_preencoded_vs_integrated",
        &preencoded,
        &integrated,
        || measure::append_durable(stores, &preencoded_label, case, AppendMode::Preencoded),
        || measure::append_durable(stores, &integrated_label, case, AppendMode::Integrated),
    );
}

fn benchmark_replay_pair(config: &Config, stores: &BenchStoreRoot, case: &BenchmarkCase) {
    let full = ScenarioMetadata {
        scenario: "multi_page_full_replay",
        operation: "scan_decode_full",
        variant: "full_decode",
        lifecycle: "created_seeded_warm",
        timing_boundary: "scan_and_decode_body_excluding_transaction_begin_and_commit",
        headline: true,
        projection: Some(case.metadata.full_projection.clone()),
    };
    let selected = ScenarioMetadata {
        scenario: "multi_page_projected_replay",
        operation: "scan_decode_projected",
        variant: "projected_decode",
        lifecycle: "created_seeded_warm",
        timing_boundary: "scan_and_decode_body_excluding_transaction_begin_and_commit",
        headline: true,
        projection: Some(case.metadata.selected_projection.clone()),
    };
    let full_label = sample_label(case, full.scenario);
    let selected_label = sample_label(case, selected.scenario);
    report::run_pair(
        config,
        case,
        "replay_full_vs_projected",
        &full,
        &selected,
        || measure::replay(stores, &full_label, case, ReplayMode::Full),
        || measure::replay(stores, &selected_label, case, ReplayMode::Selected),
    );
}

fn benchmark_reopened(config: &Config, stores: &BenchStoreRoot, case: &BenchmarkCase) {
    let scenario = ScenarioMetadata {
        scenario: "reopened_first_full_replay",
        operation: "open_store_open_log_scan_decode_full",
        variant: "reopened_first",
        lifecycle: "reopened_first",
        timing_boundary: "open_store_open_log_begin_and_first_scan_decode",
        headline: true,
        projection: Some(case.metadata.full_projection.clone()),
    };
    let label = sample_label(case, scenario.scenario);
    report::run_single(config, case, &scenario, || {
        measure::reopened_first_replay(stores, &label, case)
    });
}

fn benchmark_pipeline(config: &Config, stores: &BenchStoreRoot, case: &BenchmarkCase) {
    let scenario = ScenarioMetadata {
        scenario: "project_decode_reencode_append_cursor_durable",
        operation: "project_decode_reencode_append_update_cursor",
        variant: "integrated_pipeline",
        lifecycle: "created_seeded_durable_page_transactions",
        timing_boundary: "per_page_begin_project_decode_reencode_append_cursor_and_commit",
        headline: true,
        projection: Some(case.metadata.selected_projection.clone()),
    };
    let label = sample_label(case, scenario.scenario);
    report::run_single(config, case, &scenario, || {
        measure::projected_pipeline(stores, &label, case)
    });
}

fn sample_label(case: &BenchmarkCase, scenario: &str) -> String {
    format!("{}-{scenario}", case.metadata.case_id)
}
