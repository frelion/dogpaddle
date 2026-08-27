mod config;
mod oracle;
mod pipeline;
mod report;
mod workload;

use crate::support::{BenchStoreRoot, emit_host_environment};

use config::{BENCHMARK, Config};

pub(super) fn run() {
    let config = Config::from_environment();
    let stores = BenchStoreRoot::from_environment();
    if config.profile == "full" {
        assert_eq!(
            stores.profile(),
            "reference",
            "the full endurance workload requires DOGPADDLE_CHANGE_STORE_BENCH_PROFILE=reference and DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR"
        );
    }

    emit_host_environment(&stores, BENCHMARK);
    report::emit_configuration(&config);
    report::print_configuration(&config, stores.base());

    for mode in config.workload_modes.iter().copied() {
        let sample = stores.sample(mode.as_str());
        let result = pipeline::run_scenario(&config, mode, sample.path());
        report::emit_summary(&result);
        report::print_summary(&result);
    }
}
