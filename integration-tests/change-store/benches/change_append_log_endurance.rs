use dogpaddle_bench_protocol::require_benchmark_build;

#[path = "change_append_log_endurance/mod.rs"]
mod endurance;
#[path = "support/mod.rs"]
mod support;

fn main() {
    require_benchmark_build("change_append_log_endurance");
    endurance::run();
}
