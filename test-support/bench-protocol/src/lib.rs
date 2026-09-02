#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod environment;
mod jsonl;
mod order;
mod root;
mod settings;
mod statistics;

pub use environment::HostEnvironment;
pub use jsonl::{
    BenchmarkRecord, CompletionRecord, ConfigurationRecord, EnvironmentRecord, Fields, JsonlWriter,
    ObservationRecord, SampleRecord,
};
pub use order::{PairMeasurements, PairOrder, PairSchedule, PairVariant, measure_pair_with};
pub use root::{BENCHMARK_PROFILE_ENV, BENCHMARK_ROOT_ENV, RunRoot};
pub use settings::BenchmarkProfile;
pub use statistics::{DurationSummary, LatencySummary, PairedDurationSummary};

/// Rejects accidental execution of a benchmark built with debug assertions.
///
/// Cargo's benchmark profile is optimized by default. A debug-built executable
/// can validate neither latency nor throughput and must not exit successfully
/// without emitting samples.
///
/// # Panics
///
/// Panics when the current executable was compiled with debug assertions.
#[track_caller]
#[allow(clippy::assertions_on_constants)]
pub fn require_benchmark_build(benchmark: &str) {
    assert!(
        !cfg!(debug_assertions),
        "{benchmark} must run through `cargo bench` with debug assertions disabled"
    );
}

#[cfg(test)]
mod tests;
