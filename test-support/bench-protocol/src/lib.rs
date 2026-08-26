#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod environment;
mod jsonl;
mod order;
mod settings;
mod statistics;

pub use environment::{CommandOutput, EnvironmentCollectionError, GitState, HostEnvironment};
pub use jsonl::{
    BenchmarkRecord, ConfigurationRecord, EnvironmentRecord, ExtensionRecord, FieldError, Fields,
    JsonlError, JsonlWriter, PairSummaryRecord, RecordError, SampleRecord, SummaryRecord,
};
pub use order::{
    PairMeasurements, PairOrder, PairSchedule, PairVariant, measure_pair, measure_pair_with,
};
pub use settings::{
    BenchmarkProfile, CARGO_PROFILE_ENV, CargoProfile, CargoProfileSource, EnvError,
    positive_usize, positive_usize_list, string, string_list,
};
pub use statistics::{
    DurationSummary, LatencySummary, PairedDurationSummary, StatisticsError, duration_percentile,
};

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
