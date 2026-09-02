#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod environment;
mod jsonl;
mod order;
mod report;
mod root;
mod run;
mod settings;
mod validate;

pub use environment::HostEnvironment;
pub use jsonl::{CaseSpec, Fields, ObservationSpec, PROTOCOL_VERSION, PairSide, Record};
pub use order::{PairMeasurements, PairOrder, PairSchedule, PairVariant, measure_pair_with};
pub use root::{BENCHMARK_PROFILE_ENV, BENCHMARK_ROOT_ENV, RunRoot};
pub use run::{BENCHMARK_PLAN_ONLY_ENV, CaseId, Measurement, ObservationId, Plan, Run};
pub use settings::BenchmarkProfile;
pub use validate::{Artifact, Observation, PlanFingerprint, RunValidator, Sample};

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
