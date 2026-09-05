#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod environment;
mod root;
mod settings;

pub use environment::HostEnvironment;
pub use root::RunRoot;
pub use settings::PerformanceProfile;

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
pub fn require_release_build(benchmark: &str) {
    assert!(
        !cfg!(debug_assertions),
        "{benchmark} must run through `cargo bench` with debug assertions disabled"
    );
}

#[cfg(test)]
mod tests;
