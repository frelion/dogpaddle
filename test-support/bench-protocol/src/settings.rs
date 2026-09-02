use std::{env, fmt};

use serde::Serialize;

use crate::root::BENCHMARK_PROFILE_ENV;

/// Selects the workload scale and filesystem rules of a benchmark invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkProfile {
    /// A reduced run suitable for protocol validation and local iteration.
    Smoke,
    /// A reproducible run intended for a persistent performance baseline.
    Reference,
}

impl BenchmarkProfile {
    /// Reads [`BENCHMARK_PROFILE_ENV`], defaulting to [`Self::Smoke`].
    ///
    /// # Panics
    ///
    /// Panics for a non-Unicode value or a value other than exactly `smoke` and
    /// `reference`.
    #[must_use]
    #[track_caller]
    pub fn from_environment() -> Self {
        match env::var_os(BENCHMARK_PROFILE_ENV) {
            None => Self::Smoke,
            Some(value) => {
                let value = value.into_string().unwrap_or_else(|value| {
                    panic!(
                        "benchmark profile failure: stage=read_environment label={BENCHMARK_PROFILE_ENV} value={} source=value is not valid Unicode",
                        value.to_string_lossy()
                    )
                });
                match value.as_str() {
                    "smoke" => Self::Smoke,
                    "reference" => Self::Reference,
                    _ => panic!(
                        "benchmark profile failure: stage=validate_profile label={BENCHMARK_PROFILE_ENV} value={value:?} source=expected smoke or reference"
                    ),
                }
            }
        }
    }

    /// Returns the stable protocol spelling used in JSONL output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Reference => "reference",
        }
    }
}

impl fmt::Display for BenchmarkProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
