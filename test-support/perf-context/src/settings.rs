use std::{env, fmt};

use serde::{Deserialize, Serialize};

use crate::root::PERFORMANCE_PROFILE_ENV;

/// Selects the workload scale and filesystem rules of a benchmark invocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceProfile {
    /// A reduced run suitable for protocol validation and local iteration.
    Smoke,
    /// A reproducible run intended for a persistent performance baseline.
    Reference,
}

impl PerformanceProfile {
    /// Reads the required `DOGPADDLE_PERF_PROFILE` value.
    ///
    /// # Panics
    ///
    /// Panics for a non-Unicode value or a value other than exactly `smoke` and
    /// `reference`.
    #[must_use]
    #[track_caller]
    pub fn from_environment() -> Self {
        match env::var_os(PERFORMANCE_PROFILE_ENV) {
            None => panic!("missing required {PERFORMANCE_PROFILE_ENV}=smoke|reference"),
            Some(value) => {
                let value = value.into_string().unwrap_or_else(|value| {
                    panic!(
                        "performance profile failure: stage=read_environment label={PERFORMANCE_PROFILE_ENV} value={} source=value is not valid Unicode",
                        value.to_string_lossy()
                    )
                });
                match value.as_str() {
                    "smoke" => Self::Smoke,
                    "reference" => Self::Reference,
                    _ => panic!(
                        "performance profile failure: stage=validate_profile label={PERFORMANCE_PROFILE_ENV} value={value:?} source=expected smoke or reference"
                    ),
                }
            }
        }
    }

    /// Selects smoke automatically for Cargo's benchmark test mode, while
    /// requiring an explicit profile for a real `cargo bench` invocation.
    #[must_use]
    pub fn for_benchmark() -> Self {
        if env::args_os().any(|argument| argument == "--bench") {
            Self::from_environment()
        } else {
            Self::Smoke
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

impl fmt::Display for PerformanceProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
