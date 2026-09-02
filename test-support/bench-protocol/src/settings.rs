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
    /// # Errors
    ///
    /// Returns [`EnvError`] for a non-Unicode value or a value other than
    /// exactly `smoke` and `reference`.
    pub fn from_environment() -> Result<Self, EnvError> {
        match env::var_os(BENCHMARK_PROFILE_ENV) {
            None => Ok(Self::Smoke),
            Some(value) => {
                let value = value.into_string().map_err(|_| EnvError::NotUnicode)?;
                match value.as_str() {
                    "smoke" => Ok(Self::Smoke),
                    "reference" => Ok(Self::Reference),
                    _ => Err(EnvError::InvalidProfile(value)),
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

/// Describes a malformed common benchmark profile.
#[derive(Debug)]
pub enum EnvError {
    /// The configured profile is not Unicode.
    NotUnicode,
    /// The configured profile is neither `smoke` nor `reference`.
    InvalidProfile(String),
}

impl fmt::Display for EnvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotUnicode => write!(formatter, "{BENCHMARK_PROFILE_ENV} must be valid Unicode"),
            Self::InvalidProfile(value) => write!(
                formatter,
                "{BENCHMARK_PROFILE_ENV} must be smoke or reference, got {value:?}"
            ),
        }
    }
}

impl std::error::Error for EnvError {}
