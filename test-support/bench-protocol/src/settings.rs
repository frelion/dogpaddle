use std::{collections::BTreeSet, env, ffi::OsString, fmt};

use serde::Serialize;

/// Environment variable used to report a non-default Cargo profile.
pub const CARGO_PROFILE_ENV: &str = "DOGPADDLE_CARGO_PROFILE";

/// Identifies where a reported Cargo profile name came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CargoProfileSource {
    /// No override was present, so the standard `bench` profile is assumed.
    Default,
    /// The profile name was supplied through [`CARGO_PROFILE_ENV`].
    Environment,
}

/// The Cargo profile recorded in benchmark output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoProfile {
    name: String,
    source: CargoProfileSource,
}

impl CargoProfile {
    /// Reads the process Cargo profile according to the workspace protocol.
    ///
    /// An absent [`CARGO_PROFILE_ENV`] means the standard `bench` profile. Cargo
    /// does not expose a custom `--profile` name to the benchmark process, so a
    /// custom invocation must set the variable explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`EnvError`] when the variable is not Unicode, is empty, has
    /// surrounding whitespace, or contains a control character.
    pub fn from_environment() -> Result<Self, EnvError> {
        Self::parse(env::var_os(CARGO_PROFILE_ENV))
    }

    /// Returns the Cargo profile name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the profile was defaulted or supplied explicitly.
    #[must_use]
    pub const fn source(&self) -> CargoProfileSource {
        self.source
    }

    pub(crate) fn parse(value: Option<OsString>) -> Result<Self, EnvError> {
        match value {
            None => Ok(Self {
                name: "bench".to_owned(),
                source: CargoProfileSource::Default,
            }),
            Some(value) => {
                let name = unicode(CARGO_PROFILE_ENV, value)?;
                validate_scalar(CARGO_PROFILE_ENV, &name)?;
                Ok(Self {
                    name,
                    source: CargoProfileSource::Environment,
                })
            }
        }
    }
}

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
    /// Reads `name`, defaulting to [`Self::Smoke`] when it is absent.
    ///
    /// # Errors
    ///
    /// Returns [`EnvError`] for non-Unicode values or values other than exactly
    /// `smoke` and `reference`.
    pub fn from_environment(name: &str) -> Result<Self, EnvError> {
        validate_name(name)?;
        Self::parse(name, env::var_os(name))
    }

    /// Returns the stable protocol spelling used in JSONL output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Reference => "reference",
        }
    }

    pub(crate) fn parse(name: &str, value: Option<OsString>) -> Result<Self, EnvError> {
        let Some(value) = value else {
            return Ok(Self::Smoke);
        };
        let value = unicode(name, value)?;
        match value.as_str() {
            "smoke" => Ok(Self::Smoke),
            "reference" => Ok(Self::Reference),
            _ => Err(EnvError::InvalidProfile {
                name: name.to_owned(),
                value,
            }),
        }
    }
}

impl fmt::Display for BenchmarkProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Reads one non-zero `usize` benchmark setting.
///
/// # Errors
///
/// Returns [`EnvError`] when `name` is invalid, the environment value is not
/// Unicode, is not a canonical decimal integer, is zero, or when the default is
/// zero.
pub fn positive_usize(name: &str, default: usize) -> Result<usize, EnvError> {
    validate_name(name)?;
    parse_positive_usize(name, env::var_os(name), default)
}

/// Reads one non-empty Unicode benchmark setting.
///
/// # Errors
///
/// Returns [`EnvError`] when `name` is invalid, the environment value is not
/// Unicode, is empty, has surrounding whitespace, contains a control character,
/// or when the default violates the same scalar contract.
pub fn string(name: &str, default: &str) -> Result<String, EnvError> {
    validate_name(name)?;
    parse_string(name, env::var_os(name), default)
}

/// Reads a non-empty comma-separated list of non-zero `usize` values.
///
/// Whitespace around individual list items is accepted. Empty items, duplicate
/// values, zero, signs, and non-decimal values are rejected rather than ignored.
///
/// # Errors
///
/// Returns [`EnvError`] when `name`, the configured value, or the supplied
/// default violates the contract above.
pub fn positive_usize_list(name: &str, default: &[usize]) -> Result<Vec<usize>, EnvError> {
    validate_name(name)?;
    parse_positive_usize_list(name, env::var_os(name), default)
}

/// Reads a non-empty comma-separated list of non-empty Unicode strings.
///
/// Items are trimmed, but empty or duplicate items are rejected rather than
/// silently discarded. Product-specific validation, such as an allowed workload
/// set, remains with the owning benchmark.
///
/// # Errors
///
/// Returns [`EnvError`] when `name`, the configured value, or the supplied
/// default violates the contract above.
pub fn string_list(name: &str, default: &[&str]) -> Result<Vec<String>, EnvError> {
    validate_name(name)?;
    parse_string_list(name, env::var_os(name), default)
}

/// Describes a malformed benchmark environment setting.
#[derive(Debug)]
pub enum EnvError {
    /// The variable name is empty or contains a forbidden character.
    InvalidName { name: String },
    /// A present environment value is not Unicode.
    NotUnicode { name: String },
    /// A scalar value is empty.
    Empty { name: String },
    /// A scalar value has leading or trailing whitespace.
    SurroundingWhitespace { name: String },
    /// A scalar value contains a control character.
    ControlCharacter { name: String },
    /// A positive integer is not in canonical unsigned decimal form.
    InvalidPositiveInteger { name: String, value: String },
    /// A list is empty or contains an empty item.
    EmptyListItem { name: String, index: usize },
    /// A list repeats an earlier value.
    DuplicateListItem {
        name: String,
        index: usize,
        value: String,
    },
    /// A benchmark run profile is neither `smoke` nor `reference`.
    InvalidProfile { name: String, value: String },
    /// The caller supplied an invalid default value.
    InvalidDefault { name: String, detail: &'static str },
}

impl fmt::Display for EnvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { name } => write!(formatter, "invalid environment name {name:?}"),
            Self::NotUnicode { name } => write!(formatter, "{name} must be valid Unicode"),
            Self::Empty { name } => write!(formatter, "{name} must not be empty"),
            Self::SurroundingWhitespace { name } => {
                write!(formatter, "{name} must not have surrounding whitespace")
            }
            Self::ControlCharacter { name } => {
                write!(formatter, "{name} must not contain control characters")
            }
            Self::InvalidPositiveInteger { name, value } => {
                write!(
                    formatter,
                    "{name} must be a positive decimal integer, got {value:?}"
                )
            }
            Self::EmptyListItem { name, index } => {
                write!(formatter, "{name} contains an empty item at index {index}")
            }
            Self::DuplicateListItem { name, index, value } => {
                write!(formatter, "{name} repeats {value:?} at list index {index}")
            }
            Self::InvalidProfile { name, value } => {
                write!(
                    formatter,
                    "{name} must be smoke or reference, got {value:?}"
                )
            }
            Self::InvalidDefault { name, detail } => {
                write!(formatter, "invalid default for {name}: {detail}")
            }
        }
    }
}

impl std::error::Error for EnvError {}

pub(crate) fn parse_positive_usize(
    name: &str,
    value: Option<OsString>,
    default: usize,
) -> Result<usize, EnvError> {
    if default == 0 {
        return Err(EnvError::InvalidDefault {
            name: name.to_owned(),
            detail: "positive integer defaults must be non-zero",
        });
    }
    let Some(value) = value else {
        return Ok(default);
    };
    let value = unicode(name, value)?;
    parse_decimal(name, &value)
}

pub(crate) fn parse_string(
    name: &str,
    value: Option<OsString>,
    default: &str,
) -> Result<String, EnvError> {
    if validate_scalar(name, default).is_err() {
        return Err(EnvError::InvalidDefault {
            name: name.to_owned(),
            detail: "string defaults must be non-empty, trimmed, and contain no control characters",
        });
    }
    let Some(value) = value else {
        return Ok(default.to_owned());
    };
    let value = unicode(name, value)?;
    validate_scalar(name, &value)?;
    Ok(value)
}

pub(crate) fn parse_positive_usize_list(
    name: &str,
    value: Option<OsString>,
    default: &[usize],
) -> Result<Vec<usize>, EnvError> {
    if default.is_empty() || default.contains(&0) || has_duplicates(default.iter().copied()) {
        return Err(EnvError::InvalidDefault {
            name: name.to_owned(),
            detail: "positive integer list defaults must be non-empty, non-zero, and unique",
        });
    }
    let Some(value) = value else {
        return Ok(default.to_vec());
    };
    let value = unicode(name, value)?;
    let values = parse_items(name, &value)?
        .into_iter()
        .map(|item| parse_decimal(name, item))
        .collect::<Result<Vec<_>, _>>()?;
    reject_duplicate_values(name, &values)?;
    Ok(values)
}

pub(crate) fn parse_string_list(
    name: &str,
    value: Option<OsString>,
    default: &[&str],
) -> Result<Vec<String>, EnvError> {
    if default.is_empty()
        || default.iter().any(|item| {
            item.is_empty() || item.trim() != *item || item.chars().any(char::is_control)
        })
        || has_duplicates(default.iter().copied())
    {
        return Err(EnvError::InvalidDefault {
            name: name.to_owned(),
            detail: "string list defaults must be non-empty, unique, trimmed, and contain no control characters",
        });
    }
    let Some(value) = value else {
        return Ok(default.iter().map(ToString::to_string).collect());
    };
    let value = unicode(name, value)?;
    let values = parse_items(name, &value)?
        .into_iter()
        .map(|item| {
            if item.chars().any(char::is_control) {
                Err(EnvError::ControlCharacter {
                    name: name.to_owned(),
                })
            } else {
                Ok(item.to_owned())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    reject_duplicate_values(name, &values)?;
    Ok(values)
}

fn has_duplicates<T>(values: impl IntoIterator<Item = T>) -> bool
where
    T: Ord,
{
    let mut seen = BTreeSet::new();
    values.into_iter().any(|value| !seen.insert(value))
}

fn reject_duplicate_values<T>(name: &str, values: &[T]) -> Result<(), EnvError>
where
    T: fmt::Display + Ord,
{
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            return Err(EnvError::DuplicateListItem {
                name: name.to_owned(),
                index,
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

fn parse_decimal(name: &str, value: &str) -> Result<usize, EnvError> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(EnvError::InvalidPositiveInteger {
            name: name.to_owned(),
            value: value.to_owned(),
        });
    }
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| EnvError::InvalidPositiveInteger {
            name: name.to_owned(),
            value: value.to_owned(),
        })
}

fn parse_items<'a>(name: &str, value: &'a str) -> Result<Vec<&'a str>, EnvError> {
    value
        .split(',')
        .enumerate()
        .map(|(index, item)| {
            let item = item.trim();
            if item.is_empty() {
                Err(EnvError::EmptyListItem {
                    name: name.to_owned(),
                    index,
                })
            } else {
                Ok(item)
            }
        })
        .collect()
}

fn unicode(name: &str, value: OsString) -> Result<String, EnvError> {
    value.into_string().map_err(|_| EnvError::NotUnicode {
        name: name.to_owned(),
    })
}

fn validate_scalar(name: &str, value: &str) -> Result<(), EnvError> {
    if value.is_empty() {
        return Err(EnvError::Empty {
            name: name.to_owned(),
        });
    }
    if value.trim() != value {
        return Err(EnvError::SurroundingWhitespace {
            name: name.to_owned(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(EnvError::ControlCharacter {
            name: name.to_owned(),
        });
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), EnvError> {
    if name.is_empty() || name.bytes().any(|byte| matches!(byte, b'=' | 0)) {
        Err(EnvError::InvalidName {
            name: name.to_owned(),
        })
    } else {
        Ok(())
    }
}
