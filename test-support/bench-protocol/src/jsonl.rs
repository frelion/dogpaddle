use std::{fmt, io::Write, time::Duration};

use serde::Serialize;
use serde_json::{Map, Value};

use crate::{BenchmarkProfile, DurationSummary, HostEnvironment, PairedDurationSummary};

mod sealed {
    pub trait Sealed {}
}

/// A protocol-owned record that can be emitted by [`JsonlWriter`].
///
/// This trait is sealed so arbitrary serializable values cannot accidentally
/// enter the benchmark JSONL stream without a stable `record` discriminator.
pub trait BenchmarkRecord: sealed::Sealed + Serialize {}

/// Validated, JSON-typed extension fields owned by an individual benchmark.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Fields(Map<String, Value>);

impl Fields {
    /// Creates an empty set of extension fields.
    #[must_use]
    pub fn new() -> Self {
        Self(Map::new())
    }

    /// Adds one JSON-serializable value.
    ///
    /// # Errors
    ///
    /// Returns [`FieldError`] when the name is empty, untrimmed, the protocol
    /// discriminator `record`, already present, or when `value` cannot be encoded
    /// as JSON.
    pub fn insert<T>(&mut self, name: impl Into<String>, value: T) -> Result<(), FieldError>
    where
        T: Serialize,
    {
        let name = name.into();
        validate_field_name(&name)?;
        if self.0.contains_key(&name) {
            return Err(FieldError::Duplicate(name));
        }
        let value = serde_json::to_value(value).map_err(FieldError::Encode)?;
        self.0.insert(name, value);
        Ok(())
    }

    /// Builder-style variant of [`Self::insert`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::insert`].
    pub fn with<T>(mut self, name: impl Into<String>, value: T) -> Result<Self, FieldError>
    where
        T: Serialize,
    {
        self.insert(name, value)?;
        Ok(self)
    }

    /// Returns the number of extension fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether there are no extension fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn into_record_fields(
        self,
        reserved: &'static [&'static str],
    ) -> Result<Map<String, Value>, RecordError> {
        if let Some(name) = reserved.iter().find(|name| self.0.contains_key(**name)) {
            return Err(RecordError::ReservedField((*name).to_owned()));
        }
        Ok(self.0)
    }
}

/// Failure to construct extension fields.
#[derive(Debug)]
pub enum FieldError {
    /// A field name is empty or has surrounding whitespace.
    InvalidName(String),
    /// `record` is always owned by the protocol envelope.
    ReservedRecord,
    /// A field name was inserted twice.
    Duplicate(String),
    /// A field value could not be represented as JSON.
    Encode(serde_json::Error),
}

impl fmt::Display for FieldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName(name) => write!(formatter, "invalid JSONL field name {name:?}"),
            Self::ReservedRecord => formatter.write_str("JSONL field `record` is protocol-owned"),
            Self::Duplicate(name) => write!(formatter, "duplicate JSONL field {name:?}"),
            Self::Encode(error) => write!(formatter, "encode JSONL field: {error}"),
        }
    }
}

impl std::error::Error for FieldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::InvalidName(_) | Self::ReservedRecord | Self::Duplicate(_) => None,
        }
    }
}

/// A typed benchmark environment JSONL record.
#[derive(Debug, Serialize)]
pub struct EnvironmentRecord {
    record: &'static str,
    benchmark: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<BenchmarkProfile>,
    #[serde(flatten)]
    host: HostEnvironment,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl EnvironmentRecord {
    /// Creates an environment record without a smoke/reference profile.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for an invalid benchmark name or fields colliding
    /// with protocol-owned environment keys.
    pub fn new(
        benchmark: impl Into<String>,
        host: HostEnvironment,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        Self::build(benchmark.into(), None, host, fields)
    }

    /// Creates an environment record with an explicit smoke/reference profile.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for an invalid benchmark name or fields colliding
    /// with protocol-owned environment keys.
    pub fn for_profile(
        benchmark: impl Into<String>,
        profile: BenchmarkProfile,
        host: HostEnvironment,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        Self::build(benchmark.into(), Some(profile), host, fields)
    }

    fn build(
        benchmark: String,
        profile: Option<BenchmarkProfile>,
        host: HostEnvironment,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        validate_label("benchmark", &benchmark)?;
        let fields = fields.into_record_fields(&[
            "benchmark",
            "profile",
            "cargo_profile",
            "cargo_profile_source",
            "filesystem_path",
            "filesystem",
            "os",
            "arch",
            "kernel",
            "cpu",
            "parallelism",
            "rustc",
            "git_revision",
            "git_state",
            "debug_assertions",
            "unix_seconds",
        ])?;
        Ok(Self {
            record: "environment",
            benchmark,
            profile,
            host,
            fields,
        })
    }
}

impl sealed::Sealed for EnvironmentRecord {}
impl BenchmarkRecord for EnvironmentRecord {}

/// A typed benchmark configuration JSONL record.
#[derive(Debug, Serialize)]
pub struct ConfigurationRecord {
    record: &'static str,
    benchmark: String,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl ConfigurationRecord {
    /// Creates a configuration record.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for an invalid benchmark name or a benchmark field
    /// named `benchmark`.
    pub fn new(benchmark: impl Into<String>, fields: Fields) -> Result<Self, RecordError> {
        let benchmark = benchmark.into();
        validate_label("benchmark", &benchmark)?;
        Ok(Self {
            record: "configuration",
            benchmark,
            fields: fields.into_record_fields(&["benchmark"])?,
        })
    }
}

impl sealed::Sealed for ConfigurationRecord {}
impl BenchmarkRecord for ConfigurationRecord {}

/// A typed raw duration sample JSONL record.
#[derive(Debug, Serialize)]
pub struct SampleRecord {
    record: &'static str,
    benchmark: String,
    scenario: String,
    sample: usize,
    elapsed_ns: u128,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl SampleRecord {
    /// Creates one raw sample record.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for invalid benchmark/scenario labels or extension
    /// fields colliding with protocol-owned sample keys.
    pub fn new(
        benchmark: impl Into<String>,
        scenario: impl Into<String>,
        sample: usize,
        elapsed: Duration,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        let benchmark = benchmark.into();
        let scenario = scenario.into();
        validate_label("benchmark", &benchmark)?;
        validate_label("scenario", &scenario)?;
        Ok(Self {
            record: "sample",
            benchmark,
            scenario,
            sample,
            elapsed_ns: elapsed.as_nanos(),
            fields: fields.into_record_fields(&[
                "benchmark",
                "scenario",
                "sample",
                "elapsed_ns",
            ])?,
        })
    }
}

impl sealed::Sealed for SampleRecord {}
impl BenchmarkRecord for SampleRecord {}

/// A typed min/median/max JSONL summary record.
#[derive(Debug, Serialize)]
pub struct SummaryRecord {
    record: &'static str,
    benchmark: String,
    scenario: String,
    #[serde(flatten)]
    summary: DurationSummary,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl SummaryRecord {
    /// Creates a standard duration summary record.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for invalid labels or extension fields colliding
    /// with the protocol-owned summary keys.
    pub fn new(
        benchmark: impl Into<String>,
        scenario: impl Into<String>,
        summary: DurationSummary,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        let benchmark = benchmark.into();
        let scenario = scenario.into();
        validate_label("benchmark", &benchmark)?;
        validate_label("scenario", &scenario)?;
        Ok(Self {
            record: "summary",
            benchmark,
            scenario,
            summary,
            fields: fields.into_record_fields(&[
                "benchmark",
                "scenario",
                "samples",
                "min_ns",
                "median_ns",
                "max_ns",
            ])?,
        })
    }
}

impl sealed::Sealed for SummaryRecord {}
impl BenchmarkRecord for SummaryRecord {}

/// A typed paired-comparison JSONL summary record.
#[derive(Debug, Serialize)]
pub struct PairSummaryRecord {
    record: &'static str,
    benchmark: String,
    scenario: String,
    first_variant: String,
    second_variant: String,
    #[serde(flatten)]
    summary: PairedDurationSummary,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl PairSummaryRecord {
    /// Creates a paired duration summary record.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for invalid labels or extension fields colliding
    /// with protocol-owned paired-summary keys.
    pub fn new(
        benchmark: impl Into<String>,
        scenario: impl Into<String>,
        first_variant: impl Into<String>,
        second_variant: impl Into<String>,
        summary: PairedDurationSummary,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        let benchmark = benchmark.into();
        let scenario = scenario.into();
        let first_variant = first_variant.into();
        let second_variant = second_variant.into();
        validate_label("benchmark", &benchmark)?;
        validate_label("scenario", &scenario)?;
        validate_label("first_variant", &first_variant)?;
        validate_label("second_variant", &second_variant)?;
        Ok(Self {
            record: "pair_summary",
            benchmark,
            scenario,
            first_variant,
            second_variant,
            summary,
            fields: fields.into_record_fields(&[
                "benchmark",
                "scenario",
                "first_variant",
                "second_variant",
                "samples",
                "median_first_over_second",
                "second_wins",
            ])?,
        })
    }
}

impl sealed::Sealed for PairSummaryRecord {}
impl BenchmarkRecord for PairSummaryRecord {}

/// A typed envelope for uncommon, benchmark-owned JSONL record shapes.
///
/// This is the narrow escape hatch for stable protocol records such as
/// `checkpoint` and `endurance_summary`. The protocol validates the
/// discriminator and `benchmark` field while the owning benchmark defines all
/// remaining typed [`Fields`]. Prefer the dedicated record structs when one
/// exists.
#[derive(Debug, Serialize)]
pub struct ExtensionRecord {
    record: String,
    benchmark: String,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl ExtensionRecord {
    /// Creates an uncommon record with an explicit stable discriminator.
    ///
    /// A discriminator must start with an ASCII lowercase letter and contain
    /// only ASCII lowercase letters, digits, and underscores.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] for an invalid discriminator or benchmark label,
    /// or an extension field named `benchmark`.
    pub fn new(
        record: impl Into<String>,
        benchmark: impl Into<String>,
        fields: Fields,
    ) -> Result<Self, RecordError> {
        let record = record.into();
        let benchmark = benchmark.into();
        validate_discriminator(&record)?;
        if matches!(
            record.as_str(),
            "environment" | "configuration" | "sample" | "summary" | "pair_summary"
        ) {
            return Err(RecordError::ReservedDiscriminator(record));
        }
        validate_label("benchmark", &benchmark)?;
        Ok(Self {
            record,
            benchmark,
            fields: fields.into_record_fields(&["benchmark"])?,
        })
    }
}

impl sealed::Sealed for ExtensionRecord {}
impl BenchmarkRecord for ExtensionRecord {}

/// Failure to construct a protocol record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordError {
    /// A required label is empty, untrimmed, or contains a control character.
    InvalidLabel { field: &'static str, value: String },
    /// A record discriminator is not stable lowercase snake case.
    InvalidDiscriminator(String),
    /// A dedicated typed record already owns this discriminator.
    ReservedDiscriminator(String),
    /// An extension field collides with a protocol-owned key.
    ReservedField(String),
}

impl fmt::Display for RecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLabel { field, value } => {
                write!(formatter, "invalid {field} label {value:?}")
            }
            Self::InvalidDiscriminator(value) => {
                write!(formatter, "invalid JSONL record discriminator {value:?}")
            }
            Self::ReservedDiscriminator(value) => {
                write!(
                    formatter,
                    "JSONL record discriminator {value:?} is owned by a dedicated record type"
                )
            }
            Self::ReservedField(field) => {
                write!(formatter, "JSONL field {field:?} is protocol-owned")
            }
        }
    }
}

impl std::error::Error for RecordError {}

/// Writes compact, newline-delimited benchmark records.
#[derive(Debug)]
pub struct JsonlWriter<W> {
    output: W,
}

impl<W> JsonlWriter<W>
where
    W: Write,
{
    /// Wraps a destination implementing [`Write`].
    pub const fn new(output: W) -> Self {
        Self { output }
    }

    /// Serializes exactly one compact JSON object followed by a newline.
    ///
    /// # Errors
    ///
    /// Returns [`JsonlError`] when serialization or writing fails.
    pub fn write<R>(&mut self, record: &R) -> Result<(), JsonlError>
    where
        R: BenchmarkRecord,
    {
        serde_json::to_writer(&mut self.output, record).map_err(JsonlError::Serialize)?;
        self.output.write_all(b"\n").map_err(JsonlError::Write)
    }

    /// Flushes the wrapped destination.
    ///
    /// # Errors
    ///
    /// Returns [`JsonlError::Write`] when flushing fails.
    pub fn flush(&mut self) -> Result<(), JsonlError> {
        self.output.flush().map_err(JsonlError::Write)
    }

    /// Returns the wrapped destination.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.output
    }
}

/// Failure while writing a JSONL record.
#[derive(Debug)]
pub enum JsonlError {
    /// JSON serialization failed.
    Serialize(serde_json::Error),
    /// Writing the record delimiter or flushing failed.
    Write(std::io::Error),
}

impl fmt::Display for JsonlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize(error) => write!(formatter, "serialize benchmark JSONL: {error}"),
            Self::Write(error) => write!(formatter, "write benchmark JSONL: {error}"),
        }
    }
}

impl std::error::Error for JsonlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize(error) => Some(error),
            Self::Write(error) => Some(error),
        }
    }
}

fn validate_field_name(name: &str) -> Result<(), FieldError> {
    if name == "record" {
        return Err(FieldError::ReservedRecord);
    }
    if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
        return Err(FieldError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn validate_label(field: &'static str, value: &str) -> Result<(), RecordError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        Err(RecordError::InvalidLabel {
            field,
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_discriminator(value: &str) -> Result<(), RecordError> {
    let mut bytes = value.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(RecordError::InvalidDiscriminator(value.to_owned()));
    }
    Ok(())
}
