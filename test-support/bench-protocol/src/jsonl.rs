use std::{collections::BTreeMap, io::Write, num::NonZeroUsize, time::Duration};

use serde::Serialize;
use serde_json::{Map, Value};

use crate::{BenchmarkProfile, HostEnvironment};

mod sealed {
    pub trait Sealed {}
}

/// A protocol-owned record that can be emitted by [`JsonlWriter`].
///
/// This trait is sealed so arbitrary serializable values cannot accidentally
/// enter the benchmark JSONL stream without a stable `record` discriminator.
pub trait BenchmarkRecord: sealed::Sealed + Serialize {}

/// Validated, JSON-typed extension fields owned by an individual benchmark.
#[derive(Clone, Debug)]
pub struct Fields(Map<String, Value>);

#[expect(
    clippy::new_without_default,
    reason = "benchmark records keep one explicit field-set construction path"
)]
impl Fields {
    /// Creates an empty set of extension fields.
    #[must_use]
    pub fn new() -> Self {
        Self(Map::new())
    }

    /// Adds one JSON-serializable value.
    ///
    /// # Panics
    ///
    /// Panics when the name is empty, untrimmed, contains a control character,
    /// is the protocol discriminator `record`, already exists, or when `value`
    /// cannot be encoded as JSON.
    #[track_caller]
    pub fn insert<T>(&mut self, name: impl Into<String>, value: T)
    where
        T: Serialize,
    {
        let name = name.into();
        validate_field_name(&name);
        assert!(
            !self.0.contains_key(&name),
            "benchmark protocol failure: stage=insert field={name:?} value=duplicate source=field already exists"
        );
        let value = serde_json::to_value(value).unwrap_or_else(|source| {
            panic!(
                "benchmark protocol failure: stage=encode field={name:?} value=<unserializable> source={source}"
            )
        });
        self.0.insert(name, value);
    }

    /// Builder-style variant of [`Self::insert`].
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::insert`].
    #[must_use]
    #[track_caller]
    pub fn with<T>(mut self, name: impl Into<String>, value: T) -> Self
    where
        T: Serialize,
    {
        self.insert(name, value);
        self
    }

    #[track_caller]
    fn into_record_fields(
        self,
        stage: &'static str,
        reserved: &'static [&'static str],
    ) -> Map<String, Value> {
        if let Some(name) = reserved.iter().find(|name| self.0.contains_key(**name)) {
            panic!(
                "benchmark protocol failure: stage={stage} field={name:?} value=<extension> source=field is protocol-owned"
            );
        }
        self.0
    }
}

/// A typed benchmark environment JSONL record.
#[derive(Debug, Serialize)]
pub struct EnvironmentRecord {
    record: &'static str,
    benchmark: String,
    profile: BenchmarkProfile,
    #[serde(flatten)]
    host: HostEnvironment,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl EnvironmentRecord {
    /// Creates an environment record with an explicit smoke/reference profile.
    ///
    /// # Panics
    ///
    /// Panics for an invalid benchmark name or fields colliding with
    /// protocol-owned environment keys.
    #[track_caller]
    pub fn new(
        benchmark: impl Into<String>,
        profile: BenchmarkProfile,
        host: HostEnvironment,
        fields: Fields,
    ) -> Self {
        let benchmark = benchmark.into();
        validate_label("environment", "benchmark", &benchmark);
        let fields = fields.into_record_fields(
            "construct_environment",
            &[
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
            ],
        );
        Self {
            record: "environment",
            benchmark,
            profile,
            host,
            fields,
        }
    }
}

impl sealed::Sealed for EnvironmentRecord {}
impl BenchmarkRecord for EnvironmentRecord {}

/// A typed benchmark configuration JSONL record.
#[derive(Debug, Serialize)]
pub struct ConfigurationRecord {
    record: &'static str,
    benchmark: String,
    expected_samples: NonZeroUsize,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    required_observations: BTreeMap<String, NonZeroUsize>,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl ConfigurationRecord {
    /// Creates a configuration record with the exact number of duration samples
    /// that must follow it before completion.
    ///
    /// Required non-duration observations are added explicitly with
    /// [`Self::require_observation`].
    ///
    /// # Panics
    ///
    /// Panics for an invalid benchmark name or fields colliding with
    /// protocol-owned keys.
    #[track_caller]
    pub fn new(
        benchmark: impl Into<String>,
        expected_samples: NonZeroUsize,
        fields: Fields,
    ) -> Self {
        let benchmark = benchmark.into();
        validate_label("configuration", "benchmark", &benchmark);
        Self {
            record: "configuration",
            benchmark,
            expected_samples,
            required_observations: BTreeMap::new(),
            fields: fields.into_record_fields(
                "construct_configuration",
                &["benchmark", "expected_samples", "required_observations"],
            ),
        }
    }

    /// Requires one stable observation series with an exact record count.
    ///
    /// # Panics
    ///
    /// Panics for an invalid or duplicate series label.
    #[track_caller]
    pub fn require_observation(&mut self, series: impl Into<String>, count: NonZeroUsize) {
        let series = series.into();
        validate_label("configuration", "observation_series", &series);
        assert!(
            self.required_observations
                .insert(series.clone(), count)
                .is_none(),
            "benchmark protocol failure: stage=construct_configuration label=observation_series value={series:?} source=series is already required"
        );
    }
}

impl sealed::Sealed for ConfigurationRecord {}
impl BenchmarkRecord for ConfigurationRecord {}

/// Marks successful completion of one benchmark target.
///
/// A target emits this record exactly once, after all raw samples, owner observations, and
/// human-readable output. Consumers can therefore distinguish a complete run
/// from a process that exited successfully before executing its full tail.
#[derive(Debug, Serialize)]
pub struct CompletionRecord {
    record: &'static str,
    benchmark: String,
}

impl CompletionRecord {
    /// Creates a target completion record.
    ///
    /// # Panics
    ///
    /// Panics for an invalid benchmark label.
    #[track_caller]
    pub fn new(benchmark: impl Into<String>) -> Self {
        let benchmark = benchmark.into();
        validate_label("completion", "benchmark", &benchmark);
        Self {
            record: "completion",
            benchmark,
        }
    }
}

impl sealed::Sealed for CompletionRecord {}
impl BenchmarkRecord for CompletionRecord {}

/// A typed raw duration sample JSONL record.
#[derive(Debug, Serialize)]
pub struct SampleRecord {
    record: &'static str,
    benchmark: String,
    series: String,
    sample: usize,
    elapsed_ns: u128,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl SampleRecord {
    /// Creates one raw sample record.
    ///
    /// # Panics
    ///
    /// Panics for invalid benchmark/series labels or extension fields colliding
    /// with protocol-owned sample keys.
    #[track_caller]
    pub fn new(
        benchmark: impl Into<String>,
        series: impl Into<String>,
        sample: usize,
        elapsed: Duration,
        fields: Fields,
    ) -> Self {
        let benchmark = benchmark.into();
        let series = series.into();
        validate_label("sample", "benchmark", &benchmark);
        validate_label("sample", "series", &series);
        Self {
            record: "sample",
            benchmark,
            series,
            sample,
            elapsed_ns: elapsed.as_nanos(),
            fields: fields.into_record_fields(
                "construct_sample",
                &["benchmark", "series", "sample", "elapsed_ns"],
            ),
        }
    }
}

impl sealed::Sealed for SampleRecord {}
impl BenchmarkRecord for SampleRecord {}

/// A typed non-duration observation in a stable series.
///
/// Configuration declares every observation series and its exact count through
/// `required_observations`. A zero-based continuous `sample` index makes each
/// observation independently identifiable without owner-specific validation.
#[derive(Debug, Serialize)]
pub struct ObservationRecord {
    record: &'static str,
    benchmark: String,
    series: String,
    sample: usize,
    #[serde(flatten)]
    fields: Map<String, Value>,
}

impl ObservationRecord {
    /// Creates one raw non-duration observation.
    ///
    /// # Panics
    ///
    /// Panics for invalid benchmark/series labels or fields colliding with
    /// protocol-owned observation keys.
    #[track_caller]
    pub fn new(
        benchmark: impl Into<String>,
        series: impl Into<String>,
        sample: usize,
        fields: Fields,
    ) -> Self {
        let benchmark = benchmark.into();
        let series = series.into();
        validate_label("observation", "benchmark", &benchmark);
        validate_label("observation", "series", &series);
        Self {
            record: "observation",
            benchmark,
            series,
            sample,
            fields: fields
                .into_record_fields("construct_observation", &["benchmark", "series", "sample"]),
        }
    }
}

impl sealed::Sealed for ObservationRecord {}
impl BenchmarkRecord for ObservationRecord {}

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
    /// # Panics
    ///
    /// Panics with the record type, failing stage, and source error when
    /// serialization or writing fails.
    #[track_caller]
    pub fn write<R>(&mut self, record: &R)
    where
        R: BenchmarkRecord,
    {
        serde_json::to_writer(&mut self.output, record).unwrap_or_else(|source| {
            panic!(
                "benchmark JSONL failure: stage=serialize record_type={} source={source}",
                std::any::type_name::<R>()
            )
        });
        self.output.write_all(b"\n").unwrap_or_else(|source| {
            panic!(
                "benchmark JSONL failure: stage=write_delimiter record_type={} source={source}",
                std::any::type_name::<R>()
            )
        });
    }

    /// Flushes the wrapped destination.
    ///
    /// # Panics
    ///
    /// Panics with the failing stage and source error when flushing fails.
    #[track_caller]
    pub fn flush(&mut self) {
        self.output.flush().unwrap_or_else(|source| {
            panic!("benchmark JSONL failure: stage=flush source={source}")
        });
    }
}

#[track_caller]
fn validate_field_name(name: &str) {
    assert!(
        name != "record",
        "benchmark protocol failure: stage=validate_field field=name value={name:?} source=field is protocol-owned"
    );
    assert!(
        !name.is_empty() && name.trim() == name && !name.chars().any(char::is_control),
        "benchmark protocol failure: stage=validate_field field=name value={name:?} source=name must be non-empty, trimmed, and contain no control characters"
    );
}

#[track_caller]
fn validate_label(stage: &'static str, label: &'static str, value: &str) {
    assert!(
        !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control),
        "benchmark protocol failure: stage=construct_{stage} label={label} value={value:?} source=label must be non-empty, trimmed, and contain no control characters"
    );
}
