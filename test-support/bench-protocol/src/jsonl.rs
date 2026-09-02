use std::{io::Write, num::NonZeroUsize};

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::{Map, Value};

use crate::{BenchmarkProfile, HostEnvironment};

/// Current benchmark artifact protocol.
pub const PROTOCOL_VERSION: u16 = 2;

/// JSON values owned by a benchmark rather than the common protocol.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Fields(Map<String, Value>);

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = Map::<String, Value>::deserialize(deserializer)?;
        for name in fields.keys() {
            validate_label_value("field", name).map_err(D::Error::custom)?;
        }
        Ok(Self(fields))
    }
}

impl Fields {
    /// Creates an empty field set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one JSON-serializable value.
    ///
    /// # Panics
    ///
    /// Panics for an invalid or duplicate field name, or a value that cannot be
    /// represented by JSON.
    #[track_caller]
    pub fn insert<T>(&mut self, name: impl Into<String>, value: T)
    where
        T: Serialize,
    {
        let name = name.into();
        validate_label("field", &name);
        assert!(
            !self.0.contains_key(&name),
            "benchmark field {name:?} is already present"
        );
        let value = serde_json::to_value(value)
            .unwrap_or_else(|error| panic!("encode benchmark field {name:?}: {error}"));
        self.0.insert(name, value);
    }

    /// Builder form of [`Self::insert`].
    #[must_use]
    #[track_caller]
    pub fn with<T>(mut self, name: impl Into<String>, value: T) -> Self
    where
        T: Serialize,
    {
        self.insert(name, value);
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns one owner field by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    /// Returns one unsigned integer owner field.
    #[must_use]
    pub fn get_u64(&self, name: &str) -> Option<u64> {
        self.get(name).and_then(Value::as_u64)
    }

    /// Returns one string owner field.
    #[must_use]
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(Value::as_str)
    }
}

/// Semantic side of a paired benchmark case.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairSide {
    /// First semantic variant, independent of execution order.
    First,
    /// Second semantic variant, independent of execution order.
    Second,
}

/// Stable paired-comparison identity declared once per case.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pairing {
    #[serde(deserialize_with = "deserialize_pair")]
    pair: String,
    side: PairSide,
}

impl Pairing {
    pub(crate) fn new(pair: impl Into<String>, side: PairSide) -> Self {
        let pair = pair.into();
        validate_label("pair", &pair);
        Self { pair, side }
    }

    pub(crate) fn pair(&self) -> &str {
        &self.pair
    }

    pub(crate) const fn side(&self) -> PairSide {
        self.side
    }
}

/// One duration series in a run plan.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaseSpec {
    #[serde(deserialize_with = "deserialize_series")]
    series: String,
    samples: NonZeroUsize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pairing: Option<Pairing>,
    #[serde(default, skip_serializing_if = "Fields::is_empty")]
    fields: Fields,
}

impl CaseSpec {
    /// Declares a stable series and its exact sample count.
    #[must_use]
    #[track_caller]
    pub fn new(series: impl Into<String>, samples: NonZeroUsize, fields: Fields) -> Self {
        let series = series.into();
        validate_label("series", &series);
        Self {
            series,
            samples,
            pairing: None,
            fields,
        }
    }

    /// Declares this case as one semantic side of a pair.
    #[must_use]
    #[track_caller]
    pub fn paired(mut self, pair: impl Into<String>, side: PairSide) -> Self {
        self.pairing = Some(Pairing::new(pair, side));
        self
    }

    /// Stable series identity used by comparison tools.
    #[must_use]
    pub fn series(&self) -> &str {
        &self.series
    }

    /// Exact number of raw samples in this series.
    #[must_use]
    pub const fn samples(&self) -> NonZeroUsize {
        self.samples
    }

    /// Static case facts shared by every sample.
    #[must_use]
    pub const fn fields(&self) -> &Fields {
        &self.fields
    }

    pub(crate) const fn pairing(&self) -> Option<&Pairing> {
        self.pairing.as_ref()
    }
}

/// One non-duration observation series in a run plan.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationSpec {
    #[serde(deserialize_with = "deserialize_observation_series")]
    series: String,
    samples: NonZeroUsize,
}

impl ObservationSpec {
    /// Declares a stable observation series and exact record count.
    #[must_use]
    #[track_caller]
    pub fn new(series: impl Into<String>, samples: NonZeroUsize) -> Self {
        let series = series.into();
        validate_label("observation series", &series);
        Self { series, samples }
    }

    /// Stable observation identity.
    #[must_use]
    pub fn series(&self) -> &str {
        &self.series
    }

    /// Exact record count.
    #[must_use]
    pub const fn samples(&self) -> NonZeroUsize {
        self.samples
    }
}

/// The sole serialized and deserialized benchmark wire schema.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
pub enum Record {
    /// Self-contained run identity, environment, configuration, and exact plan.
    Run {
        /// Protocol version.
        protocol: u16,
        /// Cargo benchmark target name.
        #[serde(deserialize_with = "deserialize_benchmark")]
        benchmark: String,
        /// Workload scale.
        profile: BenchmarkProfile,
        /// Reproducibility metadata.
        host: Box<HostEnvironment>,
        /// Owner configuration.
        configuration: Fields,
        /// Exact duration-series plan. Array index is the compact case ID.
        cases: Vec<CaseSpec>,
        /// Exact non-duration-series plan. Array index is the observation ID.
        observations: Vec<ObservationSpec>,
    },
    /// One raw duration sample.
    Sample {
        /// Index into the run's cases.
        case: usize,
        /// Zero-based index within the case.
        sample: usize,
        /// Raw elapsed duration.
        elapsed_ns: u64,
        /// Facts that genuinely vary by sample.
        #[serde(default, skip_serializing_if = "Fields::is_empty")]
        fields: Fields,
    },
    /// One raw non-duration observation.
    Observation {
        /// Index into the run's observation plan.
        observation: usize,
        /// Zero-based index within the series.
        sample: usize,
        /// Owner observation payload.
        #[serde(default, skip_serializing_if = "Fields::is_empty")]
        fields: Fields,
    },
    /// Proof that all records and the final owner oracle completed.
    Completion {},
}

pub(crate) fn write_record(output: &mut impl Write, record: &Record) {
    serde_json::to_writer(&mut *output, record)
        .unwrap_or_else(|error| panic!("serialize benchmark record: {error}"));
    output
        .write_all(b"\n")
        .unwrap_or_else(|error| panic!("write benchmark record: {error}"));
}

#[track_caller]
fn validate_label(kind: &str, value: &str) {
    if let Err(error) = validate_label_value(kind, value) {
        panic!("{error}");
    }
}

fn validate_label_value(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        Err(format!(
            "benchmark {kind} must be non-empty, trimmed, and contain no control characters: {value:?}"
        ))
    } else {
        Ok(())
    }
}

fn deserialize_label<'de, D>(deserializer: D, kind: &str) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_label_value(kind, &value).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_benchmark<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_label(deserializer, "benchmark")
}

fn deserialize_pair<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_label(deserializer, "pair")
}

fn deserialize_series<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_label(deserializer, "series")
}

fn deserialize_observation_series<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_label(deserializer, "observation series")
}
