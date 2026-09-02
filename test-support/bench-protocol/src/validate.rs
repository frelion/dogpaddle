use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    BenchmarkProfile, CaseSpec, Fields, HostEnvironment, ObservationSpec, PROTOCOL_VERSION,
    PairSide, Record,
};

const PLAN_ALGORITHM: &str = "fnv1a-128-canonical-json-v1";
const FNV1A_128_OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV1A_128_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// One validated raw duration sample.
#[derive(Clone, Debug)]
pub struct Sample {
    elapsed_ns: u64,
    fields: Fields,
}

impl Sample {
    /// Raw elapsed nanoseconds.
    #[must_use]
    pub const fn elapsed_ns(&self) -> u64 {
        self.elapsed_ns
    }

    /// Dynamic sample facts.
    #[must_use]
    pub const fn fields(&self) -> &Fields {
        &self.fields
    }
}

/// One validated raw non-duration observation.
#[derive(Clone, Debug)]
pub struct Observation {
    fields: Fields,
}

impl Observation {
    /// Owner observation payload.
    #[must_use]
    pub const fn fields(&self) -> &Fields {
        &self.fields
    }
}

/// A complete, structurally validated benchmark artifact.
#[derive(Clone, Debug)]
pub struct Artifact {
    benchmark: String,
    profile: BenchmarkProfile,
    host: HostEnvironment,
    configuration: Fields,
    cases: Vec<CaseSpec>,
    observations: Vec<ObservationSpec>,
    samples: Vec<Vec<Sample>>,
    observation_values: Vec<Vec<Observation>>,
}

/// Stable identity of a complete benchmark plan, excluding dynamic host data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFingerprint {
    cases: usize,
    observations: usize,
    encoded_bytes: usize,
    digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanCatalog {
    benchmark: String,
    algorithm: String,
    smoke: PlanFingerprint,
    reference: PlanFingerprint,
}

#[derive(Serialize)]
struct StablePlan<'a> {
    protocol: u16,
    benchmark: &'a str,
    profile: BenchmarkProfile,
    configuration: &'a Fields,
    cases: &'a [CaseSpec],
    observations: &'a [ObservationSpec],
}

impl Artifact {
    /// Benchmark target identity.
    #[must_use]
    pub fn benchmark(&self) -> &str {
        &self.benchmark
    }

    /// Selected workload profile.
    #[must_use]
    pub const fn profile(&self) -> BenchmarkProfile {
        self.profile
    }

    /// Captured host metadata.
    #[must_use]
    pub const fn host(&self) -> &HostEnvironment {
        &self.host
    }

    /// Owner configuration.
    #[must_use]
    pub const fn configuration(&self) -> &Fields {
        &self.configuration
    }

    /// Exact duration cases.
    #[must_use]
    pub fn cases(&self) -> impl ExactSizeIterator<Item = (&CaseSpec, &[Sample])> {
        self.cases
            .iter()
            .zip(&self.samples)
            .map(|(case, samples)| (case, samples.as_slice()))
    }

    /// Exact non-duration observation series.
    #[must_use]
    pub fn observations(
        &self,
    ) -> impl ExactSizeIterator<Item = (&ObservationSpec, &[Observation])> {
        self.observations
            .iter()
            .zip(&self.observation_values)
            .map(|(spec, values)| (spec, values.as_slice()))
    }

    /// Returns the canonical golden identity of this run's stable plan.
    ///
    /// The encoded input is canonical JSON over protocol, benchmark, profile,
    /// owner configuration, lexicographically ordered cases, and
    /// lexicographically ordered observations. Dynamic host metadata and raw
    /// data records are deliberately excluded. The digest is FNV-1a 128 with
    /// the constants fixed by `fnv1a-128-canonical-json-v1`.
    ///
    /// # Panics
    ///
    /// Panics if the already validated stable plan cannot be encoded as JSON.
    #[must_use]
    pub fn plan_fingerprint(&self) -> PlanFingerprint {
        let encoded = serde_json::to_vec(&StablePlan {
            protocol: PROTOCOL_VERSION,
            benchmark: &self.benchmark,
            profile: self.profile,
            configuration: &self.configuration,
            cases: &self.cases,
            observations: &self.observations,
        })
        .expect("stable benchmark plan is JSON serializable");
        PlanFingerprint {
            cases: self.cases.len(),
            observations: self.observations.len(),
            encoded_bytes: encoded.len(),
            digest: format!("{:032x}", fnv1a_128(&encoded)),
        }
    }
}

/// Protocol-owned state machine for one benchmark process output.
pub struct RunValidator;

impl RunValidator {
    /// Parses human-and-JSON stdout and validates exactly one complete run.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic for malformed JSON, identity mismatch, an invalid
    /// plan, missing/extra/out-of-order data, or a missing terminal completion.
    pub fn validate(
        expected_benchmark: &str,
        expected_profile: &str,
        stdout: &str,
    ) -> Result<Artifact, String> {
        if !matches!(expected_profile, "smoke" | "reference") {
            return Err(format!(
                "benchmark profile must be smoke or reference, found {expected_profile:?}"
            ));
        }
        let mut state = ValidationState::new(expected_benchmark, expected_profile);
        for (line_index, line) in stdout.lines().enumerate() {
            let line = line.trim_start();
            if !line.starts_with('{') {
                continue;
            }
            let record = serde_json::from_str::<Record>(line).map_err(|error| {
                format!(
                    "malformed benchmark record on line {}: {error}",
                    line_index + 1
                )
            })?;
            state.push(record, line_index + 1)?;
        }
        state.finish()
    }

    /// Validates one complete run and compares its exact stable plan with an
    /// independently checked-in smoke/reference catalog.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the catalog is malformed, belongs to another
    /// target, uses another fingerprint algorithm, or the exact stable plan
    /// differs from the selected profile golden.
    pub fn validate_catalog(
        expected_benchmark: &str,
        expected_profile: &str,
        stdout: &str,
        catalog: &str,
    ) -> Result<Artifact, String> {
        let expected = catalog_fingerprint(expected_benchmark, expected_profile, catalog)?;
        let artifact = Self::validate(expected_benchmark, expected_profile, stdout)?;
        compare_plan(expected_benchmark, expected_profile, &expected, &artifact)?;
        Ok(artifact)
    }

    /// Validates the single run header emitted by the internal plan-only path
    /// against the selected independent catalog golden.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic for malformed output, any data/completion record,
    /// more or fewer than one run header, or an exact plan mismatch.
    pub fn validate_plan_catalog(
        expected_benchmark: &str,
        expected_profile: &str,
        stdout: &str,
        catalog: &str,
    ) -> Result<(), String> {
        let expected = catalog_fingerprint(expected_benchmark, expected_profile, catalog)?;
        let mut artifact = None;
        for (line_index, line) in stdout.lines().enumerate() {
            let line = line.trim_start();
            if !line.starts_with('{') {
                continue;
            }
            let record = serde_json::from_str::<Record>(line).map_err(|error| {
                format!(
                    "malformed benchmark plan record on line {}: {error}",
                    line_index + 1
                )
            })?;
            let Record::Run {
                protocol,
                benchmark,
                profile,
                host,
                configuration,
                cases,
                observations,
            } = record
            else {
                return Err(format!(
                    "plan-only output contains a data or completion record on line {}",
                    line_index + 1
                ));
            };
            if artifact.is_some() {
                return Err("plan-only output emitted more than one run header".to_owned());
            }
            if protocol != PROTOCOL_VERSION {
                return Err(format!(
                    "benchmark protocol must be {PROTOCOL_VERSION}, found {protocol}"
                ));
            }
            if benchmark != expected_benchmark {
                return Err(format!(
                    "run belongs to benchmark {benchmark:?}, expected {expected_benchmark:?}"
                ));
            }
            if profile.as_str() != expected_profile {
                return Err(format!(
                    "run reports profile {}, expected {expected_profile}",
                    profile.as_str()
                ));
            }
            validate_plan(&cases, &observations)?;
            let samples = cases.iter().map(|_| Vec::new()).collect();
            let observation_values = observations.iter().map(|_| Vec::new()).collect();
            artifact = Some(Artifact {
                benchmark,
                profile,
                host: *host,
                configuration,
                cases,
                observations,
                samples,
                observation_values,
            });
        }
        let artifact =
            artifact.ok_or_else(|| "plan-only output emitted no run header".to_owned())?;
        compare_plan(expected_benchmark, expected_profile, &expected, &artifact)
    }
}

struct ValidationState<'a> {
    expected_benchmark: &'a str,
    expected_profile: &'a str,
    artifact: Option<Artifact>,
    complete: bool,
}

impl<'a> ValidationState<'a> {
    const fn new(expected_benchmark: &'a str, expected_profile: &'a str) -> Self {
        Self {
            expected_benchmark,
            expected_profile,
            artifact: None,
            complete: false,
        }
    }

    fn push(&mut self, record: Record, line: usize) -> Result<(), String> {
        if self.complete {
            return Err(format!(
                "machine record appears after completion on line {line}"
            ));
        }
        match record {
            Record::Run {
                protocol,
                benchmark,
                profile,
                host,
                configuration,
                cases,
                observations,
            } => {
                if self.artifact.is_some() {
                    return Err("benchmark emitted more than one run header".to_owned());
                }
                if protocol != PROTOCOL_VERSION {
                    return Err(format!(
                        "benchmark protocol must be {PROTOCOL_VERSION}, found {protocol}"
                    ));
                }
                if benchmark != self.expected_benchmark {
                    return Err(format!(
                        "run belongs to benchmark {benchmark:?}, expected {:?}",
                        self.expected_benchmark
                    ));
                }
                if profile.as_str() != self.expected_profile {
                    return Err(format!(
                        "run reports profile {}, expected {}",
                        profile.as_str(),
                        self.expected_profile
                    ));
                }
                validate_plan(&cases, &observations)?;
                let samples = cases.iter().map(|_| Vec::new()).collect();
                let observation_values = observations.iter().map(|_| Vec::new()).collect();
                self.artifact = Some(Artifact {
                    benchmark,
                    profile,
                    host: *host,
                    configuration,
                    cases,
                    observations,
                    samples,
                    observation_values,
                });
                Ok(())
            }
            Record::Sample {
                case,
                sample,
                elapsed_ns,
                fields,
            } => self.push_sample(case, sample, elapsed_ns, fields),
            Record::Observation {
                observation,
                sample,
                fields,
            } => self.push_observation(observation, sample, fields),
            Record::Completion {} => {
                let artifact = self
                    .artifact
                    .as_ref()
                    .ok_or_else(|| "completion appears before the run header".to_owned())?;
                validate_counts(artifact)?;
                self.complete = true;
                Ok(())
            }
        }
    }

    fn push_sample(
        &mut self,
        case: usize,
        sample: usize,
        elapsed_ns: u64,
        fields: Fields,
    ) -> Result<(), String> {
        let artifact = self
            .artifact
            .as_mut()
            .ok_or_else(|| "sample appears before the run header".to_owned())?;
        let values = artifact
            .samples
            .get_mut(case)
            .ok_or_else(|| format!("sample references unknown case index {case}"))?;
        let expected = values.len();
        if sample != expected {
            return Err(format!(
                "case {case} requires contiguous sample index {expected}, found {sample}"
            ));
        }
        let limit = artifact.cases[case].samples().get();
        if expected == limit {
            return Err(format!("case {case} emitted more than {limit} samples"));
        }
        values.push(Sample { elapsed_ns, fields });
        Ok(())
    }

    fn push_observation(
        &mut self,
        observation: usize,
        sample: usize,
        fields: Fields,
    ) -> Result<(), String> {
        let artifact = self
            .artifact
            .as_mut()
            .ok_or_else(|| "observation appears before the run header".to_owned())?;
        let values = artifact
            .observation_values
            .get_mut(observation)
            .ok_or_else(|| format!("record references unknown observation index {observation}"))?;
        let expected = values.len();
        if sample != expected {
            return Err(format!(
                "observation {observation} requires contiguous index {expected}, found {sample}"
            ));
        }
        let limit = artifact.observations[observation].samples().get();
        if expected == limit {
            return Err(format!(
                "observation {observation} emitted more than {limit} records"
            ));
        }
        values.push(Observation { fields });
        Ok(())
    }

    fn finish(self) -> Result<Artifact, String> {
        if !self.complete {
            return Err(if self.artifact.is_some() {
                "benchmark did not finish with completion".to_owned()
            } else {
                "benchmark emitted no machine run".to_owned()
            });
        }
        self.artifact
            .ok_or_else(|| "complete benchmark has no run header".to_owned())
    }
}

fn validate_plan(cases: &[CaseSpec], observations: &[ObservationSpec]) -> Result<(), String> {
    if cases.is_empty() {
        return Err("run plan must contain at least one duration case".to_owned());
    }
    let mut series = BTreeSet::new();
    if !strictly_sorted(cases.iter().map(CaseSpec::series)) {
        return Err("duration cases must use canonical lexicographic series order".to_owned());
    }
    if !strictly_sorted(observations.iter().map(ObservationSpec::series)) {
        return Err("observations must use canonical lexicographic series order".to_owned());
    }
    for case in cases {
        if !series.insert(case.series()) {
            return Err(format!("duplicate duration series {:?}", case.series()));
        }
    }
    for observation in observations {
        if !series.insert(observation.series()) {
            return Err(format!(
                "duplicate sample or observation series {:?}",
                observation.series()
            ));
        }
    }

    let mut pairs = BTreeMap::<&str, [Option<(usize, usize)>; 2]>::new();
    for (case_index, case) in cases.iter().enumerate() {
        let Some(pairing) = case.pairing() else {
            continue;
        };
        let side = match pairing.side() {
            PairSide::First => 0,
            PairSide::Second => 1,
        };
        let slot = &mut pairs.entry(pairing.pair()).or_default()[side];
        if slot.replace((case_index, case.samples().get())).is_some() {
            return Err(format!(
                "pair {:?} declares the same semantic side twice",
                pairing.pair()
            ));
        }
    }
    for (pair, [first, second]) in pairs {
        let (_, first) = first.ok_or_else(|| format!("pair {pair:?} has no first side"))?;
        let (_, second) = second.ok_or_else(|| format!("pair {pair:?} has no second side"))?;
        if first != second {
            return Err(format!(
                "pair {pair:?} sides require different sample counts: {first} and {second}"
            ));
        }
    }
    Ok(())
}

fn strictly_sorted<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut previous = None;
    for value in values {
        if previous.is_some_and(|previous| previous >= value) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn validate_catalog_fingerprint(fingerprint: &PlanFingerprint) -> Result<(), String> {
    if fingerprint.digest.len() != 32
        || !fingerprint
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(format!(
            "plan catalog digest must contain 32 lowercase hexadecimal digits, found {:?}",
            fingerprint.digest
        ));
    }
    Ok(())
}

fn catalog_fingerprint(
    expected_benchmark: &str,
    expected_profile: &str,
    catalog: &str,
) -> Result<PlanFingerprint, String> {
    let catalog = serde_json::from_str::<PlanCatalog>(catalog)
        .map_err(|error| format!("decode benchmark plan catalog: {error}"))?;
    if catalog.benchmark != expected_benchmark {
        return Err(format!(
            "plan catalog belongs to benchmark {:?}, expected {expected_benchmark:?}",
            catalog.benchmark
        ));
    }
    if catalog.algorithm != PLAN_ALGORITHM {
        return Err(format!(
            "plan catalog algorithm must be {PLAN_ALGORITHM:?}, found {:?}",
            catalog.algorithm
        ));
    }
    let expected = match expected_profile {
        "smoke" => catalog.smoke,
        "reference" => catalog.reference,
        _ => {
            return Err(format!(
                "benchmark profile must be smoke or reference, found {expected_profile:?}"
            ));
        }
    };
    validate_catalog_fingerprint(&expected)?;
    Ok(expected)
}

fn compare_plan(
    expected_benchmark: &str,
    expected_profile: &str,
    expected: &PlanFingerprint,
    artifact: &Artifact,
) -> Result<(), String> {
    let actual = artifact.plan_fingerprint();
    if actual == *expected {
        Ok(())
    } else {
        Err(format!(
            "benchmark plan changed: target={expected_benchmark:?} profile={expected_profile:?} expected(cases={}, observations={}, bytes={}, digest={}) actual(cases={}, observations={}, bytes={}, digest={})",
            expected.cases,
            expected.observations,
            expected.encoded_bytes,
            expected.digest,
            actual.cases,
            actual.observations,
            actual.encoded_bytes,
            actual.digest,
        ))
    }
}

pub(crate) fn fnv1a_128(bytes: &[u8]) -> u128 {
    bytes.iter().fold(FNV1A_128_OFFSET, |hash, byte| {
        (hash ^ u128::from(*byte)).wrapping_mul(FNV1A_128_PRIME)
    })
}

fn validate_counts(artifact: &Artifact) -> Result<(), String> {
    for (index, (case, samples)) in artifact.cases().enumerate() {
        if samples.len() != case.samples().get() {
            return Err(format!(
                "case {index} ({:?}) emitted {} of {} samples",
                case.series(),
                samples.len(),
                case.samples()
            ));
        }
    }
    for (index, (spec, values)) in artifact.observations().enumerate() {
        if values.len() != spec.samples().get() {
            return Err(format!(
                "observation {index} ({:?}) emitted {} of {} records",
                spec.series(),
                values.len(),
                spec.samples()
            ));
        }
    }
    Ok(())
}
