use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs,
    io::{self, Write},
    path::Path,
    process::Command,
};

use serde_json::{Map, Value};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BenchTarget {
    package: String,
    target: String,
}

impl BenchTarget {
    fn new(package: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            package: package.into(),
            target: target.into(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Start,
    Environment,
    Configuration,
    Data,
    Complete,
}

type SeriesSamples = BTreeMap<String, Vec<usize>>;
type PairedSamples = BTreeMap<String, BTreeMap<String, (String, Vec<usize>)>>;

pub(crate) fn run(workspace: &Path) -> Result<(), String> {
    let targets = discover_bench_targets(workspace)?;
    if targets.is_empty() {
        return Err("cargo metadata discovered no workspace benchmark targets".to_owned());
    }
    let mut protocol_ids = BTreeSet::new();
    for target in &targets {
        if !protocol_ids.insert(&target.target) {
            return Err(format!(
                "benchmark target name {:?} is not unique across workspace packages",
                target.target
            ));
        }
    }

    for (index, target) in targets.iter().enumerate() {
        println!(
            "\n[{}/{}] {} --bench {}",
            index + 1,
            targets.len(),
            target.package,
            target.target
        );
        run_benchmark(workspace, target)?;
    }
    Ok(())
}

fn discover_bench_targets(workspace: &Path) -> Result<BTreeSet<BenchTarget>, String> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let output = Command::new(&cargo)
        .current_dir(workspace)
        .args(["metadata", "--no-deps", "--format-version", "1", "--locked"])
        .output()
        .map_err(|error| format!("execute {} metadata: {error}", Path::new(&cargo).display()))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let metadata: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode cargo metadata JSON: {error}"))?;
    let metadata = metadata
        .as_object()
        .ok_or_else(|| "cargo metadata root must be an object".to_owned())?;
    let members = array(metadata, "workspace_members")?
        .iter()
        .map(|member| {
            member
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "cargo metadata workspace member must be a string".to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut targets = BTreeSet::new();
    for package in array(metadata, "packages")? {
        let package = package
            .as_object()
            .ok_or_else(|| "cargo metadata package must be an object".to_owned())?;
        if !members.contains(string(package, "id")?) {
            continue;
        }
        let package_name = string(package, "name")?;
        for target in array(package, "targets")? {
            let target = target
                .as_object()
                .ok_or_else(|| "cargo metadata target must be an object".to_owned())?;
            if array(target, "kind")?
                .iter()
                .any(|kind| kind.as_str() == Some("bench"))
            {
                targets.insert(BenchTarget::new(package_name, string(target, "name")?));
            }
        }
    }
    Ok(targets)
}

fn run_benchmark(workspace: &Path, target: &BenchTarget) -> Result<(), String> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(&cargo);
    command.current_dir(workspace).args([
        "bench",
        "--locked",
        "--package",
        &target.package,
        "--bench",
        &target.target,
    ]);
    for (name, _) in env::vars_os().filter(|(name, _)| is_dogpaddle_variable(name)) {
        command.env_remove(name);
    }
    command.env("DOGPADDLE_BENCH_PROFILE", "smoke");

    let output = command.output().map_err(|error| {
        format!(
            "execute {} --bench {}: {error}",
            Path::new(&cargo).display(),
            target.target
        )
    })?;
    io::stdout()
        .write_all(&output.stdout)
        .map_err(|error| format!("forward {} stdout: {error}", target.target))?;
    io::stderr()
        .write_all(&output.stderr)
        .map_err(|error| format!("forward {} stderr: {error}", target.target))?;
    if !output.status.success() {
        return Err(format!(
            "benchmark {}/{} failed with {}",
            target.package, target.target, output.status
        ));
    }
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("{} stdout is not UTF-8: {error}", target.target))?;
    validate_output(&target.target, "smoke", stdout)
        .map_err(|error| format!("benchmark {}/{}: {error}", target.package, target.target))
}

pub(crate) fn validate_file(
    expected_benchmark: &str,
    expected_profile: &str,
    path: &Path,
) -> Result<(), String> {
    if !matches!(expected_profile, "smoke" | "reference") {
        return Err(format!(
            "benchmark profile must be smoke or reference, found {expected_profile:?}"
        ));
    }
    let stdout = fs::read_to_string(path)
        .map_err(|error| format!("read benchmark output {}: {error}", path.display()))?;
    validate_output(expected_benchmark, expected_profile, &stdout)
        .map_err(|error| format!("benchmark output {}: {error}", path.display()))
}

fn is_dogpaddle_variable(name: &std::ffi::OsStr) -> bool {
    name.to_string_lossy().starts_with("DOGPADDLE_")
}

fn validate_output(
    expected_benchmark: &str,
    expected_profile: &str,
    stdout: &str,
) -> Result<(), String> {
    let mut phase = Phase::Start;
    let mut data_records = 0_usize;
    let mut sample_records = 0_usize;
    let mut expected_samples = None;
    let mut sample_indices = SeriesSamples::new();
    let mut pair_indices = PairedSamples::new();
    let mut required_observations = BTreeMap::new();
    let mut observation_indices = SeriesSamples::new();

    for (index, line) in stdout.lines().enumerate() {
        let line = line.trim_start();
        if !line.starts_with('{') {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|error| {
            format!(
                "malformed JSONL record on stdout line {}: {error}",
                index + 1
            )
        })?;
        let record = value
            .as_object()
            .ok_or_else(|| format!("JSONL record on stdout line {} is not an object", index + 1))?;
        let benchmark = string(record, "benchmark")?;
        if benchmark != expected_benchmark {
            return Err(format!(
                "record on stdout line {} belongs to benchmark {benchmark:?}, expected Cargo target {expected_benchmark:?}",
                index + 1
            ));
        }
        let discriminator = string(record, "record")?;
        phase = match (phase, discriminator) {
            (Phase::Start, "environment") => {
                if string(record, "profile")? != expected_profile {
                    return Err(format!(
                        "environment record must report the {expected_profile} profile"
                    ));
                }
                Phase::Environment
            }
            (Phase::Environment, "configuration") => {
                let (samples, observations) = configuration_requirements(record)?;
                expected_samples = Some(samples);
                required_observations = observations;
                Phase::Configuration
            }
            (Phase::Configuration | Phase::Data, "completion") if data_records > 0 => {
                Phase::Complete
            }
            (Phase::Configuration | Phase::Data, "environment" | "configuration") => {
                return Err(format!(
                    "duplicate or out-of-order {discriminator} record on stdout line {}",
                    index + 1
                ));
            }
            (Phase::Configuration | Phase::Data, _) => {
                data_records += 1;
                match discriminator {
                    "sample" => {
                        sample_records += 1;
                        record_sample(record, &mut sample_indices, &mut pair_indices)?;
                    }
                    "observation" => record_observation(record, &mut observation_indices)?,
                    "summary" | "pair_summary" => {
                        return Err(format!(
                            "retired derived record {discriminator:?} on stdout line {}; emit raw samples instead",
                            index + 1
                        ));
                    }
                    _ => {
                        return Err(format!(
                            "unknown data record {discriminator:?} on stdout line {}; expected sample or observation",
                            index + 1
                        ));
                    }
                }
                Phase::Data
            }
            (Phase::Complete, _) => {
                return Err(format!(
                    "machine record {discriminator:?} appears after completion on stdout line {}",
                    index + 1
                ));
            }
            _ => {
                return Err(format!(
                    "out-of-order {discriminator:?} record on stdout line {}",
                    index + 1
                ));
            }
        };
    }

    finish_validation(
        phase,
        sample_records,
        expected_samples,
        sample_indices,
        pair_indices,
        &required_observations,
        &observation_indices,
    )
}

fn finish_validation(
    phase: Phase,
    sample_records: usize,
    expected_samples: Option<usize>,
    sample_indices: SeriesSamples,
    pair_indices: PairedSamples,
    required_observations: &BTreeMap<String, usize>,
    observation_indices: &SeriesSamples,
) -> Result<(), String> {
    if phase != Phase::Complete {
        return Err(match phase {
            Phase::Start => "benchmark emitted no machine JSONL records".to_owned(),
            Phase::Environment => "benchmark did not emit configuration".to_owned(),
            Phase::Configuration => "benchmark emitted no data records".to_owned(),
            Phase::Data => "benchmark did not finish with completion".to_owned(),
            Phase::Complete => unreachable!(),
        });
    }
    let expected_samples = expected_samples.expect("complete protocol has configuration");
    if sample_records != expected_samples {
        return Err(format!(
            "benchmark emitted {sample_records} samples, configuration requires {expected_samples}"
        ));
    }
    for (series, indices) in sample_indices {
        validate_series_indices("sample", &series, &indices)?;
    }
    for (pair, sides) in pair_indices {
        validate_pair_indices(&pair, &sides)?;
    }
    validate_observations(required_observations, observation_indices)?;
    Ok(())
}

fn record_observation(
    record: &Map<String, Value>,
    observations: &mut SeriesSamples,
) -> Result<(), String> {
    let series = string(record, "series")?.to_owned();
    if series.is_empty() {
        return Err("observation series must not be empty".to_owned());
    }
    let sample = usize::try_from(unsigned(record, "sample")?)
        .map_err(|error| format!("observation index does not fit usize: {error}"))?;
    observations.entry(series).or_default().push(sample);
    Ok(())
}

fn record_sample(
    record: &Map<String, Value>,
    samples: &mut SeriesSamples,
    pairs: &mut PairedSamples,
) -> Result<(), String> {
    let series = string(record, "series")?.to_owned();
    if series.is_empty() {
        return Err("sample series must not be empty".to_owned());
    }
    let sample = usize::try_from(unsigned(record, "sample")?)
        .map_err(|error| format!("sample index does not fit usize: {error}"))?;
    unsigned(record, "elapsed_ns")?;
    samples.entry(series.clone()).or_default().push(sample);

    match (record.get("pair"), record.get("side")) {
        (None, None) => Ok(()),
        (Some(_), None) | (None, Some(_)) => Err(format!(
            "sample series {series:?} must provide pair and side together"
        )),
        (Some(_), Some(_)) => {
            let pair = string(record, "pair")?;
            if pair.is_empty() {
                return Err("sample pair must not be empty".to_owned());
            }
            let side = string(record, "side")?;
            if !matches!(side, "first" | "second") {
                return Err(format!(
                    "sample pair {pair:?} side must be first or second, found {side:?}"
                ));
            }
            let entry = pairs
                .entry(pair.to_owned())
                .or_default()
                .entry(side.to_owned())
                .or_insert_with(|| (series.clone(), Vec::new()));
            if entry.0 != series {
                return Err(format!(
                    "sample pair {pair:?} side {side:?} is shared by series {:?} and {series:?}",
                    entry.0
                ));
            }
            entry.1.push(sample);
            Ok(())
        }
    }
}

fn validate_series_indices(kind: &str, series: &str, records: &[usize]) -> Result<(), String> {
    let mut indices = records.to_vec();
    indices.sort_unstable();
    for (expected, actual) in indices.into_iter().enumerate() {
        if actual != expected {
            return Err(format!(
                "{kind} series {series:?} must have unique contiguous indices from zero; expected {expected}, found {actual}"
            ));
        }
    }
    Ok(())
}

fn validate_pair_indices(
    pair: &str,
    sides: &BTreeMap<String, (String, Vec<usize>)>,
) -> Result<(), String> {
    let (_, first) = sides
        .get("first")
        .ok_or_else(|| format!("sample pair {pair:?} has no first side"))?;
    let (_, second) = sides
        .get("second")
        .ok_or_else(|| format!("sample pair {pair:?} has no second side"))?;
    let mut first = first.clone();
    let mut second = second.clone();
    first.sort_unstable();
    second.sort_unstable();
    if first != second {
        return Err(format!(
            "sample pair {pair:?} sides have different sample indices: first={first:?}, second={second:?}"
        ));
    }
    Ok(())
}

fn validate_observations(
    required: &BTreeMap<String, usize>,
    actual: &SeriesSamples,
) -> Result<(), String> {
    for (series, indices) in actual {
        let Some(expected) = required.get(series) else {
            return Err(format!(
                "observation series {series:?} is not declared by `required_observations`"
            ));
        };
        validate_series_indices("observation", series, indices)?;
        if indices.len() != *expected {
            return Err(format!(
                "observation series {series:?} emitted {} records, configuration requires {expected}",
                indices.len()
            ));
        }
    }
    if let Some(missing) = required.keys().find(|series| !actual.contains_key(*series)) {
        return Err(format!(
            "required observation series {missing:?} is missing"
        ));
    }
    Ok(())
}

fn configuration_requirements(
    configuration: &Map<String, Value>,
) -> Result<(usize, BTreeMap<String, usize>), String> {
    let samples = usize::try_from(unsigned(configuration, "expected_samples")?)
        .map_err(|error| format!("expected sample count does not fit usize: {error}"))?;
    if samples == 0 {
        return Err("field `expected_samples` must be non-zero".to_owned());
    }
    Ok((samples, observation_requirements(configuration)?))
}

fn array<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Vec<Value>, String> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field {field:?} must be an array"))
}

fn observation_requirements(
    configuration: &Map<String, Value>,
) -> Result<BTreeMap<String, usize>, String> {
    let Some(value) = configuration.get("required_observations") else {
        return Ok(BTreeMap::new());
    };
    let requirements = value
        .as_object()
        .ok_or_else(|| "field `required_observations` must be an object".to_owned())?;
    if requirements.is_empty() {
        return Err("field `required_observations` must not be empty".to_owned());
    }
    let mut result = BTreeMap::new();
    for (series, value) in requirements {
        if series.is_empty() {
            return Err("observation series must not be empty".to_owned());
        }
        let count = usize::try_from(unsigned_value(value, "required_observations count")?)
            .map_err(|error| format!("observation count does not fit usize: {error}"))?;
        if count == 0 {
            return Err(format!(
                "required observation series {series:?} must have a non-zero count"
            ));
        }
        result.insert(series.clone(), count);
    }
    Ok(result)
}

fn string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field {field:?} must be a string"))
}

fn unsigned(object: &Map<String, Value>, field: &str) -> Result<u128, String> {
    let value = object
        .get(field)
        .ok_or_else(|| format!("missing unsigned integer field {field:?}"))?;
    unsigned_value(value, field)
}

fn unsigned_value(value: &Value, field: &str) -> Result<u128, String> {
    let Value::Number(number) = value else {
        return Err(format!("field {field:?} must be an unsigned integer"));
    };
    number
        .to_string()
        .parse::<u128>()
        .map_err(|error| format!("field {field:?} must be an unsigned integer: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate_output(expected_benchmark: &str, stdout: &str) -> Result<(), String> {
        super::validate_output(expected_benchmark, "smoke", stdout)
    }

    const ENVIRONMENT: &str = r#"{"record":"environment","benchmark":"bench","profile":"smoke"}"#;
    const CONFIGURATION: &str =
        r#"{"record":"configuration","benchmark":"bench","expected_samples":1}"#;
    const SAMPLE: &str =
        r#"{"record":"sample","benchmark":"bench","series":"case","sample":0,"elapsed_ns":10}"#;
    const COMPLETION: &str = r#"{"record":"completion","benchmark":"bench"}"#;

    #[test]
    fn output_accepts_a_generic_complete_protocol() {
        validate_output(
            "bench",
            &[ENVIRONMENT, CONFIGURATION, SAMPLE, COMPLETION].join("\n"),
        )
        .unwrap();
    }

    #[test]
    fn output_accepts_exact_declared_observations_without_owner_schema() {
        let configuration = r#"{"record":"configuration","benchmark":"bench","expected_samples":1,"required_observations":{"record_bytes=128/checkpoint":2,"record_bytes=128/terminal":1}}"#;
        let checkpoint_0 = r#"{"record":"observation","benchmark":"bench","series":"record_bytes=128/checkpoint","sample":0,"epoch":0}"#;
        let checkpoint_1 = r#"{"record":"observation","benchmark":"bench","series":"record_bytes=128/checkpoint","sample":1,"epoch":8}"#;
        let terminal = r#"{"record":"observation","benchmark":"bench","series":"record_bytes=128/terminal","sample":0,"checksum":"owner-defined"}"#;
        validate_output(
            "bench",
            &[
                ENVIRONMENT,
                configuration,
                SAMPLE,
                checkpoint_0,
                checkpoint_1,
                terminal,
                COMPLETION,
            ]
            .join("\n"),
        )
        .unwrap();
        let reference_environment =
            r#"{"record":"environment","benchmark":"bench","profile":"reference"}"#;
        super::validate_output(
            "bench",
            "reference",
            &[
                reference_environment,
                configuration,
                SAMPLE,
                checkpoint_0,
                checkpoint_1,
                terminal,
                COMPLETION,
            ]
            .join("\n"),
        )
        .unwrap();
    }

    #[test]
    fn output_rejects_missing_or_duplicate_required_observations() {
        let required = r#"{"record":"configuration","benchmark":"bench","expected_samples":1,"required_observations":{"record_bytes=128/terminal":1}}"#;
        let missing = validate_output(
            "bench",
            &[ENVIRONMENT, required, SAMPLE, COMPLETION].join("\n"),
        )
        .unwrap_err();
        assert!(missing.contains("required observation series"));
        assert!(missing.contains("is missing"));

        let terminal = r#"{"record":"observation","benchmark":"bench","series":"record_bytes=128/terminal","sample":0}"#;
        let duplicate = validate_output(
            "bench",
            &[
                ENVIRONMENT,
                required,
                SAMPLE,
                terminal,
                terminal,
                COMPLETION,
            ]
            .join("\n"),
        )
        .unwrap_err();
        assert!(duplicate.contains("must have unique contiguous indices"));
    }

    #[test]
    fn output_rejects_undeclared_observations_and_unknown_data_records() {
        let terminal = r#"{"record":"observation","benchmark":"bench","series":"record_bytes=128/terminal","sample":0}"#;
        let undeclared = validate_output(
            "bench",
            &[ENVIRONMENT, CONFIGURATION, SAMPLE, terminal, COMPLETION].join("\n"),
        )
        .unwrap_err();
        assert!(undeclared.contains("is not declared"));

        let unknown = r#"{"record":"future_owner_record","benchmark":"bench","value":1}"#;
        let unexpected = validate_output(
            "bench",
            &[ENVIRONMENT, CONFIGURATION, SAMPLE, unknown, COMPLETION].join("\n"),
        )
        .unwrap_err();
        assert!(unexpected.contains("unknown data record"));
    }

    #[test]
    fn output_rejects_missing_or_out_of_order_envelopes() {
        assert!(validate_output("bench", "human output only").is_err());
        assert!(validate_output("bench", &[CONFIGURATION, SAMPLE, COMPLETION].join("\n")).is_err());
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, COMPLETION].join("\n")
            )
            .is_err()
        );
        assert!(
            validate_output("bench", &[ENVIRONMENT, CONFIGURATION, SAMPLE].join("\n")).is_err()
        );
    }

    #[test]
    fn output_rejects_malformed_identity_and_records_after_completion() {
        assert!(validate_output("bench", "{not json").is_err());
        let other = r#"{"record":"sample","benchmark":"other"}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, other, COMPLETION].join("\n")
            )
            .is_err()
        );
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, SAMPLE, COMPLETION, SAMPLE].join("\n")
            )
            .is_err()
        );
    }

    #[test]
    fn output_rejects_invalid_raw_sample_series() {
        let duplicate_configuration =
            r#"{"record":"configuration","benchmark":"bench","expected_samples":2}"#;
        let duplicate =
            r#"{"record":"sample","benchmark":"bench","series":"case","sample":0,"elapsed_ns":11}"#;
        assert!(
            validate_output(
                "bench",
                &[
                    ENVIRONMENT,
                    duplicate_configuration,
                    SAMPLE,
                    duplicate,
                    COMPLETION,
                ]
                .join("\n")
            )
            .is_err()
        );
        let gap =
            r#"{"record":"sample","benchmark":"bench","series":"case","sample":2,"elapsed_ns":11}"#;
        assert!(
            validate_output(
                "bench",
                &[
                    ENVIRONMENT,
                    duplicate_configuration,
                    SAMPLE,
                    gap,
                    COMPLETION,
                ]
                .join("\n")
            )
            .is_err()
        );
        let missing_series =
            r#"{"record":"sample","benchmark":"bench","sample":0,"elapsed_ns":10}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, missing_series, COMPLETION].join("\n")
            )
            .is_err()
        );
    }

    #[test]
    fn output_validates_lossless_raw_pairs() {
        let configuration =
            r#"{"record":"configuration","benchmark":"bench","expected_samples":2}"#;
        let first = r#"{"record":"sample","benchmark":"bench","series":"case/first","sample":0,"elapsed_ns":10,"pair":"case","side":"first"}"#;
        let second = r#"{"record":"sample","benchmark":"bench","series":"case/second","sample":0,"elapsed_ns":11,"pair":"case","side":"second"}"#;
        validate_output(
            "bench",
            &[ENVIRONMENT, configuration, first, second, COMPLETION].join("\n"),
        )
        .unwrap();

        let missing_side = r#"{"record":"sample","benchmark":"bench","series":"case/first","sample":0,"elapsed_ns":10,"pair":"case"}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, missing_side, COMPLETION].join("\n")
            )
            .is_err()
        );
        let only_first = r#"{"record":"sample","benchmark":"bench","series":"case/first","sample":0,"elapsed_ns":10,"pair":"case","side":"first"}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, only_first, COMPLETION].join("\n")
            )
            .is_err()
        );

        let invalid_side = r#"{"record":"sample","benchmark":"bench","series":"case/first","sample":0,"elapsed_ns":10,"pair":"case","side":"left"}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, invalid_side, COMPLETION].join("\n")
            )
            .is_err()
        );

        let three_records =
            r#"{"record":"configuration","benchmark":"bench","expected_samples":3}"#;
        let first_1 = r#"{"record":"sample","benchmark":"bench","series":"case/first","sample":1,"elapsed_ns":12,"pair":"case","side":"first"}"#;
        assert!(
            validate_output(
                "bench",
                &[
                    ENVIRONMENT,
                    three_records,
                    first,
                    first_1,
                    second,
                    COMPLETION,
                ]
                .join("\n")
            )
            .is_err()
        );
    }

    #[test]
    fn output_rejects_retired_derived_records_and_wrong_counts() {
        let summary = r#"{"record":"summary","benchmark":"bench"}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, summary, COMPLETION].join("\n")
            )
            .is_err()
        );
        let wrong_count = r#"{"record":"configuration","benchmark":"bench","expected_samples":2}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, wrong_count, SAMPLE, COMPLETION].join("\n")
            )
            .is_err()
        );
        let zero_count = r#"{"record":"configuration","benchmark":"bench","expected_samples":0}"#;
        let error = validate_output(
            "bench",
            &[ENVIRONMENT, zero_count, SAMPLE, COMPLETION].join("\n"),
        )
        .unwrap_err();
        assert!(error.contains("`expected_samples` must be non-zero"));
    }

    #[test]
    fn isolated_benchmarks_clear_the_project_namespace() {
        assert!(is_dogpaddle_variable(std::ffi::OsStr::new(
            "DOGPADDLE_BENCH_PROFILE"
        )));
        assert!(!is_dogpaddle_variable(std::ffi::OsStr::new(
            "CARGO_PROFILE"
        )));
    }
}
