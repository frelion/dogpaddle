use std::{
    collections::BTreeSet,
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

#[derive(Default)]
struct StandardRecords {
    samples: Vec<Map<String, Value>>,
    summaries: Vec<Map<String, Value>>,
}

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
    let mut expected_data_records = None;
    let mut standard = StandardRecords::default();

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
                expected_data_records = Some(
                    usize::try_from(unsigned(record, "expected_data_records")?).map_err(
                        |error| format!("expected data-record count does not fit usize: {error}"),
                    )?,
                );
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
                    "sample" => standard.samples.push(record.clone()),
                    "summary" => standard.summaries.push(record.clone()),
                    _ => {}
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

    if phase != Phase::Complete {
        return Err(match phase {
            Phase::Start => "benchmark emitted no machine JSONL records".to_owned(),
            Phase::Environment => "benchmark did not emit configuration".to_owned(),
            Phase::Configuration => "benchmark emitted no data records".to_owned(),
            Phase::Data => "benchmark did not finish with completion".to_owned(),
            Phase::Complete => unreachable!(),
        });
    }
    let expected_data_records = expected_data_records.expect("complete protocol has configuration");
    if data_records != expected_data_records {
        return Err(format!(
            "benchmark emitted {data_records} data records, configuration requires {expected_data_records}"
        ));
    }
    if standard.samples.is_empty() != standard.summaries.is_empty() {
        return Err(
            "standard samples and summaries must either both be present or both be absent"
                .to_owned(),
        );
    }
    if !standard.samples.is_empty() {
        validate_standard_summaries(&standard.samples, &standard.summaries)?;
    }
    Ok(())
}

fn validate_standard_summaries(
    samples: &[Map<String, Value>],
    summaries: &[Map<String, Value>],
) -> Result<(), String> {
    let mut summary_keys = BTreeSet::new();
    let mut sample_matches = vec![0_usize; samples.len()];
    for summary in summaries {
        let key = standard_summary_key(summary)?;
        if !summary_keys.insert(key.clone()) {
            return Err(format!("duplicate summary for series {key}"));
        }
        let mut matched = Vec::new();
        for (index, sample) in samples.iter().enumerate() {
            if standard_summary_matches(summary, sample)? {
                sample_matches[index] += 1;
                matched.push(sample);
            }
        }
        if matched.is_empty() {
            return Err(format!("summary for series {key} has no matching samples"));
        }
        validate_sample_indices(&matched)?;
        let mut durations = matched
            .iter()
            .map(|sample| unsigned(sample, "elapsed_ns"))
            .collect::<Result<Vec<_>, _>>()?;
        durations.sort_unstable();
        require_unsigned(summary, "samples", durations.len() as u128, &key)?;
        require_unsigned(summary, "min_ns", durations[0], &key)?;
        require_unsigned(summary, "median_ns", durations[durations.len() / 2], &key)?;
        require_unsigned(
            summary,
            "max_ns",
            *durations.last().expect("matched samples are non-empty"),
            &key,
        )?;
    }
    for (index, matches) in sample_matches.into_iter().enumerate() {
        if matches != 1 {
            return Err(format!(
                "sample record {index} matched {matches} summaries instead of exactly one"
            ));
        }
    }
    Ok(())
}

fn standard_summary_key(summary: &Map<String, Value>) -> Result<String, String> {
    let mut identity = Map::new();
    for (name, value) in summary {
        if !matches!(
            name.as_str(),
            "record" | "samples" | "min_ns" | "median_ns" | "max_ns"
        ) {
            identity.insert(name.clone(), value.clone());
        }
    }
    serde_json::to_string(&identity).map_err(|error| format!("encode summary identity: {error}"))
}

fn standard_summary_matches(
    summary: &Map<String, Value>,
    sample: &Map<String, Value>,
) -> Result<bool, String> {
    if string(summary, "scenario")? != string(sample, "scenario")? {
        return Ok(false);
    }
    Ok(summary.iter().all(|(name, value)| {
        matches!(
            name.as_str(),
            "record" | "benchmark" | "scenario" | "samples" | "min_ns" | "median_ns" | "max_ns"
        ) || sample.get(name) == Some(value)
    }))
}

fn validate_sample_indices(records: &[&Map<String, Value>]) -> Result<(), String> {
    let mut indices = records
        .iter()
        .map(|record| {
            usize::try_from(unsigned(record, "sample")?)
                .map_err(|error| format!("sample index does not fit usize: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    indices.sort_unstable();
    for (expected, actual) in indices.into_iter().enumerate() {
        if actual != expected {
            return Err(format!(
                "sample indices must be unique and contiguous from zero; expected {expected}, found {actual}"
            ));
        }
    }
    Ok(())
}

fn require_unsigned(
    record: &Map<String, Value>,
    field: &str,
    expected: u128,
    context: &str,
) -> Result<(), String> {
    let actual = unsigned(record, field)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{context} field {field:?} is {actual}, recomputed value is {expected}"
        ))
    }
}

fn array<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Vec<Value>, String> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field {field:?} must be an array"))
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
        r#"{"record":"configuration","benchmark":"bench","expected_data_records":2}"#;
    const SAMPLE: &str =
        r#"{"record":"sample","benchmark":"bench","scenario":"case","sample":0,"elapsed_ns":10}"#;
    const SUMMARY: &str = r#"{"record":"summary","benchmark":"bench","scenario":"case","samples":1,"min_ns":10,"median_ns":10,"max_ns":10}"#;
    const COMPLETION: &str = r#"{"record":"completion","benchmark":"bench"}"#;

    #[test]
    fn output_accepts_a_generic_complete_protocol() {
        validate_output(
            "bench",
            &[ENVIRONMENT, CONFIGURATION, SAMPLE, SUMMARY, COMPLETION].join("\n"),
        )
        .unwrap();
    }

    #[test]
    fn output_accepts_owner_extensions_without_learning_their_schema() {
        let configuration =
            r#"{"record":"configuration","benchmark":"bench","expected_data_records":1}"#;
        let extension = r#"{"record":"future_owner_record","benchmark":"bench","value":1}"#;
        validate_output(
            "bench",
            &[ENVIRONMENT, configuration, extension, COMPLETION].join("\n"),
        )
        .unwrap();
        let reference_environment =
            r#"{"record":"environment","benchmark":"bench","profile":"reference"}"#;
        super::validate_output(
            "bench",
            "reference",
            &[reference_environment, configuration, extension, COMPLETION].join("\n"),
        )
        .unwrap();
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
    fn output_recomputes_standard_summaries() {
        let wrong = r#"{"record":"summary","benchmark":"bench","scenario":"case","samples":1,"min_ns":10,"median_ns":11,"max_ns":10}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, SAMPLE, wrong, COMPLETION].join("\n")
            )
            .is_err()
        );
        let wrong_count =
            r#"{"record":"configuration","benchmark":"bench","expected_data_records":3}"#;
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, wrong_count, SAMPLE, SUMMARY, COMPLETION].join("\n")
            )
            .is_err()
        );
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, SAMPLE, COMPLETION].join("\n")
            )
            .is_err()
        );
        assert!(
            validate_output(
                "bench",
                &[ENVIRONMENT, CONFIGURATION, SUMMARY, COMPLETION].join("\n")
            )
            .is_err()
        );
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
