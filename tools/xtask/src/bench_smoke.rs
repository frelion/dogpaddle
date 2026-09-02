use std::{
    collections::{BTreeSet, HashSet},
    env,
    ffi::OsString,
    fs,
    io::{self, Write},
    path::Path,
    process::Command,
};

use dogpaddle_bench_protocol::{BENCHMARK_PLAN_ONLY_ENV, RunValidator};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BenchTarget {
    package: String,
    target: String,
    source: std::path::PathBuf,
}

impl BenchTarget {
    fn new(
        package: impl Into<String>,
        target: impl Into<String>,
        source: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            package: package.into(),
            target: target.into(),
            source: source.into(),
        }
    }

    fn catalog(&self) -> std::path::PathBuf {
        self.source.with_extension("plan.json")
    }
}

pub(crate) fn run(workspace: &Path) -> Result<(), String> {
    let targets = discover_bench_targets(workspace)?;
    if targets.is_empty() {
        return Err("cargo metadata discovered no workspace benchmark targets".to_owned());
    }
    let mut identities = HashSet::new();
    for target in &targets {
        if !identities.insert(&target.target) {
            return Err(format!(
                "benchmark target name {:?} is not unique across packages",
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

pub(crate) fn check_plans(workspace: &Path) -> Result<(), String> {
    let targets = discover_bench_targets(workspace)?;
    if targets.is_empty() {
        return Err("cargo metadata discovered no workspace benchmark targets".to_owned());
    }
    let mut failures = Vec::new();
    for target in &targets {
        let catalog = match read_catalog(target) {
            Ok(catalog) => catalog,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        for profile in ["smoke", "reference"] {
            println!(
                "plan {} --bench {} [{profile}]",
                target.package, target.target
            );
            let result =
                benchmark_output(workspace, target, profile, true, false).and_then(|output| {
                    let stdout = std::str::from_utf8(&output.stdout).map_err(|error| {
                        format!("{} stdout is not UTF-8: {error}", target.target)
                    })?;
                    RunValidator::validate_plan_catalog(&target.target, profile, stdout, &catalog)
                        .map_err(|error| {
                            format!("benchmark {}/{}: {error}", target.package, target.target)
                        })
                });
            if let Err(error) = result {
                failures.push(error);
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

pub(crate) fn validate_file(
    workspace: &Path,
    expected_benchmark: &str,
    expected_profile: &str,
    path: &Path,
) -> Result<(), String> {
    let output = fs::read_to_string(path)
        .map_err(|error| format!("read benchmark output {}: {error}", path.display()))?;
    let targets = discover_bench_targets(workspace)?;
    let mut matches = targets
        .iter()
        .filter(|target| target.target == expected_benchmark);
    let target = matches
        .next()
        .ok_or_else(|| format!("unknown benchmark target {expected_benchmark:?}"))?;
    if matches.next().is_some() {
        return Err(format!(
            "benchmark target name {expected_benchmark:?} is not unique across packages"
        ));
    }
    let catalog = read_catalog(target)?;
    RunValidator::validate_catalog(expected_benchmark, expected_profile, &output, &catalog)
        .map(|_| ())
        .map_err(|error| format!("benchmark output {}: {error}", path.display()))
}

fn run_benchmark(workspace: &Path, target: &BenchTarget) -> Result<(), String> {
    let catalog = read_catalog(target)?;
    let output = benchmark_output(workspace, target, "smoke", false, true)?;
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("{} stdout is not UTF-8: {error}", target.target))?;
    RunValidator::validate_catalog(&target.target, "smoke", stdout, &catalog)
        .map(|_| ())
        .map_err(|error| format!("benchmark {}/{}: {error}", target.package, target.target))
}

fn benchmark_output(
    workspace: &Path,
    target: &BenchTarget,
    profile: &str,
    plan_only: bool,
    forward: bool,
) -> Result<std::process::Output, String> {
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
    command.env("DOGPADDLE_BENCH_PROFILE", profile);
    if plan_only {
        command.env(BENCHMARK_PLAN_ONLY_ENV, "1");
    }
    let output = command.output().map_err(|error| {
        format!(
            "execute {} --bench {}: {error}",
            Path::new(&cargo).display(),
            target.target
        )
    })?;
    if forward || !output.status.success() {
        io::stdout()
            .write_all(&output.stdout)
            .map_err(|error| format!("forward {} stdout: {error}", target.target))?;
        io::stderr()
            .write_all(&output.stderr)
            .map_err(|error| format!("forward {} stderr: {error}", target.target))?;
    }
    if !output.status.success() {
        return Err(format!(
            "benchmark {}/{} failed with {}",
            target.package, target.target, output.status
        ));
    }
    Ok(output)
}

fn read_catalog(target: &BenchTarget) -> Result<String, String> {
    let path = target.catalog();
    fs::read_to_string(&path).map_err(|error| {
        format!(
            "benchmark {}/{} requires adjacent plan catalog {}: {error}",
            target.package,
            target.target,
            path.display()
        )
    })
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
        for target in array(package, "targets")? {
            let target = target
                .as_object()
                .ok_or_else(|| "cargo metadata target must be an object".to_owned())?;
            if array(target, "kind")?
                .iter()
                .any(|kind| kind.as_str() == Some("bench"))
            {
                targets.insert(BenchTarget::new(
                    string(package, "name")?,
                    string(target, "name")?,
                    string(target, "src_path")?,
                ));
            }
        }
    }
    Ok(targets)
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

fn is_dogpaddle_variable(name: &std::ffi::OsStr) -> bool {
    name.to_string_lossy().starts_with("DOGPADDLE_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_processes_clear_only_the_project_namespace() {
        assert!(is_dogpaddle_variable(std::ffi::OsStr::new(
            "DOGPADDLE_BENCH_PROFILE"
        )));
        assert!(!is_dogpaddle_variable(std::ffi::OsStr::new(
            "CARGO_PROFILE"
        )));
    }
}
