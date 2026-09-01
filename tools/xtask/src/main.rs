use std::{
    collections::BTreeSet,
    env,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

mod change_store_reference;

const BENCH_SMOKE: &[BenchSpec] = &[
    BenchSpec::new(
        "dogpaddle-change",
        "change_core",
        &[
            ("DOGPADDLE_BENCH_CHANGE_ROWS", "4"),
            ("DOGPADDLE_BENCH_CHANGE_PAYLOAD_BYTES", "16"),
            ("DOGPADDLE_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_BENCH_CHANGE_TARGET_ROWS", "4"),
            ("DOGPADDLE_BENCH_CHANGE_MAX_CHANGES", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-change",
        "change_codec",
        &[
            ("DOGPADDLE_BENCH_CHANGE_ROWS", "4"),
            ("DOGPADDLE_BENCH_CHANGE_PAYLOAD_BYTES", "16"),
            ("DOGPADDLE_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_BENCH_CHANGE_TARGET_ROWS", "4"),
            ("DOGPADDLE_BENCH_CHANGE_MAX_CHANGES", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-store",
        "cell",
        &[
            ("DOGPADDLE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_BENCH_CELL_READS", "1"),
            ("DOGPADDLE_BENCH_COMMITS", "1"),
            ("DOGPADDLE_BENCH_SAMPLES", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-store",
        "ordered_map",
        &[
            ("DOGPADDLE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_BENCH_ENTRIES", "4"),
            ("DOGPADDLE_BENCH_COMMITS", "1"),
            ("DOGPADDLE_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_BENCH_BACKGROUND_NAMESPACES", "1"),
            ("DOGPADDLE_BENCH_SCAN_ITEMS", "2"),
            ("DOGPADDLE_BENCH_SCAN_BYTES", "16384"),
            ("DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES", "2"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-store",
        "append_log",
        &[
            ("DOGPADDLE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_BENCH_LOG_ENTRIES", "3"),
            ("DOGPADDLE_BENCH_COMMITS", "1"),
            ("DOGPADDLE_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_BENCH_LOG_RECORD_BYTES", "16,64"),
            ("DOGPADDLE_BENCH_LOG_SOURCE_BATCH_ITEMS", "1,2"),
            ("DOGPADDLE_BENCH_LOG_STATION_RECORD_BYTES", "16"),
            ("DOGPADDLE_BENCH_LOG_STATION_BATCH_ITEMS", "1"),
            ("DOGPADDLE_BENCH_LOG_GC_ITEMS", "1"),
            ("DOGPADDLE_BENCH_LOG_READERS", "1,2"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-store",
        "append_log_endurance",
        &[
            ("DOGPADDLE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_STORE_ENDURANCE_PROFILE", "smoke"),
            ("DOGPADDLE_STORE_ENDURANCE_RECORD_BYTES", "128"),
            ("DOGPADDLE_STORE_ENDURANCE_LOGICAL_MIB", "2"),
            ("DOGPADDLE_STORE_ENDURANCE_WINDOW_MIB", "1"),
            ("DOGPADDLE_STORE_ENDURANCE_BATCH_MIB", "1"),
            ("DOGPADDLE_STORE_ENDURANCE_CHECKPOINT_EPOCHS", "1"),
            (
                "DOGPADDLE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES",
                "67108864",
            ),
            (
                "DOGPADDLE_STORE_ENDURANCE_MAX_TOTAL_WRITTEN_BYTES",
                "67108864",
            ),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-operation",
        "operation_core",
        &[
            ("DOGPADDLE_OPERATION_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_OPERATION_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_OPERATION_BENCH_CODEC_OPERATIONS", "1"),
            (
                "DOGPADDLE_OPERATION_BENCH_BODY_TRANSACTIONS_PER_SAMPLE",
                "1",
            ),
            (
                "DOGPADDLE_OPERATION_BENCH_DURABLE_TRANSACTIONS_PER_SAMPLE",
                "1",
            ),
            ("DOGPADDLE_OPERATION_BENCH_WARMUP_TRANSACTIONS", "1"),
            ("DOGPADDLE_OPERATION_BENCH_TURNS_PER_TRANSACTION", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-flow",
        "flow_lifecycle",
        &[
            ("DOGPADDLE_FLOW_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_FLOW_BENCH_STATION_COUNTS", "2,3"),
            ("DOGPADDLE_FLOW_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_FLOW_BENCH_WARMUPS", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-flow",
        "flow_runtime",
        &[
            ("DOGPADDLE_FLOW_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_FLOW_RUNTIME_BENCH_CHAIN_STATIONS", "3"),
            ("DOGPADDLE_FLOW_RUNTIME_BENCH_FANOUTS", "2"),
            ("DOGPADDLE_FLOW_RUNTIME_BENCH_ROUNDS_PER_SAMPLE", "1"),
            ("DOGPADDLE_FLOW_RUNTIME_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_FLOW_RUNTIME_BENCH_WARMUP_ROUNDS", "1"),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-change-store-integration",
        "change_append_log",
        &[
            ("DOGPADDLE_CHANGE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_ROWS_PER_CHANGE", "1"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_CHANGES_PER_TX", "1"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_TRANSACTIONS_PER_SAMPLE", "2"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_PAYLOAD_BYTES", "1"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_SAMPLES", "1"),
            ("DOGPADDLE_CHANGE_STORE_BENCH_WARMUPS", "1"),
            (
                "DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES",
                "67108864",
            ),
        ],
    ),
    BenchSpec::new(
        "dogpaddle-change-store-integration",
        "change_append_log_endurance",
        &[
            ("DOGPADDLE_CHANGE_STORE_BENCH_PROFILE", "smoke"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE", "smoke"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_ROWS_PER_CHANGE", "1"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_CHANGES_PER_CYCLE", "1"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_CYCLES", "2"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_PAYLOAD_BYTES", "1"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_RETAINED_BYTES", "65536"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_TRUNCATE_ITEMS", "1"),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_ITEMS", "1"),
            (
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_BYTES",
                "1048576",
            ),
            (
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_REOPEN_INTERVAL_CYCLES",
                "1",
            ),
            ("DOGPADDLE_CHANGE_STORE_ENDURANCE_WORKLOAD_MODE", "all"),
            (
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES",
                "67108864",
            ),
            (
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_TOTAL_WRITTEN_BYTES",
                "67108864",
            ),
        ],
    ),
];

#[derive(Clone, Copy)]
struct BenchSpec {
    package: &'static str,
    target: &'static str,
    environment: &'static [(&'static str, &'static str)],
}

impl BenchSpec {
    const fn new(
        package: &'static str,
        target: &'static str,
        environment: &'static [(&'static str, &'static str)],
    ) -> Self {
        Self {
            package,
            target,
            environment,
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    let task = arguments.next().ok_or_else(usage)?;
    let workspace = workspace_root();
    match task.as_str() {
        "check" => check(&workspace),
        "bench-smoke" => bench_smoke(&workspace),
        "change-store-reference" => {
            change_store_reference::run(&workspace, arguments.collect::<Vec<_>>())
        }
        "help" | "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(format!("unknown task {task:?}\n{}", usage())),
    }
}

fn usage() -> String {
    "usage: cargo xtask <check|bench-smoke|change-store-reference>\n\
     change-store-reference --store-dir <absolute-path> --output-dir <new-absolute-path> \
     [--target normal|endurance] [--runs N]"
        .to_owned()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask lives at tools/xtask")
        .to_path_buf()
}

fn check(workspace: &Path) -> Result<(), String> {
    run_cargo(
        workspace,
        ["fmt", "--all", "--", "--check"],
        CargoEnvironment::Inherit,
    )?;
    run_cargo(
        workspace,
        ["test", "--workspace", "--locked"],
        CargoEnvironment::Inherit,
    )?;
    run_cargo(
        workspace,
        ["test", "--workspace", "--release", "--locked"],
        CargoEnvironment::Inherit,
    )?;
    run_cargo(
        workspace,
        [
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
        CargoEnvironment::Inherit,
    )?;
    run_cargo(
        workspace,
        ["doc", "--workspace", "--no-deps", "--locked"],
        CargoEnvironment::Overlay(&[("RUSTDOCFLAGS", "-D warnings")]),
    )
}

fn bench_smoke(workspace: &Path) -> Result<(), String> {
    validate_bench_specs()?;
    for (index, spec) in BENCH_SMOKE.iter().enumerate() {
        println!(
            "\n[{}/{}] {} --bench {}",
            index + 1,
            BENCH_SMOKE.len(),
            spec.package,
            spec.target
        );
        run_cargo(
            workspace,
            [
                "bench",
                "--locked",
                "--package",
                spec.package,
                "--bench",
                spec.target,
            ],
            CargoEnvironment::IsolatedBenchmark(spec.environment),
        )?;
    }
    Ok(())
}

fn validate_bench_specs() -> Result<(), String> {
    let mut targets = BTreeSet::new();
    for spec in BENCH_SMOKE {
        if !targets.insert((spec.package, spec.target)) {
            return Err(format!(
                "duplicate benchmark smoke target {}/{}",
                spec.package, spec.target
            ));
        }
        let mut variables = BTreeSet::new();
        for (name, value) in spec.environment {
            if name.is_empty() || value.is_empty() {
                return Err(format!(
                    "empty benchmark smoke setting for {}/{}",
                    spec.package, spec.target
                ));
            }
            if !variables.insert(*name) {
                return Err(format!(
                    "duplicate environment variable {name} for {}/{}",
                    spec.package, spec.target
                ));
            }
        }
    }
    if BENCH_SMOKE.len() != 11 {
        return Err(format!(
            "benchmark smoke matrix must contain 11 explicit targets, found {}",
            BENCH_SMOKE.len()
        ));
    }
    Ok(())
}

fn run_cargo<I, S>(
    workspace: &Path,
    arguments: I,
    environment: CargoEnvironment<'_>,
) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<OsString>>();
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(&cargo);
    command.current_dir(workspace).args(&arguments);
    match environment {
        CargoEnvironment::Inherit => {}
        CargoEnvironment::Overlay(settings) => {
            command.envs(settings.iter().copied());
        }
        CargoEnvironment::IsolatedBenchmark(settings) => {
            for (name, _) in env::vars_os().filter(|(name, _)| is_dogpaddle_variable(name)) {
                command.env_remove(name);
            }
            command.envs(settings.iter().copied());
        }
    }
    let status = command
        .status()
        .map_err(|error| format!("failed to execute {}: {error}", Path::new(&cargo).display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "cargo {} failed with {status}",
            arguments
                .iter()
                .map(|argument| argument.to_string_lossy())
                .collect::<Vec<_>>()
                .join(" ")
        ))
    }
}

#[derive(Clone, Copy)]
enum CargoEnvironment<'a> {
    Inherit,
    Overlay(&'a [(&'a str, &'a str)]),
    IsolatedBenchmark(&'a [(&'a str, &'a str)]),
}

fn is_dogpaddle_variable(name: &OsStr) -> bool {
    name.to_string_lossy().starts_with("DOGPADDLE_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_smoke_matrix_is_complete_and_unambiguous() {
        validate_bench_specs().unwrap();
        assert_eq!(BENCH_SMOKE.len(), 11);
    }

    #[test]
    fn isolated_benchmarks_recognize_only_the_project_namespace() {
        assert!(is_dogpaddle_variable(OsStr::new("DOGPADDLE_CARGO_PROFILE")));
        assert!(is_dogpaddle_variable(OsStr::new(
            "DOGPADDLE_BENCH_CHANGE_WORKLOADS"
        )));
        assert!(!is_dogpaddle_variable(OsStr::new("CARGO_PROFILE")));
        assert!(!is_dogpaddle_variable(OsStr::new("DOG_PADDLE_PROFILE")));
    }
}
