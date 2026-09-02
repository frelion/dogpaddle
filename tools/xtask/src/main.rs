use std::{
    env,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

mod bench_smoke;

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
        "check" => {
            require_no_arguments(arguments)?;
            check(&workspace)
        }
        "bench-smoke" => {
            require_no_arguments(arguments)?;
            bench_smoke::run(&workspace)
        }
        "bench-plan-check" => {
            require_no_arguments(arguments)?;
            bench_smoke::check_plans(&workspace)
        }
        "bench-validate" => {
            let benchmark = arguments.next().ok_or_else(usage)?;
            let profile = arguments.next().ok_or_else(usage)?;
            let path = arguments.next().ok_or_else(usage)?;
            require_no_arguments(arguments)?;
            bench_smoke::validate_file(&workspace, &benchmark, &profile, Path::new(&path))
        }
        "help" | "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(format!("unknown task {task:?}\n{}", usage())),
    }
}

fn usage() -> String {
    "usage: cargo xtask <check|bench-smoke|bench-plan-check|bench-validate BENCHMARK PROFILE FILE>"
        .to_owned()
}

fn require_no_arguments(mut arguments: impl Iterator<Item = String>) -> Result<(), String> {
    if let Some(argument) = arguments.next() {
        Err(format!("unexpected argument {argument:?}\n{}", usage()))
    } else {
        Ok(())
    }
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
}
