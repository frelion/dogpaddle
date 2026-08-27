use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

const PACKAGE: &str = "dogpaddle-change-store-integration";
const PROFILE_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR";
const ENDURANCE_PROFILE_ENV: &str = "DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE";

pub(crate) fn run(workspace: &Path, arguments: Vec<String>) -> Result<(), String> {
    let configuration = Configuration::parse(arguments)?;
    configuration.prepare_directories()?;
    for run in 1..=configuration.runs {
        execute(workspace, &configuration, run)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    Normal,
    Endurance,
}

impl Target {
    const fn bench(self) -> &'static str {
        match self {
            Self::Normal => "change_append_log",
            Self::Endurance => "change_append_log_endurance",
        }
    }

    const fn default_runs(self) -> usize {
        match self {
            Self::Normal => 5,
            Self::Endurance => 1,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Configuration {
    store_dir: PathBuf,
    output_dir: PathBuf,
    target: Target,
    runs: usize,
}

impl Configuration {
    fn parse(arguments: Vec<String>) -> Result<Self, String> {
        let mut store_dir = None;
        let mut output_dir = None;
        let mut target = Target::Normal;
        let mut runs = None;
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--store-dir" => {
                    set_once(
                        &mut store_dir,
                        PathBuf::from(required_value(&mut arguments, "--store-dir")?),
                        "--store-dir",
                    )?;
                }
                "--output-dir" => {
                    set_once(
                        &mut output_dir,
                        PathBuf::from(required_value(&mut arguments, "--output-dir")?),
                        "--output-dir",
                    )?;
                }
                "--target" => {
                    let value = required_value(&mut arguments, "--target")?;
                    target = match value.as_str() {
                        "normal" => Target::Normal,
                        "endurance" => Target::Endurance,
                        _ => return Err("--target must be normal or endurance".to_owned()),
                    };
                }
                "--runs" => {
                    let value = required_value(&mut arguments, "--runs")?;
                    let value = value
                        .parse::<usize>()
                        .map_err(|error| format!("invalid --runs value {value:?}: {error}"))?;
                    if value == 0 {
                        return Err("--runs must be positive".to_owned());
                    }
                    set_once(&mut runs, value, "--runs")?;
                }
                _ => {
                    return Err(format!(
                        "unknown change-store-reference argument {argument:?}"
                    ));
                }
            }
        }
        let store_dir = store_dir.ok_or_else(|| "missing --store-dir".to_owned())?;
        let output_dir = output_dir.ok_or_else(|| "missing --output-dir".to_owned())?;
        if !store_dir.is_absolute() {
            return Err("--store-dir must be absolute".to_owned());
        }
        if !output_dir.is_absolute() {
            return Err("--output-dir must be absolute".to_owned());
        }
        if store_dir == output_dir {
            return Err("--store-dir and --output-dir must differ".to_owned());
        }
        Ok(Self {
            store_dir,
            output_dir,
            target,
            runs: runs.unwrap_or_else(|| target.default_runs()),
        })
    }

    fn prepare_directories(&self) -> Result<(), String> {
        fs::create_dir_all(&self.store_dir).map_err(|error| {
            format!(
                "create reference Store directory {}: {error}",
                self.store_dir.display()
            )
        })?;
        if self.output_dir.exists() {
            return Err(format!(
                "reference output directory already exists: {}",
                self.output_dir.display()
            ));
        }
        fs::create_dir_all(&self.output_dir).map_err(|error| {
            format!(
                "create reference output directory {}: {error}",
                self.output_dir.display()
            )
        })
    }
}

fn execute(workspace: &Path, configuration: &Configuration, run: usize) -> Result<(), String> {
    println!(
        "[{run}/{}] reference --bench {}",
        configuration.runs,
        configuration.target.bench()
    );
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(&cargo);
    command.current_dir(workspace).args([
        "bench",
        "--locked",
        "--package",
        PACKAGE,
        "--bench",
        configuration.target.bench(),
    ]);
    for (name, _) in env::vars_os().filter(|(name, _)| is_dogpaddle_variable(name)) {
        command.env_remove(name);
    }
    command
        .env(PROFILE_ENV, "reference")
        .env(STORE_DIR_ENV, &configuration.store_dir);
    if configuration.target == Target::Endurance {
        command.env(ENDURANCE_PROFILE_ENV, "full");
    }
    let output = command
        .output()
        .map_err(|error| format!("execute {}: {error}", Path::new(&cargo).display()))?;
    let prefix = format!("{}-run-{run:02}", configuration.target.bench());
    let stdout_path = configuration
        .output_dir
        .join(format!("{prefix}.stdout.log"));
    let stderr_path = configuration
        .output_dir
        .join(format!("{prefix}.stderr.log"));
    fs::write(&stdout_path, &output.stdout)
        .map_err(|error| format!("write {}: {error}", stdout_path.display()))?;
    fs::write(&stderr_path, &output.stderr)
        .map_err(|error| format!("write {}: {error}", stderr_path.display()))?;
    std::io::stdout()
        .write_all(&output.stdout)
        .map_err(|error| format!("forward benchmark stdout: {error}"))?;
    std::io::stderr()
        .write_all(&output.stderr)
        .map_err(|error| format!("forward benchmark stderr: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "reference benchmark run {run} failed with {}; raw output is in {}",
            output.status,
            configuration.output_dir.display()
        ))
    }
}

fn required_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value after {option}"))
}

fn set_once<T>(slot: &mut Option<T>, value: T, option: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        Err(format!("duplicate {option}"))
    } else {
        Ok(())
    }
}

fn is_dogpaddle_variable(name: &OsStr) -> bool {
    name.to_string_lossy().starts_with("DOGPADDLE_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_reference_defaults_to_five_independent_runs() {
        let configuration = Configuration::parse(vec![
            "--store-dir".to_owned(),
            "/tmp/dogpaddle-reference-store".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/dogpaddle-reference-output".to_owned(),
        ])
        .unwrap();
        assert_eq!(configuration.target, Target::Normal);
        assert_eq!(configuration.runs, 5);
    }

    #[test]
    fn endurance_reference_defaults_to_one_run_and_accepts_an_override() {
        let configuration = Configuration::parse(vec![
            "--store-dir".to_owned(),
            "/tmp/dogpaddle-reference-store".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/dogpaddle-reference-output".to_owned(),
            "--target".to_owned(),
            "endurance".to_owned(),
            "--runs".to_owned(),
            "2".to_owned(),
        ])
        .unwrap();
        assert_eq!(configuration.target, Target::Endurance);
        assert_eq!(configuration.runs, 2);
    }

    #[test]
    fn reference_paths_must_be_absolute_and_distinct() {
        assert!(Configuration::parse(vec![]).is_err());
        assert!(
            Configuration::parse(vec![
                "--store-dir".to_owned(),
                "relative".to_owned(),
                "--output-dir".to_owned(),
                "/tmp/output".to_owned(),
            ])
            .is_err()
        );
        assert!(
            Configuration::parse(vec![
                "--store-dir".to_owned(),
                "/tmp/same".to_owned(),
                "--output-dir".to_owned(),
                "/tmp/same".to_owned(),
            ])
            .is_err()
        );
    }
}
