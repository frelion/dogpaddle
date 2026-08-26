use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_FLOW_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_FLOW_BENCH_STORE_DIR";
const CARGO_PROFILE_ENV: &str = "DOGPADDLE_CARGO_PROFILE";

pub(crate) struct BenchRoot {
    profile: &'static str,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SamplePath {
    _root: TempDir,
    flow: PathBuf,
}

impl BenchRoot {
    pub(crate) fn from_environment() -> Self {
        let profile = std::env::var(PROFILE_ENV).unwrap_or_else(|_| "smoke".to_owned());
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile.as_str() {
            "smoke" => {
                if let Some(base) = configured {
                    Self::configured("smoke", &base)
                } else {
                    let temporary = tempfile::tempdir()
                        .expect("create temporary Flow benchmark base directory");
                    let base = temporary.path().to_path_buf();
                    Self {
                        profile: "smoke",
                        base,
                        _temporary_base: Some(temporary),
                    }
                }
            }
            "reference" => {
                let base = configured.unwrap_or_else(|| {
                    panic!("{PROFILE_ENV}=reference requires an explicit {STORE_DIR_ENV}")
                });
                Self::configured("reference", &base)
            }
            _ => panic!("{PROFILE_ENV} must be smoke or reference"),
        }
    }

    fn configured(profile: &'static str, base: &Path) -> Self {
        if profile == "reference" {
            assert!(
                base.is_absolute(),
                "reference benchmark base must be an absolute path"
            );
        }
        std::fs::create_dir_all(base).unwrap_or_else(|error| {
            panic!(
                "create configured Flow benchmark base {}: {error}",
                base.display()
            )
        });
        let base = base.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve configured Flow benchmark base {}: {error}",
                base.display()
            )
        });
        assert!(base.is_dir(), "Flow benchmark base must be a directory");
        Self {
            profile,
            base,
            _temporary_base: None,
        }
    }

    pub(crate) const fn profile(&self) -> &'static str {
        self.profile
    }

    pub(crate) fn base(&self) -> &Path {
        &self.base
    }

    pub(crate) fn sample(&self, scenario: &str) -> SamplePath {
        let prefix = format!("dogpaddle-{scenario}-");
        let root = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir_in(&self.base)
            .unwrap_or_else(|error| {
                panic!(
                    "create Flow benchmark sample under {}: {error}",
                    self.base.display()
                )
            });
        let flow = root.path().join("flow");
        SamplePath { _root: root, flow }
    }
}

impl SamplePath {
    pub(crate) fn path(&self) -> &Path {
        &self.flow
    }
}

pub(crate) fn setting(name: &str, default: usize) -> usize {
    let value = std::env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("{name} must be a positive integer: {error}"))
    });
    assert!(value > 0, "{name} must be non-zero");
    value
}

pub(crate) fn setting_list(name: &str, default: &[usize]) -> Vec<usize> {
    let values = std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .map(|item| {
                    item.parse::<usize>().unwrap_or_else(|error| {
                        panic!("{name} must be a comma-separated positive integer list: {error}")
                    })
                })
                .collect::<Vec<_>>()
        },
    );
    assert!(!values.is_empty(), "{name} must not be empty");
    assert!(
        values.iter().all(|value| *value > 0),
        "{name} values must be non-zero"
    );
    values
}

pub(crate) fn emit_environment(root: &BenchRoot) {
    let rustc_program = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let rustc = command_output(&rustc_program, &["--version"]);
    let git_revision = command_output("git", &["rev-parse", "HEAD"]);
    let git_status = command_output("git", &["status", "--porcelain"]);
    let git_state = if git_status.is_empty() {
        "clean"
    } else if git_status.starts_with("unavailable:") {
        "unavailable"
    } else {
        "dirty"
    };
    let cpu = cpu_description();
    let kernel = command_output("uname", &["-a"]);
    let filesystem = filesystem_description(root.base());
    let parallelism = std::thread::available_parallelism().map_or(0, usize::from);
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs();
    let (cargo_profile, cargo_profile_source) = cargo_profile();
    println!(
        "{{\"record\":\"environment\",\"benchmark\":\"flow_lifecycle\",\"profile\":{},\"cargo_profile\":{},\"cargo_profile_source\":{},\"filesystem_path\":{},\"filesystem\":{},\"mdbx_sync_mode\":\"durable\",\"os\":{},\"arch\":{},\"kernel\":{},\"cpu\":{},\"parallelism\":{parallelism},\"rustc\":{},\"git_revision\":{},\"git_state\":{},\"debug_assertions\":{},\"unix_seconds\":{unix_seconds}}}",
        json_string(root.profile()),
        json_string(&cargo_profile),
        json_string(cargo_profile_source),
        json_string(&root.base().display().to_string()),
        json_string(&filesystem),
        json_string(std::env::consts::OS),
        json_string(std::env::consts::ARCH),
        json_string(&kernel),
        json_string(&cpu),
        json_string(&rustc),
        json_string(&git_revision),
        json_string(git_state),
        cfg!(debug_assertions),
    );
}

fn cargo_profile() -> (String, &'static str) {
    match std::env::var(CARGO_PROFILE_ENV) {
        Ok(profile) => {
            assert!(
                !profile.is_empty() && profile.trim() == profile,
                "{CARGO_PROFILE_ENV} must be a non-empty Cargo profile name without surrounding whitespace"
            );
            (profile, "environment")
        }
        Err(std::env::VarError::NotPresent) => ("bench".to_owned(), "default"),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{CARGO_PROFILE_ENV} must be valid Unicode")
        }
    }
}

pub(crate) fn emit_configuration(stage_counts: &[usize], samples: usize, warmups: usize) {
    println!(
        "{{\"record\":\"configuration\",\"benchmark\":\"flow_lifecycle\",\"stage_counts\":{stage_counts:?},\"samples\":{samples},\"warmups\":{warmups},\"fresh_build_path_and_builder\":\"outside_timing\",\"fresh_build_store_per_sample\":true,\"reopen_fixture_and_warmup\":\"outside_timing\",\"reopen_cache\":\"warm_committed\",\"validation\":\"outside_timing\"}}"
    );
}

pub(crate) fn emit_sample(scenario: &str, stage_count: usize, sample: usize, elapsed: Duration) {
    println!(
        "{{\"record\":\"sample\",\"benchmark\":\"flow_lifecycle\",\"scenario\":{},\"stage_count\":{stage_count},\"sample\":{sample},\"elapsed_ns\":{}}}",
        json_string(scenario),
        elapsed.as_nanos(),
    );
}

pub(crate) fn report(scenario: &str, stage_count: usize, samples: &mut [Duration]) {
    assert!(!samples.is_empty(), "benchmark samples must be non-empty");
    samples.sort_unstable();
    let min = samples[0];
    let median = samples[samples.len() / 2];
    let max = samples[samples.len() - 1];
    assert!(!median.is_zero(), "benchmark median must be non-zero");
    println!("{scenario} stages={stage_count}: min={min:?} median={median:?} max={max:?}");
    println!(
        "{{\"record\":\"summary\",\"benchmark\":\"flow_lifecycle\",\"scenario\":{},\"stage_count\":{stage_count},\"samples\":{},\"min_ns\":{},\"median_ns\":{},\"max_ns\":{}}}",
        json_string(scenario),
        samples.len(),
        min.as_nanos(),
        median.as_nanos(),
        max.as_nanos(),
    );
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program).args(arguments).output().map_or_else(
        |error| format!("unavailable: {error}"),
        |output| {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).trim().to_owned()
            } else {
                format!(
                    "unavailable: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )
            }
        },
    )
}

fn cpu_description() -> String {
    if std::env::consts::OS == "macos" {
        let description = command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
        if !description.starts_with("unavailable:") {
            return description;
        }
    }
    if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo")
        && let Some(description) = cpuinfo.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            matches!(key.trim(), "model name" | "Hardware").then(|| value.trim().to_owned())
        })
    {
        return description;
    }
    "unavailable".to_owned()
}

fn filesystem_description(path: &Path) -> String {
    Command::new("df")
        .arg("-P")
        .arg(path)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .last()
                .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write;
                write!(escaped, "\\u{:04x}", u32::from(character))
                    .expect("writing to a String cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}
