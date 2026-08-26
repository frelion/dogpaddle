#![allow(dead_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_STORE_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_STORE_BENCH_STORE_DIR";
const CARGO_PROFILE_ENV: &str = "DOGPADDLE_CARGO_PROFILE";

static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();

pub(crate) struct Environment {
    benchmark: &'static str,
    profile: &'static str,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

#[derive(Clone, Copy)]
pub(crate) struct SampleWork {
    pub(crate) operations: usize,
    pub(crate) transactions: usize,
    pub(crate) logical_bytes: usize,
}

#[allow(clippy::assertions_on_constants)]
pub(crate) fn initialize(benchmark: &'static str) -> &'static Environment {
    assert!(
        !cfg!(debug_assertions),
        "Store benchmarks must run through `cargo bench` in the release bench profile"
    );
    let environment = ENVIRONMENT.get_or_init(|| Environment::from_process(benchmark));
    assert_eq!(
        environment.benchmark, benchmark,
        "one benchmark process cannot initialize two target names"
    );
    environment.emit();
    environment
}

impl Environment {
    fn from_process(benchmark: &'static str) -> Self {
        let profile = std::env::var(PROFILE_ENV).unwrap_or_else(|_| "smoke".to_owned());
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile.as_str() {
            "smoke" => {
                if let Some(base) = configured {
                    Self::configured(benchmark, "smoke", &base)
                } else {
                    let temporary = tempfile::tempdir()
                        .expect("create temporary Store benchmark base directory");
                    let base = temporary.path().to_path_buf();
                    Self {
                        benchmark,
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
                assert!(
                    base.is_absolute(),
                    "reference Store benchmark directory must be absolute"
                );
                Self::configured(benchmark, "reference", &base)
            }
            _ => panic!("{PROFILE_ENV} must be smoke or reference"),
        }
    }

    fn configured(benchmark: &'static str, profile: &'static str, base: &Path) -> Self {
        fs::create_dir_all(base).unwrap_or_else(|error| {
            panic!(
                "create configured Store benchmark directory {}: {error}",
                base.display()
            )
        });
        let base = base.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve configured Store benchmark directory {}: {error}",
                base.display()
            )
        });
        assert!(base.is_dir(), "Store benchmark base must be a directory");
        Self {
            benchmark,
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

    fn emit(&self) {
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
        let kernel = command_output("uname", &["-a"]);
        let cpu = cpu_description();
        let filesystem = filesystem_description(&self.base);
        let parallelism = std::thread::available_parallelism().map_or(0, usize::from);
        let unix_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_secs();
        let (cargo_profile, cargo_profile_source) = cargo_profile();
        println!(
            "{{\"record\":\"environment\",\"benchmark\":{},\"profile\":{},\"cargo_profile\":{},\"cargo_profile_source\":{},\"filesystem_path\":{},\"filesystem\":{},\"mdbx_sync_mode\":\"durable\",\"os\":{},\"arch\":{},\"kernel\":{},\"cpu\":{},\"parallelism\":{parallelism},\"rustc\":{},\"git_revision\":{},\"git_state\":{},\"debug_assertions\":{},\"unix_seconds\":{unix_seconds}}}",
            json_string(self.benchmark),
            json_string(self.profile),
            json_string(&cargo_profile),
            json_string(cargo_profile_source),
            json_string(&self.base.display().to_string()),
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

pub(crate) fn sample_dir(scenario: &str) -> TempDir {
    let environment = ENVIRONMENT
        .get()
        .expect("initialize Store benchmark environment before creating fixtures");
    let sanitized = scenario
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    tempfile::Builder::new()
        .prefix(&format!("dogpaddle-{sanitized}-"))
        .tempdir_in(&environment.base)
        .unwrap_or_else(|error| {
            panic!(
                "create Store benchmark sample under {}: {error}",
                environment.base.display()
            )
        })
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
                        panic!("{name} must contain positive integers: {error}")
                    })
                })
                .collect::<Vec<_>>()
        },
    );
    assert!(!values.is_empty(), "{name} cannot be empty");
    assert!(
        values.iter().all(|value| *value > 0),
        "{name} cannot contain zero"
    );
    values
}

pub(crate) fn emit_configuration(benchmark: &str, fields: &str) {
    println!(
        "{{\"record\":\"configuration\",\"benchmark\":{}{}}}",
        json_string(benchmark),
        if fields.is_empty() {
            String::new()
        } else {
            format!(",{fields}")
        }
    );
}

pub(crate) fn emit_samples(
    benchmark: &str,
    scenario: &str,
    variant: &str,
    samples: &[Duration],
    work: SampleWork,
) {
    assert!(!samples.is_empty(), "benchmark samples must be non-empty");
    assert!(work.operations > 0, "sample operations must be non-zero");
    for (sample, elapsed) in samples.iter().enumerate() {
        emit_sample(benchmark, scenario, variant, sample, *elapsed, work);
    }
}

pub(crate) fn emit_sample(
    benchmark: &str,
    scenario: &str,
    variant: &str,
    sample: usize,
    elapsed: Duration,
    work: SampleWork,
) {
    assert!(work.operations > 0, "sample operations must be non-zero");
    println!(
        "{{\"record\":\"sample\",\"benchmark\":{},\"scenario\":{},\"variant\":{},\"sample\":{sample},\"elapsed_ns\":{},\"operations\":{},\"transactions\":{},\"logical_bytes\":{}}}",
        json_string(benchmark),
        json_string(scenario),
        json_string(variant),
        elapsed.as_nanos(),
        work.operations,
        work.transactions,
        work.logical_bytes,
    );
}

pub(crate) fn emit_summary(
    benchmark: &str,
    scenario: &str,
    variant: &str,
    samples: &[Duration],
    work: SampleWork,
) {
    assert!(!samples.is_empty(), "benchmark samples must be non-empty");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let min = sorted[0];
    let median = sorted[sorted.len() / 2];
    let max = sorted[sorted.len() - 1];
    println!(
        "{{\"record\":\"summary\",\"benchmark\":{},\"scenario\":{},\"variant\":{},\"samples\":{},\"min_ns\":{},\"median_ns\":{},\"max_ns\":{},\"operations\":{},\"transactions\":{},\"logical_bytes\":{}}}",
        json_string(benchmark),
        json_string(scenario),
        json_string(variant),
        sorted.len(),
        min.as_nanos(),
        median.as_nanos(),
        max.as_nanos(),
        work.operations,
        work.transactions,
        work.logical_bytes,
    );
}

pub(crate) fn emit_pair_summary(
    benchmark: &str,
    scenario: &str,
    first_variant: &str,
    second_variant: &str,
    first: &[Duration],
    second: &[Duration],
) {
    assert_eq!(first.len(), second.len());
    assert!(!first.is_empty());
    assert!(
        first.iter().chain(second).all(|elapsed| !elapsed.is_zero()),
        "paired benchmark durations must be non-zero"
    );
    let mut ratios = first
        .iter()
        .zip(second)
        .map(|(first, second)| first.as_secs_f64() / second.as_secs_f64())
        .collect::<Vec<_>>();
    ratios.sort_by(f64::total_cmp);
    let second_wins = first
        .iter()
        .zip(second)
        .filter(|(first, second)| second < first)
        .count();
    println!(
        "{{\"record\":\"pair_summary\",\"benchmark\":{},\"scenario\":{},\"first_variant\":{},\"second_variant\":{},\"samples\":{},\"median_first_over_second\":{:.9},\"second_wins\":{second_wins}}}",
        json_string(benchmark),
        json_string(scenario),
        json_string(first_variant),
        json_string(second_variant),
        first.len(),
        ratios[ratios.len() / 2],
    );
}

pub(crate) fn average_duration(total: Duration, operations: usize) -> String {
    let nanos = total.as_nanos()
        / u128::try_from(operations).expect("benchmark operation count fits in u128");
    format_duration(Duration::from_nanos(
        u64::try_from(nanos).expect("average duration fits in u64 nanoseconds"),
    ))
}

pub(crate) fn format_duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}

pub(crate) fn json_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                write!(&mut output, "\\u{:04x}", u32::from(character)).expect("write JSON escape");
            }
            character => output.push(character),
        }
    }
    output.push('"');
    output
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
    if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo")
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
    if std::env::consts::OS == "macos" {
        let output = command_output("df", &[&path.display().to_string()]);
        if output.starts_with("unavailable:") {
            return output;
        }
        if let Some(device) = output
            .lines()
            .last()
            .and_then(|line| line.split_whitespace().next())
        {
            let mounts = command_output("mount", &[]);
            if let Some(line) = mounts
                .lines()
                .find(|line| line.starts_with(&format!("{device} on ")))
                && let Some(options) = line.split_once(" (").map(|(_, options)| options)
                && let Some(kind) = options.split([',', ')']).next()
            {
                return format!("{} ({device})", kind.trim());
            }
            return device.to_owned();
        }
        return output;
    }
    let output = command_output("df", &["-T", &path.display().to_string()]);
    if output.starts_with("unavailable:") {
        return output;
    }
    output
        .lines()
        .last()
        .and_then(|line| line.split_whitespace().nth(1))
        .map_or(output.clone(), str::to_owned)
}
