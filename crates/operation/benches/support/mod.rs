use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_OPERATION_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_OPERATION_BENCH_STORE_DIR";
const CARGO_PROFILE_ENV: &str = "DOGPADDLE_CARGO_PROFILE";
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_CODEC_OPERATIONS: usize = 100_000;
const DEFAULT_BODY_TRANSACTIONS: usize = 512;
const DEFAULT_DURABLE_TRANSACTIONS: usize = 64;
const DEFAULT_WARMUP_TRANSACTIONS: usize = 4;
const DEFAULT_STEPS: &[usize] = &[1, 64, 1_024];

pub(crate) struct Config {
    pub(crate) samples: usize,
    pub(crate) codec_operations: usize,
    pub(crate) body_transactions: usize,
    pub(crate) durable_transactions: usize,
    pub(crate) warmup_transactions: usize,
    pub(crate) steps: Vec<usize>,
}

pub(crate) struct BenchRoot {
    profile: &'static str,
    filesystem_base: PathBuf,
    run_root: TempDir,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SampleRecord {
    operation: &'static str,
    scenario: &'static str,
    sample: usize,
    elapsed: Duration,
    operations: usize,
    transactions: usize,
    steps_per_transaction: usize,
}

impl Config {
    pub(crate) fn load() -> Self {
        let config = Self {
            samples: setting("DOGPADDLE_OPERATION_BENCH_SAMPLES", DEFAULT_SAMPLES),
            codec_operations: setting(
                "DOGPADDLE_OPERATION_BENCH_CODEC_OPERATIONS",
                DEFAULT_CODEC_OPERATIONS,
            ),
            body_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_BODY_TRANSACTIONS_PER_SAMPLE",
                DEFAULT_BODY_TRANSACTIONS,
            ),
            durable_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_DURABLE_TRANSACTIONS_PER_SAMPLE",
                DEFAULT_DURABLE_TRANSACTIONS,
            ),
            warmup_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_WARMUP_TRANSACTIONS",
                DEFAULT_WARMUP_TRANSACTIONS,
            ),
            steps: setting_list(
                "DOGPADDLE_OPERATION_BENCH_STEPS_PER_TRANSACTION",
                DEFAULT_STEPS,
            ),
        };
        assert!(!config.steps.is_empty());
        assert!(config.steps.iter().all(|steps| *steps > 0));
        let mut sorted_steps = config.steps.clone();
        sorted_steps.sort_unstable();
        assert!(
            sorted_steps.windows(2).all(|pair| pair[0] != pair[1]),
            "DOGPADDLE_OPERATION_BENCH_STEPS_PER_TRANSACTION must not contain duplicate values"
        );
        config
    }

    pub(crate) fn codec_warmup_operations(&self) -> usize {
        self.codec_operations.min(1_000)
    }

    pub(crate) fn emit(&self, profile: &str) {
        let steps = self
            .steps
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let (cargo_profile, cargo_profile_source) = cargo_profile();
        println!(
            "{{\"record\":\"config\",\"benchmark\":\"operation_core\",\"profile\":{},\"cargo_profile\":{},\"cargo_profile_source\":{},\"samples\":{},\"codec_operations_per_sample\":{},\"body_transactions_per_sample\":{},\"durable_transactions_per_sample\":{},\"warmup_transactions\":{},\"steps_per_transaction\":[{}]}}",
            json_string(profile),
            json_string(&cargo_profile),
            json_string(cargo_profile_source),
            self.samples,
            self.codec_operations,
            self.body_transactions,
            self.durable_transactions,
            self.warmup_transactions,
            steps
        );
    }
}

impl BenchRoot {
    pub(crate) fn from_environment() -> Self {
        let profile = env::var(PROFILE_ENV).unwrap_or_else(|_| "smoke".to_owned());
        let configured = env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile.as_str() {
            "smoke" => {
                configured.map_or_else(Self::temporary, |base| Self::configured("smoke", &base))
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

    fn temporary() -> Self {
        let temporary_base = tempfile::tempdir().expect("create temporary benchmark Store base");
        let filesystem_base = temporary_base.path().to_path_buf();
        let run_root = tempfile::Builder::new()
            .prefix("dogpaddle-operation-run-")
            .tempdir_in(&filesystem_base)
            .expect("create temporary operation benchmark run root");
        Self {
            profile: "smoke",
            filesystem_base,
            run_root,
            _temporary_base: Some(temporary_base),
        }
    }

    fn configured(profile: &'static str, base: &Path) -> Self {
        if profile == "reference" {
            assert!(
                base.is_absolute(),
                "reference benchmark Store base must be an absolute path"
            );
        }
        fs::create_dir_all(base).unwrap_or_else(|error| {
            panic!(
                "create configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        let filesystem_base = base.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        assert!(
            filesystem_base.is_dir(),
            "benchmark Store base must be a directory"
        );
        let run_root = tempfile::Builder::new()
            .prefix("dogpaddle-operation-run-")
            .tempdir_in(&filesystem_base)
            .unwrap_or_else(|error| {
                panic!(
                    "create operation benchmark run root under {}: {error}",
                    filesystem_base.display()
                )
            });
        Self {
            profile,
            filesystem_base,
            run_root,
            _temporary_base: None,
        }
    }

    pub(crate) const fn profile(&self) -> &'static str {
        self.profile
    }

    pub(crate) fn path(&self) -> &Path {
        self.run_root.path()
    }

    pub(crate) fn store_path(&self, name: &str) -> PathBuf {
        self.path().join(name)
    }

    pub(crate) fn emit_environment(&self) {
        let rustc_program = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
        let rustc = command_output(&rustc_program, &["--version"]);
        let kernel = command_output("uname", &["-sr"]);
        let cpu = cpu_name();
        let revision = command_output("git", &["rev-parse", "HEAD"]);
        let git_state = match Command::new("git").args(["status", "--porcelain"]).output() {
            Ok(output) if output.status.success() && output.stdout.is_empty() => "clean".to_owned(),
            Ok(output) if output.status.success() => "dirty".to_owned(),
            _ => "unavailable".to_owned(),
        };
        let filesystem = filesystem(&self.filesystem_base);
        let (cargo_profile, cargo_profile_source) = cargo_profile();
        println!(
            "{{\"record\":\"environment\",\"benchmark\":\"operation_core\",\"profile\":{},\"cargo_profile\":{},\"cargo_profile_source\":{},\"debug_assertions\":{},\"rustc\":{},\"os\":{},\"arch\":{},\"kernel\":{},\"cpu\":{},\"git_revision\":{},\"git_state\":{},\"filesystem_path\":{},\"filesystem\":{},\"store_root\":{},\"execution\":\"single-thread\",\"cache\":\"warm\",\"mdbx_sync_mode\":\"durable\"}}",
            json_string(self.profile),
            json_string(&cargo_profile),
            json_string(cargo_profile_source),
            cfg!(debug_assertions),
            json_string(&rustc),
            json_string(env::consts::OS),
            json_string(env::consts::ARCH),
            json_string(&kernel),
            json_string(&cpu),
            json_string(&revision),
            json_string(&git_state),
            json_string(&self.filesystem_base.display().to_string()),
            json_string(&filesystem),
            json_string(&self.path().display().to_string()),
        );
    }
}

pub(crate) fn record_samples(
    records: &mut Vec<SampleRecord>,
    operation: &'static str,
    scenario: &'static str,
    operations: usize,
    transactions: usize,
    steps_per_transaction: usize,
    durations: Vec<Duration>,
) {
    assert!(!durations.is_empty());
    assert!(operations > 0);
    let mut sorted = durations.clone();
    sorted.sort_unstable();
    let min = sorted[0];
    let median = sorted[sorted.len() / 2];
    let max = sorted[sorted.len() - 1];
    println!(
        "{operation:<10} {scenario:<28} steps/tx={steps_per_transaction:<5} operations={operations:<9} min={} median={} max={}",
        duration(min),
        duration(median),
        duration(max)
    );
    records.extend(
        durations
            .into_iter()
            .enumerate()
            .map(|(sample, elapsed)| SampleRecord {
                operation,
                scenario,
                sample,
                elapsed,
                operations,
                transactions,
                steps_per_transaction,
            }),
    );
}

pub(crate) fn emit_samples(records: &[SampleRecord]) {
    println!();
    println!("=== machine-readable raw JSON samples ===");
    for record in records {
        let elapsed_ns = record.elapsed.as_nanos();
        let operations = u128::try_from(record.operations).expect("operation count fits u128");
        let ns_per_operation = elapsed_ns / operations;
        println!(
            "{{\"record\":\"sample\",\"benchmark\":\"operation_core\",\"operation\":{},\"scenario\":{},\"sample\":{},\"elapsed_ns\":{},\"operations\":{},\"transactions\":{},\"steps_per_transaction\":{},\"ns_per_operation\":{}}}",
            json_string(record.operation),
            json_string(record.scenario),
            record.sample,
            elapsed_ns,
            record.operations,
            record.transactions,
            record.steps_per_transaction,
            ns_per_operation
        );
    }
}

fn setting(name: &str, default: usize) -> usize {
    let value = env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
    });
    assert!(value > 0, "{name} must be positive");
    value
}

fn cargo_profile() -> (String, &'static str) {
    match env::var(CARGO_PROFILE_ENV) {
        Ok(profile) => {
            assert!(
                !profile.is_empty() && profile.trim() == profile,
                "{CARGO_PROFILE_ENV} must be a non-empty Cargo profile name without surrounding whitespace"
            );
            (profile, "environment")
        }
        Err(env::VarError::NotPresent) => ("bench".to_owned(), "default"),
        Err(env::VarError::NotUnicode(_)) => {
            panic!("{CARGO_PROFILE_ENV} must be valid Unicode")
        }
    }
}

fn setting_list(name: &str, default: &[usize]) -> Vec<usize> {
    env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .map(|item| {
                    item.parse::<usize>()
                        .unwrap_or_else(|_| panic!("{name} must be a comma-separated integer list"))
                })
                .collect()
        },
    )
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || "unavailable".to_owned(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )
}

fn cpu_name() -> String {
    if env::consts::OS == "macos" {
        return command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    if env::consts::OS == "linux" {
        return fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|contents| {
                contents.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    matches!(key.trim(), "model name" | "Hardware").then(|| value.trim().to_owned())
                })
            })
            .unwrap_or_else(|| "unavailable".to_owned());
    }
    "unavailable".to_owned()
}

fn filesystem(path: &Path) -> String {
    let path = path.display().to_string();
    if env::consts::OS == "macos" {
        let kind = command_output("stat", &["-f", "%T", &path]);
        let usage = command_output("df", &["-k", &path]);
        return format!("type={kind}; {usage}");
    }
    let typed = command_output("df", &["-T", &path]);
    if typed == "unavailable" || typed.is_empty() {
        command_output("df", &[&path])
    } else {
        typed
    }
}

fn duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}

fn json_string(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for character in value.chars() {
        match character {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            character if character.is_control() => {
                write!(encoded, "\\u{:04x}", u32::from(character))
                    .expect("writing to String cannot fail");
            }
            character => encoded.push(character),
        }
    }
    encoded.push('"');
    encoded
}
