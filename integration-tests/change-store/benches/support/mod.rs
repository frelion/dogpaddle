use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dogpaddle_change::{Change, ChangeProjection, decode_change, decode_change_projected};
use dogpaddle_store::CodecError as StoreCodecError;
use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR";
const CARGO_PROFILE_ENV: &str = "DOGPADDLE_CARGO_PROFILE";

pub(crate) struct BenchStoreRoot {
    profile: &'static str,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct SampleWork {
    pub(crate) transactions: usize,
    pub(crate) rows: usize,
    pub(crate) changes: usize,
    pub(crate) encoded_bytes: usize,
    pub(crate) logical_bytes: usize,
}

impl BenchStoreRoot {
    pub(crate) fn from_environment() -> Self {
        let profile = std::env::var(PROFILE_ENV).unwrap_or_else(|_| "smoke".to_owned());
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile.as_str() {
            "smoke" => {
                if let Some(base) = configured {
                    Self::configured("smoke", &base)
                } else {
                    let temporary = tempfile::tempdir()
                        .expect("create temporary benchmark Store base directory");
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
                "reference benchmark Store base must be an absolute path"
            );
        }
        std::fs::create_dir_all(base).unwrap_or_else(|error| {
            panic!(
                "create configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        let base = base.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        assert!(base.is_dir(), "benchmark Store base must be a directory");
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

    pub(crate) fn sample(&self, scenario: &str) -> SampleStore {
        let prefix = format!("dogpaddle-{scenario}-");
        let root = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir_in(&self.base)
            .unwrap_or_else(|error| {
                panic!(
                    "create benchmark sample directory under {}: {error}",
                    self.base.display()
                )
            });
        let store = root.path().join("store");
        SampleStore { _root: root, store }
    }
}

impl SampleStore {
    pub(crate) fn path(&self) -> &Path {
        &self.store
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

#[allow(dead_code)]
pub(crate) fn checked_product(label: &str, left: usize, right: usize) -> usize {
    left.checked_mul(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

#[allow(dead_code)]
pub(crate) fn checked_sum(label: &str, left: usize, right: usize) -> usize {
    left.checked_add(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

#[allow(dead_code)]
pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}

#[allow(dead_code)]
pub(crate) fn emit_environment(
    root: &BenchStoreRoot,
    rows_per_change: usize,
    changes_per_transaction: usize,
    payload_bytes: usize,
    samples: usize,
    warmups: usize,
    max_working_set_bytes: usize,
) {
    emit_host_environment(root, "change_append_log");
    println!(
        "{{\"record\":\"configuration\",\"benchmark\":\"change_append_log\",\"rows_per_change\":{rows_per_change},\"changes_per_transaction\":{changes_per_transaction},\"payload_bytes\":{payload_bytes},\"samples\":{samples},\"warmups\":{warmups},\"max_working_set_bytes\":{max_working_set_bytes}}}"
    );
}

#[allow(dead_code)]
pub(crate) fn emit_host_environment(root: &BenchStoreRoot, benchmark: &str) {
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
        "{{\"record\":\"environment\",\"benchmark\":{},\"profile\":{},\"cargo_profile\":{},\"cargo_profile_source\":{},\"filesystem_path\":{},\"filesystem\":{},\"mdbx_sync_mode\":\"durable\",\"os\":{},\"arch\":{},\"kernel\":{},\"cpu\":{},\"parallelism\":{parallelism},\"rustc\":{},\"git_revision\":{},\"git_state\":{},\"debug_assertions\":{},\"unix_seconds\":{unix_seconds}}}",
        json_string(benchmark),
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

#[allow(dead_code)]
pub(crate) fn emit_sample(scenario: &str, sample: usize, elapsed: Duration, work: SampleWork) {
    assert!(
        work.transactions > 0,
        "sample transaction count must be non-zero"
    );
    let rows_per_transaction = work.rows / work.transactions;
    let changes_per_transaction = work.changes / work.transactions;
    let encoded_bytes_per_transaction = work.encoded_bytes / work.transactions;
    let bytes_per_transaction = work.logical_bytes / work.transactions;
    println!(
        "{{\"record\":\"sample\",\"benchmark\":\"change_append_log\",\"scenario\":{},\"sample\":{sample},\"elapsed_ns\":{},\"transactions\":{},\"rows\":{},\"changes\":{},\"encoded_bytes\":{},\"logical_bytes\":{},\"rows_per_transaction\":{rows_per_transaction},\"changes_per_transaction\":{changes_per_transaction},\"encoded_bytes_per_transaction\":{encoded_bytes_per_transaction},\"bytes_per_transaction\":{bytes_per_transaction}}}",
        json_string(scenario),
        elapsed.as_nanos(),
        work.transactions,
        work.rows,
        work.changes,
        work.encoded_bytes,
        work.logical_bytes,
    );
}

#[allow(clippy::cast_precision_loss)]
#[allow(dead_code)]
pub(crate) fn report(label: &str, samples: &mut [Duration], work: SampleWork) {
    assert!(!samples.is_empty(), "benchmark samples must be non-empty");
    assert!(
        work.transactions > 0,
        "sample transaction count must be non-zero"
    );
    samples.sort_unstable();
    let min = samples[0];
    let median = samples[samples.len() / 2];
    let max = samples[samples.len() - 1];
    assert!(!median.is_zero(), "benchmark median must be non-zero");
    let seconds = median.as_secs_f64();
    let rows_per_second = work.rows as f64 / seconds;
    let changes_per_second = work.changes as f64 / seconds;
    let encoded_mebibytes_per_second = work.encoded_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let mebibytes_per_second = work.logical_bytes as f64 / (1024.0 * 1024.0) / seconds;
    println!(
        "{label}: min={min:?} median={median:?} max={max:?} rows/s={rows_per_second:.0} changes/s={changes_per_second:.0} encoded_MiB/s={encoded_mebibytes_per_second:.2} logical_MiB/s={mebibytes_per_second:.2}"
    );
    println!(
        "{{\"record\":\"summary\",\"benchmark\":\"change_append_log\",\"scenario\":{},\"samples\":{},\"min_ns\":{},\"median_ns\":{},\"max_ns\":{},\"rows\":{},\"changes\":{},\"encoded_bytes\":{},\"logical_bytes\":{}}}",
        json_string(label),
        samples.len(),
        min.as_nanos(),
        median.as_nanos(),
        max.as_nanos(),
        work.rows,
        work.changes,
        work.encoded_bytes,
        work.logical_bytes,
    );
}

#[allow(dead_code)]
pub(crate) fn fold_checksum(state: u64, value: u64) -> u64 {
    state.rotate_left(11) ^ value.wrapping_mul(0x9e37_79b9_7f4a_7c15)
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
