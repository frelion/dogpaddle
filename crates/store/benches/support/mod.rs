use std::{
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, EnvironmentRecord, Fields, HostEnvironment, JsonlWriter,
    require_benchmark_build,
};
use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_STORE_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_STORE_BENCH_STORE_DIR";

static BENCH_BASE: OnceLock<PathBuf> = OnceLock::new();

pub(crate) struct BenchRoot {
    benchmark: &'static str,
    pub(crate) profile: BenchmarkProfile,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

#[must_use = "the benchmark root owns the temporary Store directory for the full run"]
pub(crate) fn initialize(benchmark: &'static str) -> BenchRoot {
    require_benchmark_build(benchmark);
    let root = BenchRoot::from_process(benchmark);
    BENCH_BASE
        .set(root.base.clone())
        .expect("one benchmark process cannot initialize two Store roots");
    root.emit_environment();
    root
}

impl BenchRoot {
    fn from_process(benchmark: &'static str) -> Self {
        let profile =
            BenchmarkProfile::from_environment(PROFILE_ENV).expect("parse Store benchmark profile");
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match (profile, configured) {
            (BenchmarkProfile::Smoke, Some(base)) => Self::configured(benchmark, profile, &base),
            (BenchmarkProfile::Smoke, None) => {
                let temporary =
                    tempfile::tempdir().expect("create temporary Store benchmark base directory");
                Self {
                    benchmark,
                    profile,
                    base: temporary.path().to_path_buf(),
                    _temporary_base: Some(temporary),
                }
            }
            (BenchmarkProfile::Reference, Some(base)) => {
                assert!(
                    base.is_absolute(),
                    "reference Store benchmark directory must be absolute"
                );
                Self::configured(benchmark, profile, &base)
            }
            (BenchmarkProfile::Reference, None) => {
                panic!("{PROFILE_ENV}=reference requires an explicit {STORE_DIR_ENV}")
            }
        }
    }

    fn configured(benchmark: &'static str, profile: BenchmarkProfile, base: &Path) -> Self {
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

    fn emit_environment(&self) {
        let host = HostEnvironment::collect(Some(&self.base))
            .expect("collect Store benchmark host environment");
        let fields = Fields::new()
            .with("mdbx_sync_mode", "durable")
            .expect("construct Store environment fields");
        let record = EnvironmentRecord::for_profile(self.benchmark, self.profile, host, fields)
            .expect("construct Store environment record");
        write_record(&record);
    }
}

pub(crate) fn sample_dir(scenario: &str) -> TempDir {
    let base = BENCH_BASE
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
        .tempdir_in(base)
        .unwrap_or_else(|error| {
            panic!(
                "create Store benchmark sample under {}: {error}",
                base.display()
            )
        })
}

pub(crate) fn write_record(record: &impl BenchmarkRecord) {
    let stdout = std::io::stdout();
    JsonlWriter::new(stdout.lock())
        .write(record)
        .expect("write Store benchmark JSONL record");
}

#[allow(dead_code)]
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
