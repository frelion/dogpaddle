use std::{
    io,
    path::{Path, PathBuf},
};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, EnvironmentRecord, Fields, HostEnvironment, JsonlWriter,
    positive_usize,
};
use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::CodecError as StoreCodecError;
use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR";

pub(crate) struct BenchStoreRoot {
    profile: BenchmarkProfile,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

impl BenchStoreRoot {
    pub(crate) fn from_environment() -> Self {
        let profile = BenchmarkProfile::from_environment(PROFILE_ENV)
            .expect("load Change + Store benchmark profile");
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile {
            BenchmarkProfile::Smoke => {
                if let Some(base) = configured {
                    Self::configured(profile, &base)
                } else {
                    let temporary = tempfile::tempdir()
                        .expect("create temporary benchmark Store base directory");
                    let base = temporary.path().to_path_buf();
                    Self {
                        profile,
                        base,
                        _temporary_base: Some(temporary),
                    }
                }
            }
            BenchmarkProfile::Reference => {
                let base = configured.unwrap_or_else(|| {
                    panic!("{PROFILE_ENV}=reference requires an explicit {STORE_DIR_ENV}")
                });
                Self::configured(profile, &base)
            }
        }
    }

    fn configured(profile: BenchmarkProfile, base: &Path) -> Self {
        if profile == BenchmarkProfile::Reference {
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
        self.profile.as_str()
    }

    pub(crate) const fn benchmark_profile(&self) -> BenchmarkProfile {
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
    positive_usize(name, default).expect("load positive Change + Store benchmark setting")
}

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn emit_host_environment(root: &BenchStoreRoot, benchmark: &str) {
    let host = HostEnvironment::collect(Some(root.base()))
        .expect("collect Change + Store benchmark environment");
    let mut fields = Fields::new();
    fields
        .insert("mdbx_sync_mode", "durable")
        .expect("encode MDBX sync mode");
    emit_record(
        &EnvironmentRecord::for_profile(benchmark, root.benchmark_profile(), host, fields)
            .expect("build Change + Store environment record"),
    );
}

pub(crate) fn emit_record(record: &impl BenchmarkRecord) {
    JsonlWriter::new(io::stdout().lock())
        .write(record)
        .expect("write Change + Store benchmark JSONL");
}
