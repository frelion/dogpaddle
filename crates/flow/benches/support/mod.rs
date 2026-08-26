use std::path::{Path, PathBuf};

use dogpaddle_bench_protocol::BenchmarkProfile;
use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_FLOW_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_FLOW_BENCH_STORE_DIR";

pub(crate) struct BenchRoot {
    profile: BenchmarkProfile,
    base: PathBuf,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SamplePath {
    _root: TempDir,
    flow: PathBuf,
}

impl BenchRoot {
    pub(crate) fn from_environment() -> Self {
        let profile =
            BenchmarkProfile::from_environment(PROFILE_ENV).expect("read Flow benchmark profile");
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile {
            BenchmarkProfile::Smoke => {
                if let Some(base) = configured {
                    Self::configured(profile, &base)
                } else {
                    let temporary = tempfile::tempdir()
                        .expect("create temporary Flow benchmark base directory");
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

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
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
