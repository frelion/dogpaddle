use std::{
    fs,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

use crate::PerformanceProfile;

/// Selects the common benchmark workload profile.
pub(crate) const PERFORMANCE_PROFILE_ENV: &str = "DOGPADDLE_PERF_PROFILE";

/// Selects the common filesystem root for persistent benchmark fixtures.
pub(crate) const PERFORMANCE_ROOT_ENV: &str = "DOGPADDLE_PERF_ROOT";

/// Owns one benchmark process's filesystem root and workload profile.
///
/// Smoke runs use a temporary root unless `DOGPADDLE_PERF_ROOT` is supplied.
/// Reference runs require an explicit absolute root. Each benchmark process and
/// sample receives a fresh child directory, so no global initialization state is
/// needed.
pub struct RunRoot {
    profile: PerformanceProfile,
    filesystem_root: PathBuf,
    run: PathBuf,
    _temporary_filesystem: Option<TempDir>,
}

impl RunRoot {
    /// Builds a run root from the common benchmark environment.
    ///
    /// # Panics
    ///
    /// Panics for an invalid profile, a missing or relative reference root, or
    /// when the configured filesystem cannot be prepared.
    #[must_use]
    pub fn from_environment(benchmark: &str) -> Self {
        let profile = PerformanceProfile::from_environment();
        Self::for_profile(benchmark, profile)
    }

    /// Builds a run root for an already selected performance profile.
    ///
    /// This is used by Criterion's Cargo test mode, which always selects the
    /// smoke workload without requiring benchmark environment variables.
    ///
    /// # Panics
    ///
    /// Panics when the configured root is invalid or cannot be prepared.
    #[must_use]
    pub fn for_profile(benchmark: &str, profile: PerformanceProfile) -> Self {
        let configured = std::env::var_os(PERFORMANCE_ROOT_ENV).map(PathBuf::from);
        match (profile, configured) {
            (PerformanceProfile::Smoke, None) => Self::temporary(benchmark),
            (PerformanceProfile::Smoke, Some(root)) => Self::configured(benchmark, profile, &root),
            (PerformanceProfile::Reference, Some(root)) => {
                assert!(
                    root.is_absolute(),
                    "{PERFORMANCE_ROOT_ENV} must be absolute for reference runs"
                );
                Self::configured(benchmark, profile, &root)
            }
            (PerformanceProfile::Reference, None) => {
                panic!("{PERFORMANCE_PROFILE_ENV}=reference requires {PERFORMANCE_ROOT_ENV}")
            }
        }
    }

    fn temporary(benchmark: &str) -> Self {
        let temporary = tempfile::tempdir().expect("create temporary benchmark filesystem root");
        let filesystem_root = temporary.path().to_path_buf();
        let run = filesystem_root.join(format!("dogpaddle-{}-run", sanitized(benchmark)));
        fs::create_dir(&run).expect("create temporary performance run directory");
        Self {
            profile: PerformanceProfile::Smoke,
            filesystem_root,
            run,
            _temporary_filesystem: Some(temporary),
        }
    }

    fn configured(benchmark: &str, profile: PerformanceProfile, root: &Path) -> Self {
        fs::create_dir_all(root).unwrap_or_else(|error| {
            panic!(
                "create benchmark filesystem root {}: {error}",
                root.display()
            )
        });
        let filesystem_root = root.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve benchmark filesystem root {}: {error}",
                root.display()
            )
        });
        assert!(
            filesystem_root.is_dir(),
            "benchmark filesystem root must be a directory"
        );
        let run = run_directory(benchmark, &filesystem_root);
        Self {
            profile,
            filesystem_root,
            run,
            _temporary_filesystem: None,
        }
    }

    /// Returns the selected workload profile.
    #[must_use]
    pub const fn profile(&self) -> PerformanceProfile {
        self.profile
    }

    /// Returns the filesystem used for environment reporting.
    #[must_use]
    pub fn filesystem_root(&self) -> &Path {
        &self.filesystem_root
    }

    /// Returns this process's fresh run directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.run
    }

    /// Creates a fresh sample directory owned by the returned guard.
    ///
    /// # Panics
    ///
    /// Panics when the sample directory cannot be created.
    #[must_use]
    pub fn sample(&self, scenario: &str) -> TempDir {
        tempfile::Builder::new()
            .prefix(&format!("dogpaddle-{}-", sanitized(scenario)))
            .tempdir_in(&self.run)
            .unwrap_or_else(|error| {
                panic!(
                    "create benchmark sample under {}: {error}",
                    self.run.display()
                )
            })
    }
}

fn run_directory(benchmark: &str, root: &Path) -> PathBuf {
    tempfile::Builder::new()
        .prefix(&format!("dogpaddle-{}-run-", sanitized(benchmark)))
        .tempdir_in(root)
        .unwrap_or_else(|error| {
            panic!(
                "create benchmark run directory under {}: {error}",
                root.display()
            )
        })
        .keep()
}

fn sanitized(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect()
}
