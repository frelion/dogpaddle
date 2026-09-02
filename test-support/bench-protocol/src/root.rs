use std::{
    fs,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

use crate::BenchmarkProfile;

/// Selects the common benchmark workload profile.
pub const BENCHMARK_PROFILE_ENV: &str = "DOGPADDLE_BENCH_PROFILE";

/// Selects the common filesystem root for persistent benchmark fixtures.
pub const BENCHMARK_ROOT_ENV: &str = "DOGPADDLE_BENCH_ROOT";

/// Owns one benchmark process's filesystem root and workload profile.
///
/// Smoke runs use a temporary root unless [`BENCHMARK_ROOT_ENV`] is supplied.
/// Reference runs require an explicit absolute root. Each benchmark process and
/// sample receives a fresh child directory, so no global initialization state is
/// needed.
pub struct RunRoot {
    profile: BenchmarkProfile,
    filesystem_root: PathBuf,
    run: TempDir,
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
        let profile = BenchmarkProfile::from_environment();
        let configured = std::env::var_os(BENCHMARK_ROOT_ENV).map(PathBuf::from);
        match (profile, configured) {
            (BenchmarkProfile::Smoke, None) => Self::temporary(benchmark),
            (BenchmarkProfile::Smoke, Some(root)) => Self::configured(benchmark, profile, &root),
            (BenchmarkProfile::Reference, Some(root)) => {
                assert!(
                    root.is_absolute(),
                    "{BENCHMARK_ROOT_ENV} must be absolute for reference runs"
                );
                Self::configured(benchmark, profile, &root)
            }
            (BenchmarkProfile::Reference, None) => {
                panic!("{BENCHMARK_PROFILE_ENV}=reference requires {BENCHMARK_ROOT_ENV}")
            }
        }
    }

    fn temporary(benchmark: &str) -> Self {
        let temporary = tempfile::tempdir().expect("create temporary benchmark filesystem root");
        let filesystem_root = temporary.path().to_path_buf();
        let run = run_directory(benchmark, &filesystem_root);
        Self {
            profile: BenchmarkProfile::Smoke,
            filesystem_root,
            run,
            _temporary_filesystem: Some(temporary),
        }
    }

    fn configured(benchmark: &str, profile: BenchmarkProfile, root: &Path) -> Self {
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
    pub const fn profile(&self) -> BenchmarkProfile {
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
        self.run.path()
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
            .tempdir_in(self.run.path())
            .unwrap_or_else(|error| {
                panic!(
                    "create benchmark sample under {}: {error}",
                    self.run.path().display()
                )
            })
    }
}

fn run_directory(benchmark: &str, root: &Path) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("dogpaddle-{}-run-", sanitized(benchmark)))
        .tempdir_in(root)
        .unwrap_or_else(|error| {
            panic!(
                "create benchmark run directory under {}: {error}",
                root.display()
            )
        })
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
