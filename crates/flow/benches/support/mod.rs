use std::path::{Path, PathBuf};

use dogpaddle_bench_protocol::{BenchmarkProfile, RunRoot};
use tempfile::TempDir;

pub(crate) struct BenchRoot {
    root: RunRoot,
}

pub(crate) struct SamplePath {
    _root: TempDir,
    flow: PathBuf,
}

impl BenchRoot {
    pub(crate) fn from_environment(benchmark: &str) -> Self {
        Self {
            root: RunRoot::from_environment(benchmark),
        }
    }

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
        self.root.profile()
    }

    pub(crate) fn base(&self) -> &Path {
        self.root.filesystem_root()
    }

    pub(crate) fn sample(&self, scenario: &str) -> SamplePath {
        let root = self.root.sample(scenario);
        let flow = root.path().join("flow");
        SamplePath { _root: root, flow }
    }
}

impl SamplePath {
    pub(crate) fn path(&self) -> &Path {
        &self.flow
    }
}
