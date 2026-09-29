use std::path::PathBuf;

use dogpaddle_perf_context::RunRoot;
use tempfile::TempDir;

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

impl SampleStore {
    pub(crate) fn new(run: &RunRoot, scenario: &str) -> Self {
        let root = run.sample(scenario);
        let store = root.path().join("store");
        Self { _root: root, store }
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.store
    }
}
