use std::path::PathBuf;

use dogpaddle_change::{Change, decode_change};
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

pub(crate) fn decode_entry(encoded: &[u8]) -> Change {
    decode_change(encoded).expect("decode fixture Change")
}
