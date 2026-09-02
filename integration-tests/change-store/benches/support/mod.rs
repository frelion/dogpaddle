use std::path::PathBuf;

use dogpaddle_bench_protocol::Run;
use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::CodecError as StoreCodecError;
use tempfile::TempDir;

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

impl SampleStore {
    pub(crate) fn new(run: &Run, scenario: &str) -> Self {
        let root = run.sample(scenario);
        let store = root.path().join("store");
        Self { _root: root, store }
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.store
    }
}

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}
