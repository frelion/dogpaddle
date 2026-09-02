use std::{io, path::PathBuf};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, CompletionRecord, EnvironmentRecord, Fields,
    HostEnvironment, JsonlWriter, RunRoot,
};
use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::CodecError as StoreCodecError;
use tempfile::TempDir;

pub(crate) struct BenchStoreRoot {
    root: RunRoot,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

impl BenchStoreRoot {
    pub(crate) fn from_environment(benchmark: &str) -> Self {
        Self {
            root: RunRoot::from_environment(benchmark),
        }
    }

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
        self.root.profile()
    }

    pub(crate) fn sample(&self, scenario: &str) -> SampleStore {
        let root = self.root.sample(scenario);
        let store = root.path().join("store");
        SampleStore { _root: root, store }
    }

    pub(crate) fn emit_environment(&self, benchmark: &str) {
        let host = HostEnvironment::collect(Some(self.root.filesystem_root()))
            .expect("collect Change + Store benchmark environment");
        let fields = Fields::new()
            .with("mdbx_sync_mode", "durable")
            .expect("encode MDBX sync mode");
        emit_record(
            &EnvironmentRecord::new(benchmark, self.profile(), host, fields)
                .expect("build Change + Store environment record"),
        );
    }
}

impl SampleStore {
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.store
    }
}

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn complete(benchmark: &str) {
    emit_record(&CompletionRecord::new(benchmark).expect("build benchmark completion record"));
}

pub(crate) fn emit_record(record: &impl BenchmarkRecord) {
    JsonlWriter::new(io::stdout().lock())
        .write(record)
        .expect("write Change + Store benchmark JSONL");
}
