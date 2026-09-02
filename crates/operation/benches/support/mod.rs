use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use dogpaddle_bench_protocol::{BenchmarkProfile, CaseId, CaseSpec, Fields, Measurement, Run};
use tempfile::TempDir;

const REFERENCE_TURNS: &[usize] = &[1, 64, 1_024];

pub(crate) struct Config {
    pub(crate) samples: usize,
    pub(crate) codec_operations: usize,
    pub(crate) body_transactions: usize,
    pub(crate) durable_transactions: usize,
    pub(crate) warmup_transactions: usize,
    pub(crate) turns: Vec<usize>,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

impl Config {
    pub(crate) fn for_profile(profile: BenchmarkProfile) -> Self {
        match profile {
            BenchmarkProfile::Smoke => Self {
                samples: 1,
                codec_operations: 1,
                body_transactions: 1,
                durable_transactions: 1,
                warmup_transactions: 1,
                turns: vec![1],
            },
            BenchmarkProfile::Reference => Self {
                samples: 9,
                codec_operations: 100_000,
                body_transactions: 512,
                durable_transactions: 64,
                warmup_transactions: 4,
                turns: REFERENCE_TURNS.to_vec(),
            },
        }
    }

    pub(crate) fn codec_warmup_operations(&self) -> usize {
        self.codec_operations.min(1_000)
    }

    pub(crate) fn fields(&self) -> Fields {
        Fields::new()
            .with("samples", self.samples)
            .with("codec_operations_per_sample", self.codec_operations)
            .with("body_transactions_per_sample", self.body_transactions)
            .with("durable_transactions_per_sample", self.durable_transactions)
            .with("warmup_transactions", self.warmup_transactions)
            .with("turns_per_transaction", &self.turns)
            .with("execution", "single_thread")
            .with("cache", "warm")
            .with("mdbx_sync_mode", "durable")
    }
}

impl SampleStore {
    pub(crate) fn new(run: &Run, name: &str) -> Self {
        let root = run.sample(name);
        let store = root.path().join("store");
        Self { _root: root, store }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.store
    }
}

pub(crate) fn case(
    operation: &str,
    scenario: &str,
    operations: usize,
    transactions: usize,
    turns: usize,
    samples: usize,
) -> CaseSpec {
    assert!(operations > 0);
    let fields = Fields::new()
        .with("operation", operation)
        .with("operations", operations)
        .with("transactions", transactions)
        .with("turns_per_transaction", turns);
    CaseSpec::new(
        format!(
            "{operation}/{scenario}/turns={turns}/operations={operations}/transactions={transactions}"
        ),
        NonZeroUsize::new(samples).expect("Operation benchmark has samples"),
        fields,
    )
}

pub(crate) fn record(run: &mut Run, id: CaseId, durations: Vec<Duration>) {
    for elapsed in durations {
        run.push(id, Measurement::new(elapsed));
    }
}
