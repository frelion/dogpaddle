use std::{
    fs, io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, CompletionRecord, ConfigurationRecord, DurationSummary,
    EnvironmentRecord, Fields, HostEnvironment, JsonlWriter, RunRoot, SampleRecord,
};
use tempfile::TempDir;

use super::BENCHMARK;

const REFERENCE_TURNS: &[usize] = &[1, 64, 1_024];

pub(crate) struct Config {
    pub(crate) samples: usize,
    pub(crate) codec_operations: usize,
    pub(crate) body_transactions: usize,
    pub(crate) durable_transactions: usize,
    pub(crate) warmup_transactions: usize,
    pub(crate) turns: Vec<usize>,
}

pub(crate) struct BenchRoot {
    root: RunRoot,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

pub(crate) struct MachineRecords {
    samples: Vec<SampleRecord>,
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

    pub(crate) fn emit(&self) {
        let mut fields = Fields::new();
        fields.insert("samples", self.samples);
        fields.insert("codec_operations_per_sample", self.codec_operations);
        fields.insert("body_transactions_per_sample", self.body_transactions);
        fields.insert("durable_transactions_per_sample", self.durable_transactions);
        fields.insert("warmup_transactions", self.warmup_transactions);
        fields.insert("turns_per_transaction", &self.turns);
        emit_record(&ConfigurationRecord::new(
            BENCHMARK,
            self.expected_samples(),
            fields,
        ));
    }

    fn expected_samples(&self) -> NonZeroUsize {
        NonZeroUsize::new(
            (6 + 4 * self.turns.len())
                .checked_mul(self.samples)
                .expect("Operation record count fits usize"),
        )
        .expect("Operation benchmark emits data records")
    }
}

impl BenchRoot {
    pub(crate) fn from_environment() -> Self {
        Self {
            root: RunRoot::from_environment(BENCHMARK),
        }
    }

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
        self.root.profile()
    }

    pub(crate) fn sample(&self, name: &str) -> SampleStore {
        let root = self.root.sample(name);
        let store = root.path().join("store");
        SampleStore { _root: root, store }
    }

    pub(crate) fn emit_environment(&self) {
        let host = HostEnvironment::collect(Some(self.root.filesystem_root()));
        let mut fields = Fields::new();
        fields.insert("store_root", self.root.path().display().to_string());
        fields.insert("execution", "single-thread");
        fields.insert("cache", "warm");
        fields.insert("mdbx_sync_mode", "durable");
        emit_record(&EnvironmentRecord::new(
            BENCHMARK,
            self.profile(),
            host,
            fields,
        ));
    }

    pub(crate) fn assert_samples_released(&self) {
        assert!(
            fs::read_dir(self.root.path())
                .expect("read Operation benchmark run root")
                .next()
                .is_none(),
            "validated Operation sample Stores must be released immediately"
        );
    }
}

impl SampleStore {
    pub(crate) fn path(&self) -> &Path {
        &self.store
    }
}

impl MachineRecords {
    pub(crate) const fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    pub(crate) fn record(
        &mut self,
        operation: &str,
        scenario: &str,
        operations: usize,
        transactions: usize,
        turns: usize,
        durations: Vec<Duration>,
    ) {
        assert!(operations > 0);
        let summary = DurationSummary::from_samples(&durations);
        println!(
            "{operation:<10} {scenario:<28} turns/tx={turns:<5} operations={operations:<9} min={} median={} max={}",
            duration(summary.min()),
            duration(summary.median()),
            duration(summary.max())
        );

        let fields = measurement_fields(operation, operations, transactions, turns);
        let series = format!(
            "{operation}/{scenario}/turns={turns}/operations={operations}/transactions={transactions}"
        );
        for (sample, elapsed) in durations.into_iter().enumerate() {
            self.samples.push(SampleRecord::new(
                BENCHMARK,
                &series,
                sample,
                elapsed,
                fields.clone(),
            ));
        }
    }

    pub(crate) fn emit(&self) {
        println!();
        println!("=== machine-readable JSONL samples ===");
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        for sample in &self.samples {
            writer.write(sample);
        }
        writer.write(&CompletionRecord::new(BENCHMARK));
        writer.flush();
    }
}

fn measurement_fields(
    operation: &str,
    operations: usize,
    transactions: usize,
    turns: usize,
) -> Fields {
    Fields::new()
        .with("operation", operation)
        .with("operations", operations)
        .with("transactions", transactions)
        .with("turns_per_transaction", turns)
}

fn emit_record(record: &impl BenchmarkRecord) {
    JsonlWriter::new(io::stdout().lock()).write(record);
}

fn duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}
