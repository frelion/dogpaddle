use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, ConfigurationRecord, DurationSummary, EnvironmentRecord,
    Fields, HostEnvironment, JsonlWriter, SampleRecord, SummaryRecord, positive_usize,
    positive_usize_list,
};
use tempfile::TempDir;

const PROFILE_ENV: &str = "DOGPADDLE_OPERATION_BENCH_PROFILE";
const STORE_DIR_ENV: &str = "DOGPADDLE_OPERATION_BENCH_STORE_DIR";
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_CODEC_OPERATIONS: usize = 100_000;
const DEFAULT_BODY_TRANSACTIONS: usize = 512;
const DEFAULT_DURABLE_TRANSACTIONS: usize = 64;
const DEFAULT_WARMUP_TRANSACTIONS: usize = 4;
const DEFAULT_STEPS: &[usize] = &[1, 64, 1_024];

pub(crate) struct Config {
    pub(crate) samples: usize,
    pub(crate) codec_operations: usize,
    pub(crate) body_transactions: usize,
    pub(crate) durable_transactions: usize,
    pub(crate) warmup_transactions: usize,
    pub(crate) steps: Vec<usize>,
}

pub(crate) struct BenchRoot {
    profile: BenchmarkProfile,
    filesystem_base: PathBuf,
    run_root: TempDir,
    _temporary_base: Option<TempDir>,
}

pub(crate) struct SampleStore {
    _root: TempDir,
    store: PathBuf,
}

pub(crate) struct MachineRecords {
    samples: Vec<SampleRecord>,
    summaries: Vec<SummaryRecord>,
}

impl Config {
    pub(crate) fn load() -> Self {
        Self {
            samples: setting("DOGPADDLE_OPERATION_BENCH_SAMPLES", DEFAULT_SAMPLES),
            codec_operations: setting(
                "DOGPADDLE_OPERATION_BENCH_CODEC_OPERATIONS",
                DEFAULT_CODEC_OPERATIONS,
            ),
            body_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_BODY_TRANSACTIONS_PER_SAMPLE",
                DEFAULT_BODY_TRANSACTIONS,
            ),
            durable_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_DURABLE_TRANSACTIONS_PER_SAMPLE",
                DEFAULT_DURABLE_TRANSACTIONS,
            ),
            warmup_transactions: setting(
                "DOGPADDLE_OPERATION_BENCH_WARMUP_TRANSACTIONS",
                DEFAULT_WARMUP_TRANSACTIONS,
            ),
            steps: positive_usize_list(
                "DOGPADDLE_OPERATION_BENCH_STEPS_PER_TRANSACTION",
                DEFAULT_STEPS,
            )
            .expect("load Operation benchmark step counts"),
        }
    }

    pub(crate) fn codec_warmup_operations(&self) -> usize {
        self.codec_operations.min(1_000)
    }

    pub(crate) fn emit(&self, profile: BenchmarkProfile) {
        let mut fields = Fields::new();
        fields
            .insert("profile", profile)
            .expect("encode Operation benchmark profile");
        fields
            .insert("samples", self.samples)
            .expect("encode sample count");
        fields
            .insert("codec_operations_per_sample", self.codec_operations)
            .expect("encode codec operation count");
        fields
            .insert("body_transactions_per_sample", self.body_transactions)
            .expect("encode body transaction count");
        fields
            .insert("durable_transactions_per_sample", self.durable_transactions)
            .expect("encode durable transaction count");
        fields
            .insert("warmup_transactions", self.warmup_transactions)
            .expect("encode warmup transaction count");
        fields
            .insert("steps_per_transaction", &self.steps)
            .expect("encode step counts");
        emit_record(
            &ConfigurationRecord::new("operation_core", fields)
                .expect("build Operation configuration record"),
        );
    }
}

impl BenchRoot {
    pub(crate) fn from_environment() -> Self {
        let profile = BenchmarkProfile::from_environment(PROFILE_ENV)
            .expect("load Operation benchmark profile");
        let configured = std::env::var_os(STORE_DIR_ENV).map(PathBuf::from);
        match profile {
            BenchmarkProfile::Smoke => {
                configured.map_or_else(Self::temporary, |base| Self::configured(profile, &base))
            }
            BenchmarkProfile::Reference => {
                let base = configured.unwrap_or_else(|| {
                    panic!("{PROFILE_ENV}=reference requires an explicit {STORE_DIR_ENV}")
                });
                Self::configured(profile, &base)
            }
        }
    }

    fn temporary() -> Self {
        let temporary_base = tempfile::tempdir().expect("create temporary benchmark Store base");
        let filesystem_base = temporary_base.path().to_path_buf();
        let run_root = tempfile::Builder::new()
            .prefix("dogpaddle-operation-run-")
            .tempdir_in(&filesystem_base)
            .expect("create temporary operation benchmark run root");
        Self {
            profile: BenchmarkProfile::Smoke,
            filesystem_base,
            run_root,
            _temporary_base: Some(temporary_base),
        }
    }

    fn configured(profile: BenchmarkProfile, base: &Path) -> Self {
        if profile == BenchmarkProfile::Reference {
            assert!(
                base.is_absolute(),
                "reference benchmark Store base must be an absolute path"
            );
        }
        fs::create_dir_all(base).unwrap_or_else(|error| {
            panic!(
                "create configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        let filesystem_base = base.canonicalize().unwrap_or_else(|error| {
            panic!(
                "resolve configured benchmark Store base {}: {error}",
                base.display()
            )
        });
        assert!(
            filesystem_base.is_dir(),
            "benchmark Store base must be a directory"
        );
        let run_root = tempfile::Builder::new()
            .prefix("dogpaddle-operation-run-")
            .tempdir_in(&filesystem_base)
            .unwrap_or_else(|error| {
                panic!(
                    "create operation benchmark run root under {}: {error}",
                    filesystem_base.display()
                )
            });
        Self {
            profile,
            filesystem_base,
            run_root,
            _temporary_base: None,
        }
    }

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
        self.profile
    }

    pub(crate) fn sample(&self, name: &str) -> SampleStore {
        let root = tempfile::Builder::new()
            .prefix(&format!("dogpaddle-{name}-"))
            .tempdir_in(self.run_root.path())
            .unwrap_or_else(|error| {
                panic!(
                    "create Operation benchmark sample under {}: {error}",
                    self.run_root.path().display()
                )
            });
        let store = root.path().join("store");
        SampleStore { _root: root, store }
    }

    pub(crate) fn emit_environment(&self) {
        let host = HostEnvironment::collect(Some(&self.filesystem_base))
            .expect("collect Operation benchmark environment");
        let mut fields = Fields::new();
        fields
            .insert("store_root", self.run_root.path().display().to_string())
            .expect("encode benchmark Store root");
        fields
            .insert("execution", "single-thread")
            .expect("encode execution mode");
        fields.insert("cache", "warm").expect("encode cache mode");
        fields
            .insert("mdbx_sync_mode", "durable")
            .expect("encode MDBX sync mode");
        emit_record(
            &EnvironmentRecord::for_profile("operation_core", self.profile, host, fields)
                .expect("build Operation environment record"),
        );
    }

    pub(crate) fn assert_samples_released(&self) {
        assert!(
            fs::read_dir(self.run_root.path())
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
            summaries: Vec::new(),
        }
    }

    pub(crate) fn record(
        &mut self,
        operation: &str,
        scenario: &str,
        operations: usize,
        transactions: usize,
        steps_per_transaction: usize,
        durations: Vec<Duration>,
    ) {
        assert!(operations > 0);
        let summary =
            DurationSummary::from_samples(&durations).expect("summarize Operation samples");
        println!(
            "{operation:<10} {scenario:<28} steps/tx={steps_per_transaction:<5} operations={operations:<9} min={} median={} max={}",
            duration(summary.min()),
            duration(summary.median()),
            duration(summary.max())
        );

        let fields = measurement_fields(operation, operations, transactions, steps_per_transaction);
        for (sample, elapsed) in durations.into_iter().enumerate() {
            let mut sample_fields = fields.clone();
            let ns_per_operation =
                elapsed.as_nanos() / u128::try_from(operations).expect("operation count fits u128");
            sample_fields
                .insert("ns_per_operation", ns_per_operation)
                .expect("encode per-operation duration");
            self.samples.push(
                SampleRecord::new("operation_core", scenario, sample, elapsed, sample_fields)
                    .expect("build Operation sample record"),
            );
        }
        self.summaries.push(
            SummaryRecord::new("operation_core", scenario, summary, fields)
                .expect("build Operation summary record"),
        );
    }

    pub(crate) fn emit(&self) {
        println!();
        println!("=== machine-readable JSONL samples and summaries ===");
        let stdout = io::stdout();
        let mut writer = JsonlWriter::new(stdout.lock());
        for sample in &self.samples {
            writer
                .write(sample)
                .expect("write Operation benchmark sample record");
        }
        for summary in &self.summaries {
            writer
                .write(summary)
                .expect("write Operation benchmark summary record");
        }
        writer
            .flush()
            .expect("flush Operation benchmark protocol records");
    }
}

fn measurement_fields(
    operation: &str,
    operations: usize,
    transactions: usize,
    steps_per_transaction: usize,
) -> Fields {
    Fields::new()
        .with("operation", operation)
        .expect("encode Operation name")
        .with("operations", operations)
        .expect("encode operation count")
        .with("transactions", transactions)
        .expect("encode transaction count")
        .with("steps_per_transaction", steps_per_transaction)
        .expect("encode step count")
}

fn setting(name: &str, default: usize) -> usize {
    positive_usize(name, default).expect("load positive Operation benchmark setting")
}

fn emit_record(record: &impl BenchmarkRecord) {
    JsonlWriter::new(io::stdout().lock())
        .write(record)
        .expect("write Operation benchmark JSONL");
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
