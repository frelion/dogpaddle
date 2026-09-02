use std::time::Duration;

use dogpaddle_bench_protocol::{
    BenchmarkProfile, BenchmarkRecord, CompletionRecord, EnvironmentRecord, Fields,
    HostEnvironment, JsonlWriter, RunRoot, require_benchmark_build,
};
use tempfile::TempDir;

pub(crate) struct BenchRoot {
    benchmark: &'static str,
    root: RunRoot,
}

#[must_use = "the benchmark root owns the temporary Store directory for the full run"]
pub(crate) fn initialize(benchmark: &'static str) -> BenchRoot {
    require_benchmark_build(benchmark);
    let root = BenchRoot::from_process(benchmark);
    root.emit_environment();
    root
}

impl BenchRoot {
    fn from_process(benchmark: &'static str) -> Self {
        let root = RunRoot::from_environment(benchmark);
        Self { benchmark, root }
    }

    fn emit_environment(&self) {
        let host = HostEnvironment::collect(Some(self.root.filesystem_root()))
            .expect("collect Store benchmark host environment");
        let fields = Fields::new()
            .with("mdbx_sync_mode", "durable")
            .expect("construct Store environment fields");
        let record = EnvironmentRecord::new(self.benchmark, self.root.profile(), host, fields)
            .expect("construct Store environment record");
        write_record(&record);
    }

    pub(crate) fn sample(&self, scenario: &str) -> TempDir {
        self.root.sample(scenario)
    }

    pub(crate) const fn profile(&self) -> BenchmarkProfile {
        self.root.profile()
    }
}

pub(crate) fn write_record(record: &impl BenchmarkRecord) {
    let stdout = std::io::stdout();
    JsonlWriter::new(stdout.lock())
        .write(record)
        .expect("write Store benchmark JSONL record");
}

pub(crate) fn complete(benchmark: &'static str) {
    write_record(
        &CompletionRecord::new(benchmark).expect("construct Store benchmark completion record"),
    );
}

#[allow(dead_code)]
pub(crate) fn average_duration(total: Duration, operations: usize) -> String {
    let nanos = total.as_nanos()
        / u128::try_from(operations).expect("benchmark operation count fits in u128");
    format_duration(Duration::from_nanos(
        u64::try_from(nanos).expect("average duration fits in u64 nanoseconds"),
    ))
}

pub(crate) fn format_duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}
