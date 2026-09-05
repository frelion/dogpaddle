#![allow(dead_code)]

use std::{io::Write, path::PathBuf, time::Duration};

use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use serde_json::{Value, json};
use tempfile::TempDir;

pub(crate) struct StoreRun {
    benchmark: &'static str,
    root: RunRoot,
}

impl StoreRun {
    pub(crate) fn new(
        benchmark: &'static str,
        profile: PerformanceProfile,
        configuration: &Value,
    ) -> Self {
        require_release_build(benchmark);
        let root = RunRoot::from_environment(benchmark);
        emit(&json!({
            "record": "context",
            "benchmark": benchmark,
            "profile": profile,
            "result_directory": root.path().display().to_string(),
            "host": HostEnvironment::collect(Some(root.filesystem_root())),
            "configuration": configuration,
        }));
        Self { benchmark, root }
    }

    pub(crate) const fn root(&self) -> &RunRoot {
        &self.root
    }

    pub(crate) fn sample(&self, series: &str, sample: usize, elapsed: Duration, fields: &Value) {
        emit(&json!({
            "record": "sample",
            "benchmark": self.benchmark,
            "series": series,
            "sample": sample,
            "elapsed_ns": nanos(elapsed),
            "fields": fields,
        }));
    }

    #[allow(dead_code)]
    pub(crate) fn observation(&self, series: &str, sample: usize, fields: &Value) {
        emit(&json!({
            "record": "observation",
            "benchmark": self.benchmark,
            "series": series,
            "sample": sample,
            "fields": fields,
        }));
    }

    pub(crate) fn finish(&self) {
        emit(&json!({
            "record": "completion",
            "benchmark": self.benchmark,
        }));
    }
}

pub(crate) struct BenchRoot {
    path: PathBuf,
}

impl BenchRoot {
    pub(crate) fn new(root: &RunRoot) -> Self {
        Self {
            path: root.path().to_path_buf(),
        }
    }

    pub(crate) fn sample(&self, scenario: &str) -> TempDir {
        let prefix = scenario
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '-' {
                    character
                } else {
                    '-'
                }
            })
            .collect::<String>();
        tempfile::Builder::new()
            .prefix(&format!("dogpaddle-{prefix}-"))
            .tempdir_in(&self.path)
            .expect("create Store benchmark sample")
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PairVariant {
    First,
    Second,
}

pub(crate) fn measure_pair<T>(ab: bool, mut measure: impl FnMut(PairVariant) -> T) -> (T, T) {
    if ab {
        (measure(PairVariant::First), measure(PairVariant::Second))
    } else {
        let second = measure(PairVariant::Second);
        let first = measure(PairVariant::First);
        (first, second)
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("performance duration fits in u64 nanoseconds")
}

fn emit(value: &Value) {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, value).expect("serialize Store performance record");
    stdout
        .write_all(b"\n")
        .expect("write Store performance record");
    stdout.flush().expect("flush Store performance record");
}
