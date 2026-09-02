use std::path::PathBuf;

use dogpaddle_bench_protocol::Run;
use tempfile::TempDir;

pub(crate) struct BenchRoot {
    path: PathBuf,
}

impl BenchRoot {
    pub(crate) fn new(run: &Run) -> Self {
        Self {
            path: run.path().to_path_buf(),
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
