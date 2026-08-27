use std::num::NonZeroUsize;

use dogpaddle_bench_protocol::string;
use dogpaddle_change_store_integration::{ChangeWorkloadSpec, DiffModel, WorkloadPersona};

use crate::support::setting;

pub(super) const BENCHMARK: &str = "change_append_log_endurance";
pub(super) const MODE_FILTER_ENV: &str = "DOGPADDLE_CHANGE_STORE_ENDURANCE_WORKLOAD_MODE";
const DEFAULT_MAX_WORKING_SET_BYTES: usize = 1_073_741_824;
const DEFAULT_MAX_TOTAL_WRITTEN_BYTES: usize = 1_099_511_627_776;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkloadMode {
    HeterogeneousPipeline,
    HomogeneousControl,
}

impl WorkloadMode {
    pub(super) const ALL: [Self; 2] = [Self::HeterogeneousPipeline, Self::HomogeneousControl];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::HeterogeneousPipeline => "heterogeneous_pipeline",
            Self::HomogeneousControl => "homogeneous_control",
        }
    }

    pub(super) const fn persona(self) -> WorkloadPersona {
        match self {
            Self::HeterogeneousPipeline => WorkloadPersona::Heterogeneous,
            Self::HomogeneousControl => WorkloadPersona::BlobEvent4,
        }
    }

    pub(super) const fn diff_model(self) -> DiffModel {
        self.persona().descriptor().diff_model
    }
}

pub(super) struct Config {
    pub(super) profile: String,
    pub(super) rows_per_change: usize,
    pub(super) changes_per_cycle: usize,
    pub(super) cycles: usize,
    pub(super) payload_bytes: usize,
    pub(super) retained_encoded_bytes: usize,
    pub(super) truncate_items: NonZeroUsize,
    pub(super) consumer_page_items: usize,
    pub(super) consumer_page_bytes: usize,
    pub(super) reopen_interval_cycles: NonZeroUsize,
    pub(super) max_working_set_bytes: usize,
    pub(super) max_total_written_bytes: usize,
    pub(super) workload_modes: Vec<WorkloadMode>,
}

impl Config {
    pub(super) fn from_environment() -> Self {
        let profile = string("DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE", "smoke")
            .expect("load Change + Store endurance workload profile");
        let defaults = match profile.as_str() {
            "smoke" => Defaults {
                rows_per_change: 256,
                changes_per_cycle: 8,
                cycles: 16,
                payload_bytes: 128,
                retained_encoded_bytes: 4 * 1_024 * 1_024,
                truncate_items: 64,
                consumer_page_items: 8,
                consumer_page_bytes: 32 * 1_024 * 1_024,
                reopen_interval_cycles: 4,
            },
            "full" => Defaults {
                rows_per_change: 4_096,
                changes_per_cycle: 32,
                cycles: 500,
                payload_bytes: 1_024,
                retained_encoded_bytes: 512 * 1_024 * 1_024,
                truncate_items: 4_096,
                consumer_page_items: 16,
                consumer_page_bytes: 128 * 1_024 * 1_024,
                reopen_interval_cycles: 25,
            },
            _ => panic!("DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE must be smoke or full"),
        };
        let config = Self {
            profile,
            rows_per_change: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_ROWS_PER_CHANGE",
                defaults.rows_per_change,
            ),
            changes_per_cycle: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_CHANGES_PER_CYCLE",
                defaults.changes_per_cycle,
            ),
            cycles: setting("DOGPADDLE_CHANGE_STORE_ENDURANCE_CYCLES", defaults.cycles),
            payload_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_PAYLOAD_BYTES",
                defaults.payload_bytes,
            ),
            retained_encoded_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_RETAINED_BYTES",
                defaults.retained_encoded_bytes,
            ),
            truncate_items: non_zero_setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_TRUNCATE_ITEMS",
                defaults.truncate_items,
            ),
            consumer_page_items: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_ITEMS",
                defaults.consumer_page_items,
            ),
            consumer_page_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_BYTES",
                defaults.consumer_page_bytes,
            ),
            reopen_interval_cycles: non_zero_setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_REOPEN_INTERVAL_CYCLES",
                defaults.reopen_interval_cycles,
            ),
            max_working_set_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES",
                DEFAULT_MAX_WORKING_SET_BYTES,
            ),
            max_total_written_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_TOTAL_WRITTEN_BYTES",
                DEFAULT_MAX_TOTAL_WRITTEN_BYTES,
            ),
            workload_modes: workload_modes(),
        };
        config.validate();
        config
    }

    pub(super) fn spec(&self, mode: WorkloadMode, ordinal: usize) -> ChangeWorkloadSpec {
        if mode == WorkloadMode::HomogeneousControl {
            return ChangeWorkloadSpec::new(self.rows_per_change, self.payload_bytes);
        }

        let half_rows = self.rows_per_change.div_ceil(2);
        let half_payload = self.payload_bytes.div_ceil(2);
        match ordinal % 4 {
            0 => ChangeWorkloadSpec::new(self.rows_per_change, self.payload_bytes),
            1 => ChangeWorkloadSpec::new(
                self.rows_per_change
                    .checked_add(1)
                    .expect("heterogeneous row variation fits usize"),
                half_payload,
            ),
            2 => ChangeWorkloadSpec::new(
                half_rows,
                self.payload_bytes
                    .checked_add(3)
                    .expect("heterogeneous payload variation fits usize"),
            ),
            _ => ChangeWorkloadSpec::new(
                self.rows_per_change,
                self.payload_bytes
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(1))
                    .expect("heterogeneous payload variation fits usize"),
            ),
        }
    }

    fn validate(&self) {
        for ordinal in 0..8 {
            let spec = self.spec(WorkloadMode::HeterogeneousPipeline, ordinal);
            assert!(
                i32::try_from(spec.rows).is_ok(),
                "rows per heterogeneous Change must fit Arrow i32 offsets"
            );
            let payload = spec
                .rows
                .checked_mul(spec.payload_bytes)
                .expect("heterogeneous payload bytes fit usize");
            assert!(
                i32::try_from(payload).is_ok(),
                "rows * payload bytes must fit Arrow Binary i32 offsets"
            );
        }
        assert!(
            self.retained_encoded_bytes <= self.max_working_set_bytes,
            "retained byte target exceeds the configured working-set budget"
        );
    }
}

struct Defaults {
    rows_per_change: usize,
    changes_per_cycle: usize,
    cycles: usize,
    payload_bytes: usize,
    retained_encoded_bytes: usize,
    truncate_items: usize,
    consumer_page_items: usize,
    consumer_page_bytes: usize,
    reopen_interval_cycles: usize,
}

fn non_zero_setting(name: &str, default: usize) -> NonZeroUsize {
    NonZeroUsize::new(setting(name, default)).expect("benchmark setting is non-zero")
}

fn workload_modes() -> Vec<WorkloadMode> {
    let selected = string(MODE_FILTER_ENV, "all").expect("load endurance workload-mode filter");
    match selected.as_str() {
        "all" => WorkloadMode::ALL.to_vec(),
        "heterogeneous_pipeline" => vec![WorkloadMode::HeterogeneousPipeline],
        "homogeneous_control" => vec![WorkloadMode::HomogeneousControl],
        _ => {
            panic!("{MODE_FILTER_ENV} must be all, heterogeneous_pipeline, or homogeneous_control")
        }
    }
}
