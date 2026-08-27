use dogpaddle_bench_protocol::BenchmarkProfile;

use crate::support::setting;

const DEFAULT_ROWS_PER_CHANGE: usize = 1_024;
const DEFAULT_CHANGES_PER_TRANSACTION: usize = 32;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_SMOKE_SAMPLES: usize = 7;
const DEFAULT_SMOKE_WARMUPS: usize = 1;
const DEFAULT_REFERENCE_SAMPLES: usize = 15;
const DEFAULT_REFERENCE_WARMUPS: usize = 3;
const DEFAULT_MAX_WORKING_SET_BYTES: usize = 512 * 1_024 * 1_024;
const SMOKE_TRANSACTIONS_PER_SAMPLE: usize = 2;
const REFERENCE_TRANSACTIONS_PER_SAMPLE: usize = 8;

pub(crate) struct Config {
    pub(crate) profile: BenchmarkProfile,
    pub(crate) rows_per_change: usize,
    pub(crate) changes_per_transaction: usize,
    pub(crate) transactions_per_sample: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) samples: usize,
    pub(crate) warmups: usize,
    pub(crate) max_working_set_bytes: usize,
}

impl Config {
    pub(crate) fn load(profile: BenchmarkProfile) -> Self {
        let default_transactions = match profile {
            BenchmarkProfile::Smoke => SMOKE_TRANSACTIONS_PER_SAMPLE,
            BenchmarkProfile::Reference => REFERENCE_TRANSACTIONS_PER_SAMPLE,
        };
        let (default_samples, default_warmups) = match profile {
            BenchmarkProfile::Smoke => (DEFAULT_SMOKE_SAMPLES, DEFAULT_SMOKE_WARMUPS),
            BenchmarkProfile::Reference => (DEFAULT_REFERENCE_SAMPLES, DEFAULT_REFERENCE_WARMUPS),
        };
        let config = Self {
            profile,
            rows_per_change: setting(
                "DOGPADDLE_CHANGE_STORE_BENCH_ROWS_PER_CHANGE",
                DEFAULT_ROWS_PER_CHANGE,
            ),
            changes_per_transaction: setting(
                "DOGPADDLE_CHANGE_STORE_BENCH_CHANGES_PER_TX",
                DEFAULT_CHANGES_PER_TRANSACTION,
            ),
            transactions_per_sample: setting(
                "DOGPADDLE_CHANGE_STORE_BENCH_TRANSACTIONS_PER_SAMPLE",
                default_transactions,
            ),
            payload_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_BENCH_PAYLOAD_BYTES",
                DEFAULT_PAYLOAD_BYTES,
            ),
            samples: setting("DOGPADDLE_CHANGE_STORE_BENCH_SAMPLES", default_samples),
            warmups: setting("DOGPADDLE_CHANGE_STORE_BENCH_WARMUPS", default_warmups),
            max_working_set_bytes: setting(
                "DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES",
                DEFAULT_MAX_WORKING_SET_BYTES,
            ),
        };
        assert!(
            config.transactions_per_sample >= 2,
            "DOGPADDLE_CHANGE_STORE_BENCH_TRANSACTIONS_PER_SAMPLE must be at least two so replay really spans multiple pages"
        );
        config
            .total_changes()
            .checked_mul(config.rows_per_change)
            .expect("benchmark row count fits usize");
        config
    }

    pub(crate) fn total_changes(&self) -> usize {
        self.changes_per_transaction
            .checked_mul(self.transactions_per_sample)
            .expect("Changes per sample fit usize")
    }
}
