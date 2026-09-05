use std::time::Duration;

use serde_json::json;

use super::support::{PairVariant, StoreRun, measure_pair};

#[derive(Debug, PartialEq)]
pub(super) struct LogCase {
    workload: String,
    records: usize,
    record_bytes: usize,
    transactions: usize,
}

impl LogCase {
    pub(super) fn new(
        workload: impl Into<String>,
        records: usize,
        record_bytes: usize,
        transactions: usize,
    ) -> Self {
        Self {
            workload: workload.into(),
            records,
            record_bytes,
            transactions,
        }
    }

    fn series(&self, variant: &str) -> String {
        format!(
            "{}::{variant}::records={}::record_bytes={}::transactions={}",
            self.workload, self.records, self.record_bytes, self.transactions
        )
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct LogPair {
    scenario: String,
    first: LogCase,
    second_workload: String,
    first_data: &'static str,
    second_data: &'static str,
}

impl LogPair {
    pub(super) fn variants(
        scenario: impl Into<String>,
        first: LogCase,
        second_workload: impl Into<String>,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            first,
            second_workload: second_workload.into(),
            first_data: "first",
            second_data: "second",
        }
    }

    pub(super) fn modes(
        scenario: impl Into<String>,
        first: LogCase,
        second_workload: impl Into<String>,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            first,
            second_workload: second_workload.into(),
            first_data: "default",
            second_data: "default",
        }
    }

    fn second(&self) -> LogCase {
        LogCase {
            workload: self.second_workload.clone(),
            records: self.first.records,
            record_bytes: self.first.record_bytes,
            transactions: self.first.transactions,
        }
    }

    fn identity(&self) -> String {
        format!(
            "{}::{}::{}",
            self.scenario, self.first.workload, self.second_workload
        )
    }
}

pub(super) fn report_log(
    run: &StoreRun,
    case: &LogCase,
    samples: usize,
    mut measure: impl FnMut() -> Duration,
) {
    measure();
    for sample in 0..samples {
        emit(run, case, "default", sample, measure(), None);
    }
}

pub(super) fn report_log_pair(
    run: &StoreRun,
    pair: &LogPair,
    samples: usize,
    mut first: impl FnMut() -> Duration,
    mut second: impl FnMut() -> Duration,
) {
    first();
    second();
    report_pair(run, pair, samples, |variant| match variant {
        PairVariant::First => first(),
        PairVariant::Second => second(),
    });
}

pub(super) fn report_log_mode_pair(
    run: &StoreRun,
    pair: &LogPair,
    samples: usize,
    mut measure: impl FnMut(bool) -> Duration,
) {
    measure(false);
    measure(true);
    report_pair(run, pair, samples, |variant| {
        measure(matches!(variant, PairVariant::Second))
    });
}

fn report_pair(
    run: &StoreRun,
    pair: &LogPair,
    samples: usize,
    mut measure: impl FnMut(PairVariant) -> Duration,
) {
    let second = pair.second();
    for sample in 0..samples {
        let ab = pair_is_ab(sample);
        let (first_elapsed, second_elapsed) = measure_pair(ab, &mut measure);
        let order = if ab { "ab" } else { "ba" };
        emit(
            run,
            &pair.first,
            pair.first_data,
            sample,
            first_elapsed,
            Some((&pair.identity(), order)),
        );
        emit(
            run,
            &second,
            pair.second_data,
            sample,
            second_elapsed,
            Some((&pair.identity(), order)),
        );
    }
}

pub(super) fn validate_pair_schedule(samples: usize) {
    assert!(
        samples >= 4,
        "AB/BA/BA/AB measurement requires at least four samples"
    );
    assert_eq!(
        [pair_is_ab(0), pair_is_ab(1), pair_is_ab(2), pair_is_ab(3),],
        [true, false, false, true]
    );
}

const fn pair_is_ab(sample: usize) -> bool {
    matches!(sample % 4, 0 | 3)
}

fn emit(
    run: &StoreRun,
    case: &LogCase,
    variant: &str,
    sample: usize,
    elapsed: Duration,
    pair: Option<(&str, &str)>,
) {
    run.sample(
        &case.series(variant),
        sample,
        elapsed,
        &json!({
            "variant": variant,
            "pair": pair.map(|value| value.0),
            "order": pair.map(|value| value.1),
            "operations": case.records,
            "transactions": case.transactions,
            "logical_bytes": case.records.checked_mul(case.record_bytes).unwrap(),
            "record_bytes": case.record_bytes,
        }),
    );
}
