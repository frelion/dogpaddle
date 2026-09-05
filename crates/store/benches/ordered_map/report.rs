use std::time::Duration;

use serde_json::json;

use super::support::{PairVariant, StoreRun, measure_pair};

#[derive(Debug, PartialEq)]
pub(super) struct BenchmarkCase {
    workload: String,
    operations: usize,
    transactions: usize,
    logical_bytes: usize,
}

impl BenchmarkCase {
    pub(super) fn per_operation(
        workload: impl Into<String>,
        operations: usize,
        transactions: usize,
        bytes_per_operation: usize,
    ) -> Self {
        Self {
            workload: workload.into(),
            operations,
            transactions,
            logical_bytes: operations
                .checked_mul(bytes_per_operation)
                .expect("benchmark logical byte count fits in usize"),
        }
    }

    fn series(&self, variant: &str) -> String {
        format!("{}::{variant}", self.workload)
    }
}

pub(super) fn report_size_pair(
    run: &StoreRun,
    case: &BenchmarkCase,
    samples: usize,
    mut small: impl FnMut() -> Duration,
    mut large: impl FnMut() -> Duration,
) {
    small();
    large();
    report_pair(
        run,
        case,
        "size",
        "Small",
        "Large",
        samples,
        |variant| match variant {
            PairVariant::First => small(),
            PairVariant::Second => large(),
        },
    );
}

pub(super) fn report_mode_pair<T>(
    run: &StoreRun,
    case: &BenchmarkCase,
    samples: usize,
    fixture: &mut T,
    mut full: impl FnMut(&mut T) -> Duration,
    mut projected: impl FnMut(&mut T) -> Duration,
) {
    full(fixture);
    projected(fixture);
    report_pair(
        run,
        case,
        "mode",
        "Full",
        "Projected",
        samples,
        |variant| match variant {
            PairVariant::First => full(fixture),
            PairVariant::Second => projected(fixture),
        },
    );
}

fn report_pair(
    run: &StoreRun,
    case: &BenchmarkCase,
    pair_kind: &str,
    first_variant: &str,
    second_variant: &str,
    samples: usize,
    mut measure: impl FnMut(PairVariant) -> Duration,
) {
    for sample in 0..samples {
        let ab = pair_is_ab(sample);
        let (first, second) = measure_pair(ab, &mut measure);
        let order = if ab { "ab" } else { "ba" };
        for (variant, elapsed) in [(first_variant, first), (second_variant, second)] {
            run.sample(
                &case.series(variant),
                sample,
                elapsed,
                &json!({
                    "pair": case.workload,
                    "pair_kind": pair_kind,
                    "order": order,
                    "variant": variant,
                    "operations": case.operations,
                    "transactions": case.transactions,
                    "logical_bytes": case.logical_bytes,
                }),
            );
        }
    }
}

pub(super) fn validate_pair_schedule(samples: usize) {
    assert!(
        samples >= 2,
        "AB/BA measurement requires at least two samples"
    );
    assert_eq!([pair_is_ab(0), pair_is_ab(1)], [true, false]);
}

const fn pair_is_ab(sample: usize) -> bool {
    sample.is_multiple_of(2)
}
