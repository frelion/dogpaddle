use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{
    CaseId, CaseSpec, Fields, Measurement, PairSchedule, PairVariant, Plan, Run,
};

#[derive(Debug, PartialEq)]
pub(super) struct BenchmarkCase {
    workload: String,
    operations: usize,
    transactions: usize,
    logical_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PairKind {
    Size,
    Mode,
}

struct PlannedPair {
    case: BenchmarkCase,
    samples: usize,
    kind: PairKind,
    first: CaseId,
    second: CaseId,
}

pub(super) struct FrozenCases {
    pairs: VecDeque<PlannedPair>,
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

    fn spec(&self, variant: &str, samples: usize) -> CaseSpec {
        CaseSpec::new(
            format!("{}::{variant}", self.workload),
            NonZeroUsize::new(samples).expect("benchmark has samples"),
            Fields::new()
                .with("variant", variant)
                .with("operations", self.operations)
                .with("transactions", self.transactions)
                .with("logical_bytes", self.logical_bytes),
        )
    }
}

impl FrozenCases {
    pub(super) const fn new() -> Self {
        Self {
            pairs: VecDeque::new(),
        }
    }

    pub(super) fn size(&mut self, plan: &mut Plan, case: BenchmarkCase, samples: usize) {
        self.declare(plan, case, samples, PairKind::Size, "Small", "Large");
    }

    pub(super) fn mode(&mut self, plan: &mut Plan, case: BenchmarkCase, samples: usize) {
        self.declare(plan, case, samples, PairKind::Mode, "Full", "Projected");
    }

    pub(super) fn finish(self) {
        assert!(
            self.pairs.is_empty(),
            "all frozen OrderedMap benchmark pairs are consumed"
        );
    }

    fn declare(
        &mut self,
        plan: &mut Plan,
        case: BenchmarkCase,
        samples: usize,
        kind: PairKind,
        first_variant: &str,
        second_variant: &str,
    ) {
        let (first, second) = plan.pair(
            &case.workload,
            case.spec(first_variant, samples),
            case.spec(second_variant, samples),
        );
        self.pairs.push_back(PlannedPair {
            case,
            samples,
            kind,
            first,
            second,
        });
    }

    fn take(
        &mut self,
        expected_case: &BenchmarkCase,
        expected_samples: usize,
        expected_kind: PairKind,
    ) -> (CaseId, CaseId) {
        let planned = self
            .pairs
            .pop_front()
            .expect("missing frozen OrderedMap benchmark pair");
        assert_eq!(&planned.case, expected_case);
        assert_eq!(planned.samples, expected_samples);
        assert_eq!(planned.kind, expected_kind);
        (planned.first, planned.second)
    }
}

pub(super) fn report_size_pair(
    run: &mut Run,
    plan: &mut FrozenCases,
    case: &BenchmarkCase,
    samples: usize,
    mut small: impl FnMut() -> Duration,
    mut large: impl FnMut() -> Duration,
) {
    small();
    large();
    let (first, second) = plan.take(case, samples, PairKind::Size);
    run.paired(first, second, PairSchedule::Alternating, |variant| {
        Measurement::new(match variant {
            PairVariant::First => small(),
            PairVariant::Second => large(),
        })
    });
}

pub(super) fn report_mode_pair<T>(
    run: &mut Run,
    plan: &mut FrozenCases,
    case: &BenchmarkCase,
    samples: usize,
    fixture: &mut T,
    mut full: impl FnMut(&mut T) -> Duration,
    mut projected: impl FnMut(&mut T) -> Duration,
) {
    full(fixture);
    projected(fixture);
    let (first, second) = plan.take(case, samples, PairKind::Mode);
    run.paired(first, second, PairSchedule::Alternating, |variant| {
        Measurement::new(match variant {
            PairVariant::First => full(fixture),
            PairVariant::Second => projected(fixture),
        })
    });
}
