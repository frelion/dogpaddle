use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{
    CaseId, CaseSpec, Fields, Measurement, PairSchedule, PairVariant, Plan, Run,
};

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

    fn spec(&self, variant: &str, samples: usize) -> CaseSpec {
        CaseSpec::new(
            format!(
                "{}::{variant}::records={}::record_bytes={}::transactions={}",
                self.workload, self.records, self.record_bytes, self.transactions
            ),
            NonZeroUsize::new(samples).expect("benchmark has samples"),
            Fields::new()
                .with("variant", variant)
                .with("operations", self.records)
                .with("transactions", self.transactions)
                .with(
                    "logical_bytes",
                    self.records
                        .checked_mul(self.record_bytes)
                        .expect("benchmark logical byte count fits in usize"),
                ),
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

enum Planned {
    Single {
        case: LogCase,
        samples: usize,
        id: CaseId,
    },
    Pair {
        pair: LogPair,
        samples: usize,
        first: CaseId,
        second: CaseId,
    },
}

pub(super) struct FrozenCases {
    cases: VecDeque<Planned>,
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

impl FrozenCases {
    pub(super) const fn new() -> Self {
        Self {
            cases: VecDeque::new(),
        }
    }

    pub(super) fn single(&mut self, plan: &mut Plan, case: LogCase, samples: usize) {
        let id = plan.case(case.spec("default", samples));
        self.cases.push_back(Planned::Single { case, samples, id });
    }

    pub(super) fn pair(&mut self, plan: &mut Plan, pair: LogPair, samples: usize) {
        let second_case = pair.second();
        let (first, second) = plan.pair(
            pair.identity(),
            pair.first.spec(pair.first_data, samples),
            second_case.spec(pair.second_data, samples),
        );
        self.cases.push_back(Planned::Pair {
            pair,
            samples,
            first,
            second,
        });
    }

    pub(super) fn finish(self) {
        assert!(
            self.cases.is_empty(),
            "all frozen AppendLog benchmark cases are consumed"
        );
    }

    fn take_single(&mut self, expected: &LogCase, samples: usize) -> CaseId {
        let Planned::Single {
            case,
            samples: planned_samples,
            id,
        } = self
            .cases
            .pop_front()
            .expect("missing frozen AppendLog benchmark case")
        else {
            panic!("frozen AppendLog plan expected a paired case")
        };
        assert_eq!(&case, expected);
        assert_eq!(planned_samples, samples);
        id
    }

    fn take_pair(&mut self, expected: &LogPair, samples: usize) -> (CaseId, CaseId) {
        let Planned::Pair {
            pair,
            samples: planned_samples,
            first,
            second,
        } = self
            .cases
            .pop_front()
            .expect("missing frozen AppendLog benchmark pair")
        else {
            panic!("frozen AppendLog plan expected an ordinary case")
        };
        assert_eq!(&pair, expected);
        assert_eq!(planned_samples, samples);
        (first, second)
    }
}

pub(super) fn report_log(
    run: &mut Run,
    plan: &mut FrozenCases,
    case: &LogCase,
    samples: usize,
    mut measure: impl FnMut() -> Duration,
) {
    measure();
    let id = plan.take_single(case, samples);
    run.samples(id, |_| Measurement::new(measure()));
}

pub(super) fn report_log_pair(
    run: &mut Run,
    plan: &mut FrozenCases,
    pair: &LogPair,
    samples: usize,
    mut first: impl FnMut() -> Duration,
    mut second: impl FnMut() -> Duration,
) {
    first();
    second();
    report_pair(run, plan, pair, samples, |variant| match variant {
        PairVariant::First => first(),
        PairVariant::Second => second(),
    });
}

pub(super) fn report_log_mode_pair(
    run: &mut Run,
    plan: &mut FrozenCases,
    pair: &LogPair,
    samples: usize,
    mut measure: impl FnMut(bool) -> Duration,
) {
    measure(false);
    measure(true);
    report_pair(run, plan, pair, samples, |variant| {
        measure(matches!(variant, PairVariant::Second))
    });
}

fn report_pair(
    run: &mut Run,
    plan: &mut FrozenCases,
    pair: &LogPair,
    samples: usize,
    mut measure: impl FnMut(PairVariant) -> Duration,
) {
    let (first, second) = plan.take_pair(pair, samples);
    run.paired(first, second, PairSchedule::Counterbalanced, |variant| {
        Measurement::new(measure(variant))
    });
}
