use std::{
    io::{self, Write},
    path::Path,
    time::Duration,
};

use tempfile::TempDir;

use crate::{
    Artifact, BenchmarkProfile, CaseSpec, Fields, HostEnvironment, ObservationSpec,
    PROTOCOL_VERSION, PairSchedule, PairSide, PairVariant, Record, RunRoot, RunValidator,
    measure_pair_with, report, require_benchmark_build,
};

/// Internal switch used by `cargo xtask bench-plan-check`.
pub const BENCHMARK_PLAN_ONLY_ENV: &str = "DOGPADDLE_BENCH_PLAN_ONLY";

/// Compact identity of a declared duration case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaseId(usize);

/// Compact identity of a declared observation series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationId(usize);

/// One owner-timed and owner-validated sample.
#[derive(Clone, Debug)]
pub struct Measurement {
    elapsed: Duration,
    fields: Fields,
}

impl Measurement {
    /// Creates a sample with no dynamic fields.
    #[must_use]
    pub fn new(elapsed: Duration) -> Self {
        Self {
            elapsed,
            fields: Fields::new(),
        }
    }

    /// Creates a sample carrying facts that vary by sample.
    #[must_use]
    pub const fn with_fields(elapsed: Duration, fields: Fields) -> Self {
        Self { elapsed, fields }
    }
}

struct CaseRun {
    spec: CaseSpec,
    samples: Vec<Measurement>,
}

struct ObservationRun {
    spec: ObservationSpec,
    values: Vec<Fields>,
}

/// Frozen, profile-specific benchmark plan built before any fixture or timing.
pub struct Plan {
    profile: BenchmarkProfile,
    configuration: Fields,
    cases: Vec<CaseSpec>,
    observations: Vec<ObservationSpec>,
}

impl Plan {
    /// Starts a plan for one exact profile and owner configuration.
    #[must_use]
    pub const fn new(profile: BenchmarkProfile, configuration: Fields) -> Self {
        Self {
            profile,
            configuration,
            cases: Vec::new(),
            observations: Vec::new(),
        }
    }

    /// Adds one duration case and returns its frozen identity.
    ///
    /// # Panics
    ///
    /// Panics for a duplicate sample or observation series.
    #[track_caller]
    pub fn case(&mut self, spec: CaseSpec) -> CaseId {
        self.assert_new_series(spec.series());
        let id = CaseId(self.cases.len());
        self.cases.push(spec);
        id
    }

    /// Adds both semantic sides of one paired comparison.
    ///
    /// # Panics
    ///
    /// Panics when the sides have unequal counts or duplicate identities.
    #[track_caller]
    pub fn pair(
        &mut self,
        pair: impl Into<String>,
        first: CaseSpec,
        second: CaseSpec,
    ) -> (CaseId, CaseId) {
        assert_eq!(
            first.samples(),
            second.samples(),
            "paired cases require equal sample counts"
        );
        let pair = pair.into();
        let first = self.case(first.paired(&pair, PairSide::First));
        let second = self.case(second.paired(pair, PairSide::Second));
        (first, second)
    }

    /// Adds one non-duration observation series.
    ///
    /// # Panics
    ///
    /// Panics for a duplicate sample or observation series.
    #[track_caller]
    pub fn observation(&mut self, spec: ObservationSpec) -> ObservationId {
        self.assert_new_series(spec.series());
        let id = ObservationId(self.observations.len());
        self.observations.push(spec);
        id
    }

    fn assert_new_series(&self, series: &str) {
        assert!(
            !self.cases.iter().any(|case| case.series() == series)
                && !self
                    .observations
                    .iter()
                    .any(|observation| observation.series() == series),
            "duplicate benchmark series {series:?}"
        );
    }
}

/// Concrete lifecycle, sampling, and evidence harness for one benchmark target.
pub struct Run {
    benchmark: &'static str,
    profile: BenchmarkProfile,
    root: Option<RunRoot>,
    host: HostEnvironment,
    configuration: Fields,
    cases: Vec<CaseRun>,
    observations: Vec<ObservationRun>,
    plan_only: bool,
}

impl Run {
    /// Starts an in-memory benchmark process.
    ///
    /// # Panics
    ///
    /// Panics outside a benchmark build or when the frozen profile differs
    /// from the process profile.
    #[must_use]
    pub fn memory(benchmark: &'static str, plan: Plan) -> Self {
        require_benchmark_build(benchmark);
        assert_eq!(
            plan.profile,
            BenchmarkProfile::from_environment(),
            "benchmark plan profile differs from the process profile"
        );
        Self::from_plan(benchmark, plan, None)
    }

    /// Starts a benchmark process with the common fixed-filesystem rules.
    ///
    /// # Panics
    ///
    /// Panics outside a benchmark build, when the frozen profile differs from
    /// the process profile, or when a required persistent run root is invalid.
    #[must_use]
    pub fn persistent(benchmark: &'static str, plan: Plan) -> Self {
        require_benchmark_build(benchmark);
        assert_eq!(
            plan.profile,
            BenchmarkProfile::from_environment(),
            "benchmark plan profile differs from the process profile"
        );
        let root = (!plan_only()).then(|| RunRoot::from_environment(benchmark));
        Self::from_plan(benchmark, plan, root)
    }

    /// Selected smoke/reference profile.
    #[must_use]
    pub const fn profile(&self) -> BenchmarkProfile {
        self.profile
    }

    /// Returns whether this process should emit only its frozen run header.
    #[must_use]
    pub const fn is_plan_only(&self) -> bool {
        self.plan_only
    }

    /// Creates a fresh fixture directory under this run's filesystem root.
    ///
    /// # Panics
    ///
    /// Panics for an in-memory run or when the directory cannot be created.
    #[must_use]
    #[track_caller]
    pub fn sample(&self, scenario: &str) -> TempDir {
        self.root
            .as_ref()
            .expect("in-memory benchmark has no sample directory")
            .sample(scenario)
    }

    /// Returns the fresh process run directory.
    ///
    /// # Panics
    ///
    /// Panics for an in-memory run.
    #[must_use]
    #[track_caller]
    pub fn path(&self) -> &Path {
        self.root
            .as_ref()
            .expect("in-memory benchmark has no run directory")
            .path()
    }

    /// Records one custom-scheduled sample.
    ///
    /// # Panics
    ///
    /// Panics for an unknown case or too many samples.
    #[track_caller]
    pub fn push(&mut self, case: CaseId, measurement: Measurement) {
        let run = self.cases.get_mut(case.0).expect("unknown benchmark case");
        assert!(
            run.samples.len() < run.spec.samples().get(),
            "benchmark case {:?} received too many samples",
            run.spec.series()
        );
        run.samples.push(measurement);
    }

    /// Measures one frozen ordinary case using owner-defined timing.
    #[track_caller]
    pub fn samples(&mut self, id: CaseId, mut measure: impl FnMut(&Self) -> Measurement) {
        let count = self.case(id).spec.samples().get();
        for _ in 0..count {
            let measurement = measure(self);
            self.push(id, measurement);
        }
    }

    /// Measures one frozen deterministic paired comparison.
    ///
    /// The owner remains responsible for any warmup before this call.
    ///
    /// # Panics
    ///
    /// Panics when the cases have unequal counts or duplicate identities.
    #[track_caller]
    pub fn paired(
        &mut self,
        first: CaseId,
        second: CaseId,
        schedule: PairSchedule,
        mut measure: impl FnMut(PairVariant) -> Measurement,
    ) {
        self.assert_pair(first, second);
        let count = self.case(first).spec.samples().get();
        for sample in 0..count {
            let measurements = measure_pair_with(schedule.order(sample), &mut measure);
            self.push(first, measurements.first);
            self.push(second, measurements.second);
        }
    }

    /// Records one observation.
    ///
    /// # Panics
    ///
    /// Panics for an unknown observation or too many records.
    #[track_caller]
    pub fn observe(&mut self, observation: ObservationId, fields: Fields) {
        let run = self
            .observations
            .get_mut(observation.0)
            .expect("unknown benchmark observation");
        assert!(
            run.values.len() < run.spec.samples().get(),
            "benchmark observation {:?} received too many records",
            run.spec.series()
        );
        run.values.push(fields);
    }

    /// Emits only the frozen run header for `cargo xtask bench-plan-check`.
    ///
    /// This path never creates a fixture, records a sample, runs an owner
    /// oracle, or emits completion. Normal benchmark consumers must use a
    /// complete artifact from [`Self::finish`].
    ///
    /// # Panics
    ///
    /// Panics outside the internal plan-only process mode or when stdout
    /// cannot be written.
    #[track_caller]
    pub fn emit_plan(mut self) {
        assert!(self.plan_only, "emit_plan requires plan-only process mode");
        self.sort_plan();
        let mut encoded = Vec::new();
        crate::jsonl::write_record(&mut encoded, &self.run_record());
        write_stdout(&encoded);
    }

    /// Runs the final owner oracle, emits a complete artifact, and prints its
    /// derived human report.
    ///
    /// # Panics
    ///
    /// Panics when configuration or records are incomplete, the final oracle
    /// panics, or stdout cannot be written.
    #[track_caller]
    pub fn finish(mut self, final_oracle: impl FnOnce()) -> Artifact {
        assert!(
            !self.plan_only,
            "plan-only process must call emit_plan instead of finish"
        );
        final_oracle();
        self.sort_plan();
        for case in &self.cases {
            assert_eq!(
                case.samples.len(),
                case.spec.samples().get(),
                "benchmark case {:?} is incomplete",
                case.spec.series()
            );
        }
        for observation in &self.observations {
            assert_eq!(
                observation.values.len(),
                observation.spec.samples().get(),
                "benchmark observation {:?} is incomplete",
                observation.spec.series()
            );
        }

        let mut encoded = Vec::new();
        crate::jsonl::write_record(&mut encoded, &self.run_record());
        for (case, run) in self.cases.into_iter().enumerate() {
            for (sample, measurement) in run.samples.into_iter().enumerate() {
                crate::jsonl::write_record(
                    &mut encoded,
                    &Record::Sample {
                        case,
                        sample,
                        elapsed_ns: u64::try_from(measurement.elapsed.as_nanos())
                            .expect("benchmark sample duration fits u64 nanoseconds"),
                        fields: measurement.fields,
                    },
                );
            }
        }
        for (observation, run) in self.observations.into_iter().enumerate() {
            for (sample, fields) in run.values.into_iter().enumerate() {
                crate::jsonl::write_record(
                    &mut encoded,
                    &Record::Observation {
                        observation,
                        sample,
                        fields,
                    },
                );
            }
        }
        crate::jsonl::write_record(&mut encoded, &Record::Completion {});
        let output = std::str::from_utf8(&encoded).expect("benchmark JSONL is Unicode");
        let artifact = RunValidator::validate(self.benchmark, self.profile.as_str(), output)
            .unwrap_or_else(|error| panic!("validate emitted benchmark artifact: {error}"));
        write_stdout(&encoded);
        report::print(&artifact);
        artifact
    }

    fn from_plan(benchmark: &'static str, plan: Plan, root: Option<RunRoot>) -> Self {
        let Plan {
            profile,
            configuration,
            cases,
            observations,
        } = plan;
        let host = HostEnvironment::collect(root.as_ref().map(RunRoot::filesystem_root));
        Self {
            benchmark,
            profile,
            root,
            host,
            configuration,
            cases: cases
                .into_iter()
                .map(|spec| CaseRun {
                    samples: Vec::with_capacity(spec.samples().get()),
                    spec,
                })
                .collect(),
            observations: observations
                .into_iter()
                .map(|spec| ObservationRun {
                    values: Vec::with_capacity(spec.samples().get()),
                    spec,
                })
                .collect(),
            plan_only: plan_only(),
        }
    }

    fn case(&self, id: CaseId) -> &CaseRun {
        self.cases.get(id.0).expect("unknown benchmark case")
    }

    fn assert_pair(&self, first: CaseId, second: CaseId) {
        let first = self.case(first).spec.pairing();
        let second = self.case(second).spec.pairing();
        assert!(
            matches!((first, second), (Some(first), Some(second))
                if first.pair() == second.pair()
                    && first.side() == PairSide::First
                    && second.side() == PairSide::Second),
            "paired measurement IDs must name first and second sides of one frozen pair"
        );
    }

    fn sort_plan(&mut self) {
        self.cases
            .sort_by(|left, right| left.spec.series().cmp(right.spec.series()));
        self.observations
            .sort_by(|left, right| left.spec.series().cmp(right.spec.series()));
    }

    fn run_record(&self) -> Record {
        Record::Run {
            protocol: PROTOCOL_VERSION,
            benchmark: self.benchmark.to_owned(),
            profile: self.profile,
            host: Box::new(self.host.clone()),
            configuration: self.configuration.clone(),
            cases: self.cases.iter().map(|case| case.spec.clone()).collect(),
            observations: self
                .observations
                .iter()
                .map(|observation| observation.spec.clone())
                .collect(),
        }
    }
}

fn plan_only() -> bool {
    match std::env::var_os(BENCHMARK_PLAN_ONLY_ENV) {
        None => false,
        Some(value) if value == "1" => true,
        Some(value) => panic!(
            "{BENCHMARK_PLAN_ONLY_ENV} must be exactly 1 when set, found {:?}",
            value.to_string_lossy()
        ),
    }
}

fn write_stdout(encoded: &[u8]) {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout
        .write_all(encoded)
        .unwrap_or_else(|error| panic!("write benchmark artifact: {error}"));
    stdout
        .flush()
        .unwrap_or_else(|error| panic!("flush benchmark artifact: {error}"));
}
