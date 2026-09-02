use std::{hint::black_box, num::NonZeroUsize, time::Duration};

use dogpaddle_bench_protocol::{
    BenchmarkProfile, CaseId, CaseSpec, Fields, Measurement, ObservationId, ObservationSpec, Plan,
    Run,
};

use super::fixture::{DEFAULT_WORKLOADS, Fixture, validate_dimensions};

const SMOKE_ROWS: &[usize] = &[4];
const REFERENCE_ROWS: &[usize] = &[1, 64, 1_024, 16_384];

pub(crate) struct Config {
    pub(crate) rows: Vec<usize>,
    pub(crate) payload_bytes: usize,
    pub(crate) samples: usize,
    pub(crate) target_rows: usize,
    pub(crate) max_changes: usize,
    pub(crate) workloads: Vec<String>,
}

#[derive(Clone, Copy)]
pub(crate) struct Timed {
    pub(crate) elapsed: Duration,
    pub(crate) checksum: u64,
}

pub(crate) struct FixturePlan {
    workload: &'static str,
    rows: usize,
    encoded_bytes: ObservationId,
    cases: Vec<(&'static str, CaseId)>,
}

impl Config {
    pub(crate) fn load(profile: BenchmarkProfile) -> Self {
        let (rows, payload_bytes, samples, target_rows, max_changes) = match profile {
            BenchmarkProfile::Smoke => (SMOKE_ROWS, 16, 1, 4, 1),
            BenchmarkProfile::Reference => (REFERENCE_ROWS, 1_024, 9, 65_536, 1_024),
        };
        let rows = rows.to_vec();
        let workloads = DEFAULT_WORKLOADS
            .iter()
            .map(|workload| (*workload).to_owned())
            .collect::<Vec<_>>();
        for &rows in &rows {
            validate_dimensions(rows, payload_bytes, &workloads);
        }
        Self {
            rows,
            payload_bytes,
            samples,
            target_rows,
            max_changes,
            workloads,
        }
    }

    pub(crate) fn iterations(&self, rows: usize) -> usize {
        self.target_rows.div_ceil(rows).clamp(1, self.max_changes)
    }

    pub(crate) fn fields(&self) -> Fields {
        Fields::new()
            .with("rows_per_change", &self.rows)
            .with("target_rows_per_sample", self.target_rows)
            .with("max_changes_per_sample", self.max_changes)
            .with("samples", self.samples)
            .with("payload_bytes", self.payload_bytes)
            .with("workloads", &self.workloads)
            .with("execution", "single_thread")
            .with("cache", "warm")
            .with("validation", "outside_timing")
    }
}

impl FixturePlan {
    pub(crate) fn case(&self, scenario: &str) -> CaseId {
        self.cases
            .iter()
            .find_map(|(name, id)| (*name == scenario).then_some(*id))
            .unwrap_or_else(|| panic!("unknown Change benchmark scenario {scenario:?}"))
    }

    pub(crate) fn observe(&self, run: &mut Run, fixture: &Fixture, encoded_bytes: usize) {
        assert_eq!(fixture.name, self.workload);
        assert_eq!(fixture.change.num_rows(), self.rows);
        run.observe(
            self.encoded_bytes,
            Fields::new().with("encoded_bytes_per_change", encoded_bytes),
        );
    }
}

pub(crate) fn plan_fixtures(
    plan: &mut Plan,
    config: &Config,
    scenarios: &[&'static str],
) -> Vec<FixturePlan> {
    let mut fixtures = Vec::new();
    for &rows in &config.rows {
        let operations = config.iterations(rows);
        for &workload in DEFAULT_WORKLOADS {
            let encoded_bytes = plan.observation(ObservationSpec::new(
                format!("{workload}/fixture/rows={rows}"),
                NonZeroUsize::MIN,
            ));
            let cases = scenarios
                .iter()
                .map(|&scenario| {
                    let case = plan.case(CaseSpec::new(
                        format!("{workload}/{scenario}/rows={rows}/operations={operations}"),
                        NonZeroUsize::new(config.samples).expect("Change benchmark has samples"),
                        Fields::new()
                            .with("workload", workload)
                            .with("operations", operations)
                            .with("rows_per_change", rows),
                    ));
                    (scenario, case)
                })
                .collect();
            fixtures.push(FixturePlan {
                workload,
                rows,
                encoded_bytes,
                cases,
            });
        }
    }
    fixtures
}

pub(crate) fn record(run: &mut Run, id: CaseId, measurements: &[Timed]) {
    for measurement in measurements {
        run.push(id, Measurement::new(measurement.elapsed));
    }
}

pub(crate) fn timed(iterations: usize, mut operation: impl FnMut() -> u64) -> Timed {
    let mut checksum = 0_u64;
    let started = std::time::Instant::now();
    for _ in 0..iterations {
        checksum = checksum.wrapping_add(operation());
    }
    black_box(checksum);
    Timed {
        elapsed: started.elapsed(),
        checksum,
    }
}
