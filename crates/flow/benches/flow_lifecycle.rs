use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    time::Duration,
};

use dogpaddle_bench_protocol::{BenchmarkProfile, CaseSpec, Fields, Measurement, Plan, Run};
use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition,
    transform::RunningEventCountDefinition,
};

const BENCHMARK: &str = "flow_lifecycle";
const SMOKE_STATION_COUNTS: &[usize] = &[2, 3];
const REFERENCE_STATION_COUNTS: &[usize] = &[2, 64, 1_024];
const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();

struct Config {
    station_counts: Vec<usize>,
    samples: usize,
    warmups: usize,
}

impl Config {
    fn for_profile(profile: dogpaddle_bench_protocol::BenchmarkProfile) -> Self {
        let (station_counts, samples, warmups) = match profile {
            dogpaddle_bench_protocol::BenchmarkProfile::Smoke => (SMOKE_STATION_COUNTS, 1, 1),
            dogpaddle_bench_protocol::BenchmarkProfile::Reference => {
                (REFERENCE_STATION_COUNTS, 9, 2)
            }
        };
        assert!(station_counts.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(station_counts.iter().all(|count| *count >= 2));
        Self {
            station_counts: station_counts.to_vec(),
            samples,
            warmups,
        }
    }

    fn fields(&self) -> Fields {
        Fields::new()
            .with("station_counts", &self.station_counts)
            .with("samples", self.samples)
            .with("warmups", self.warmups)
            .with("scope", "build_open_runtime_excluded")
            .with("validation", "outside_timing")
            .with("mdbx_sync_mode", "durable")
    }
}

fn main() {
    let profile = BenchmarkProfile::from_environment();
    let config = Config::for_profile(profile);
    let mut plan = Plan::new(profile, config.fields());
    let cases = config
        .station_counts
        .iter()
        .map(|&station_count| {
            (
                station_count,
                plan.case(case("fresh_durable_build", station_count, config.samples)),
                plan.case(case("warm_reopen", station_count, config.samples)),
            )
        })
        .collect::<Vec<_>>();
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    for (station_count, build, reopen) in cases {
        for _ in 0..config.warmups {
            measure_fresh_build(&run, station_count);
        }
        run.samples(build, |run| {
            Measurement::new(measure_fresh_build(run, station_count))
        });

        let fixture = run.sample("flow-reopen");
        let path = fixture.path().join("flow");
        let flow = linear_factory(&path, station_count)
            .build()
            .expect("build reopen benchmark fixture");
        validate_flow(&flow, &path, station_count);
        drop(flow);
        for _ in 0..=config.warmups {
            measure_reopen(&path, station_count);
        }
        run.samples(reopen, |_| {
            Measurement::new(measure_reopen(&path, station_count))
        });
    }
    run.finish(|| {});
}

fn case(scenario: &str, station_count: usize, samples: usize) -> CaseSpec {
    CaseSpec::new(
        format!("{scenario}/stations={station_count}"),
        NonZeroUsize::new(samples).unwrap(),
        Fields::new()
            .with("station_count", station_count)
            .with("operations", 1_usize),
    )
}

fn measure_fresh_build(run: &Run, station_count: usize) -> Duration {
    let sample = run.sample("flow-build");
    let path = sample.path().join("flow");
    let factory = linear_factory(&path, station_count);
    let started = std::time::Instant::now();
    let flow = factory.build().expect("build benchmark Flow");
    let elapsed = started.elapsed();
    validate_flow(&flow, &path, station_count);
    drop(flow);
    let reopened = FlowFactory::open(&path).expect("reopen freshly built benchmark Flow");
    validate_flow(&reopened, &path, station_count);
    elapsed
}

fn measure_reopen(path: &Path, station_count: usize) -> Duration {
    let started = std::time::Instant::now();
    let flow = FlowFactory::open(path).expect("open benchmark Flow");
    let elapsed = started.elapsed();
    validate_flow(&flow, path, station_count);
    elapsed
}

fn linear_factory(path: &Path, station_count: usize) -> FlowFactory {
    let mut factory = FlowFactory::new(path);
    let mut previous = factory.station("source", SequenceSourceDefinition::new(0));
    factory.output_capacity_bytes(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..station_count - 1 {
        let current = factory.station(
            format!("count-{index:08x}"),
            RunningEventCountDefinition::new(),
        );
        factory.output_capacity_bytes(current, OUTPUT_CAPACITY_BYTES);
        factory.connect([previous], current);
        previous = current;
    }
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.connect([previous], sink);
    factory
}

fn validate_flow(flow: &Flow, path: &Path, station_count: usize) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), station_count);
    let mut ids = flow.station_ids();
    assert_eq!(ids.next(), Some("source"));
    for index in 1..station_count - 1 {
        let expected = format!("count-{index:08x}");
        assert_eq!(ids.next(), Some(expected.as_str()));
    }
    assert_eq!(ids.next(), Some("sink"));
    assert_eq!(ids.next(), None);
}
