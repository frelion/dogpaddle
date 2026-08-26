mod support;

use std::{path::Path, time::Duration};

use dogpaddle_flow::{Flow, FlowBuilder};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};

use support::{
    BenchRoot, emit_configuration, emit_environment, emit_sample, report, setting, setting_list,
};

const SMOKE_STAGE_COUNTS: &[usize] = &[1, 8, 64];
const REFERENCE_STAGE_COUNTS: &[usize] = &[1, 64, 1_024];
const SMOKE_SAMPLES: usize = 3;
const REFERENCE_SAMPLES: usize = 9;
const SMOKE_WARMUPS: usize = 1;
const REFERENCE_WARMUPS: usize = 2;

struct Config {
    stage_counts: Vec<usize>,
    samples: usize,
    warmups: usize,
}

impl Config {
    fn load(profile: &str) -> Self {
        let (default_stage_counts, default_samples, default_warmups) = match profile {
            "smoke" => (SMOKE_STAGE_COUNTS, SMOKE_SAMPLES, SMOKE_WARMUPS),
            "reference" => (REFERENCE_STAGE_COUNTS, REFERENCE_SAMPLES, REFERENCE_WARMUPS),
            _ => unreachable!("BenchRoot validates the benchmark profile"),
        };
        let stage_counts = setting_list("DOGPADDLE_FLOW_BENCH_STAGE_COUNTS", default_stage_counts);
        let samples = setting("DOGPADDLE_FLOW_BENCH_SAMPLES", default_samples);
        let warmups = setting("DOGPADDLE_FLOW_BENCH_WARMUPS", default_warmups);
        assert!(
            stage_counts.windows(2).all(|pair| pair[0] < pair[1]),
            "DOGPADDLE_FLOW_BENCH_STAGE_COUNTS must be strictly increasing"
        );
        Self {
            stage_counts,
            samples,
            warmups,
        }
    }
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!("flow_lifecycle must run through `cargo bench`");
        return;
    }

    let root = BenchRoot::from_environment();
    let config = Config::load(root.profile());
    println!("DogPaddle Flow lifecycle benchmark");
    println!(
        "scope=build/open runtime=absent sync=durable execution=single-thread validation=outside-timing"
    );
    emit_environment(&root);
    emit_configuration(&config.stage_counts, config.samples, config.warmups);

    for &stage_count in &config.stage_counts {
        benchmark_fresh_build(&root, &config, stage_count);
        benchmark_warm_reopen(&root, &config, stage_count);
    }
}

fn benchmark_fresh_build(root: &BenchRoot, config: &Config, stage_count: usize) {
    for _ in 0..config.warmups {
        measure_fresh_build(root, stage_count);
    }

    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = measure_fresh_build(root, stage_count);
        emit_sample("fresh_durable_build", stage_count, sample, elapsed);
        durations.push(elapsed);
    }
    report("fresh_durable_build", stage_count, &mut durations);
}

fn measure_fresh_build(root: &BenchRoot, stage_count: usize) -> Duration {
    let sample = root.sample("flow-build");
    let builder = linear_builder(sample.path(), stage_count);

    let started = std::time::Instant::now();
    let flow = builder.build().expect("build benchmark Flow");
    let elapsed = started.elapsed();

    validate_flow(&flow, sample.path(), stage_count);
    drop(flow);
    let reopened = Flow::open(sample.path()).expect("reopen freshly built benchmark Flow");
    validate_flow(&reopened, sample.path(), stage_count);
    drop(reopened);
    drop(sample);
    elapsed
}

fn benchmark_warm_reopen(root: &BenchRoot, config: &Config, stage_count: usize) {
    let fixture = root.sample("flow-reopen");
    let flow = linear_builder(fixture.path(), stage_count)
        .build()
        .expect("build reopen benchmark fixture");
    validate_flow(&flow, fixture.path(), stage_count);
    drop(flow);

    let preflight = Flow::open(fixture.path()).expect("preflight reopen benchmark fixture");
    validate_flow(&preflight, fixture.path(), stage_count);
    drop(preflight);
    for _ in 0..config.warmups {
        measure_reopen(fixture.path(), stage_count);
    }

    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = measure_reopen(fixture.path(), stage_count);
        emit_sample("warm_reopen", stage_count, sample, elapsed);
        durations.push(elapsed);
    }
    report("warm_reopen", stage_count, &mut durations);
}

fn measure_reopen(path: &Path, stage_count: usize) -> Duration {
    let started = std::time::Instant::now();
    let flow = Flow::open(path).expect("open benchmark Flow");
    let elapsed = started.elapsed();

    validate_flow(&flow, path, stage_count);
    drop(flow);
    elapsed
}

fn linear_builder(path: &Path, stage_count: usize) -> FlowBuilder {
    assert!(stage_count > 0, "benchmark Flow must contain a stage");
    let mut builder = Flow::builder(path);
    let mut previous = builder.stage("source", SequenceSourceDefinition::new(0));
    for index in 1..stage_count {
        let current = builder.stage(format!("count-{index:08x}"), CountDefinition::new());
        builder.connect([previous], current);
        previous = current;
    }
    builder
}

fn validate_flow(flow: &Flow, path: &Path, stage_count: usize) {
    assert_eq!(flow.path(), path);
    assert_eq!(flow.stage_count(), stage_count);
    let mut ids = flow.stage_ids();
    assert_eq!(ids.next(), Some("source"));
    for index in 1..stage_count {
        let expected = format!("count-{index:08x}");
        assert_eq!(ids.next(), Some(expected.as_str()));
    }
    assert_eq!(ids.next(), None);
}
