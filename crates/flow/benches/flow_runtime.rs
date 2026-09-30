//! Per-round latency for the durable call stack. Fixture and oracle are untimed.
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{
            FilterDefinition, RunningEventCountDefinition, SchemaAlignDefinition, SchemaAlignField,
            SelectDefinition,
        },
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Cell, Queue, Store};
use serde_json::json;
use std::{path::Path, time::Instant};

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Sink,
    PureChain,
    CountChain(usize),
    Fanout(usize),
}
fn main() {
    let profile = PerformanceProfile::for_benchmark();
    if std::env::args_os().any(|arg| arg == "--bench") {
        require_release_build("flow_runtime");
    }
    let root = RunRoot::for_profile("flow_runtime", profile);
    let (warmup, rounds, samples, scenarios) = match profile {
        PerformanceProfile::Smoke => (
            4,
            3,
            1,
            vec![
                Scenario::Sink,
                Scenario::PureChain,
                Scenario::CountChain(1),
                Scenario::Fanout(2),
            ],
        ),
        PerformanceProfile::Reference => (
            64,
            1024,
            9,
            vec![
                Scenario::Sink,
                Scenario::PureChain,
                Scenario::CountChain(1),
                Scenario::CountChain(14),
                Scenario::CountChain(62),
                Scenario::Fanout(4),
                Scenario::Fanout(16),
            ],
        ),
    };
    println!(
        "{}",
        json!({"record":"context","benchmark":"flow_runtime","protocol":"durable_call_stack_v1","profile":profile,"host":HostEnvironment::collect(Some(root.filesystem_root())),"configuration":{"warmup_rounds":warmup,"rounds_per_sample":rounds,"samples":samples,"timing_scope":"one bounded advance; final backlog drain and queue, frame, counter oracles are untimed","fixture_and_oracle":"outside timing","comparison":"compare matching logical workloads and completed source events; advance latency alone is not throughput when backlog remains"}})
    );
    for scenario in scenarios {
        let fixture = root.sample(&format!("{scenario:?}"));
        let path = fixture.path().join("flow");
        let mut flow = build(&path, scenario);
        let mut completed = 0;
        for _ in 0..warmup {
            advance(&mut flow);
            completed += 1;
        }
        for sample in 0..samples {
            for round in 0..rounds {
                let started = Instant::now();
                advance(&mut flow);
                let elapsed = started.elapsed();
                completed += 1;
                println!(
                    "{}",
                    json!({"record":"advance","benchmark":"flow_runtime","profile":profile,"series":format!("{scenario:?}"),"sample":sample,"round":round,"elapsed_ns":elapsed.as_nanos(),"captured_source_events":completed})
                );
            }
        }
        let depth_before_drain = flow.status().unwrap().depth;
        drop(flow);
        let caught_up_before_drain = depth_before_drain == 0
            && verify_when_drained(&path, scenario, completed, completed - 1);
        if profile == PerformanceProfile::Smoke {
            assert!(caught_up_before_drain);
        }
        stop_source(&path, completed);
        let mut flow = FlowFactory::new(&path).open().unwrap();
        for _ in 0..completed * 64 + 64 {
            if flow.advance().unwrap() == AdvanceOutcome::Idle {
                break;
            }
        }
        assert_eq!(flow.status().unwrap().depth, 0);
        drop(flow);
        assert!(verify_when_drained(&path, scenario, completed, u64::MAX));
        let reopened = FlowFactory::new(&path).open().unwrap();
        assert_eq!(reopened.status().unwrap().depth, 0);
        println!(
            "{}",
            json!({"record":"oracle","benchmark":"flow_runtime","series":format!("{scenario:?}"),"passed":true,"captured_source_events":completed,"depth_before_drain":depth_before_drain,"caught_up_before_drain":caught_up_before_drain,"reopen_depth":0})
        );
    }
    println!(
        "{}",
        json!({"record":"completion","benchmark":"flow_runtime","profile":profile})
    );
}
fn build(path: &Path, scenario: Scenario) -> Flow {
    let mut factory = FlowFactory::new(path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let mut tail = scan;
    match scenario {
        Scenario::PureChain => {
            tail = factory.operation(
                "project",
                SelectDefinition::try_new([("value", col("value"))]).unwrap(),
                [tail],
            );
            tail = factory.operation(
                "extend",
                SelectDefinition::try_new([
                    ("value", col("value")),
                    ("next", col("value") + lit(1_u64)),
                ])
                .unwrap(),
                [tail],
            );
            tail = factory.operation(
                "filter",
                FilterDefinition::try_new(col("next").gt(lit(0_u64))).unwrap(),
                [tail],
            );
            tail = factory.operation(
                "select",
                SelectDefinition::try_new([("value", col("value")), ("next", col("next"))])
                    .unwrap(),
                [tail],
            );
            tail = factory.operation(
                "align",
                SchemaAlignDefinition::try_new([
                    SchemaAlignField::try_new("source_value", col("value"), false).unwrap(),
                    SchemaAlignField::try_new("derived_value", col("next"), false).unwrap(),
                ])
                .unwrap(),
                [tail],
            );
        }
        Scenario::CountChain(count) => {
            for index in 0..count {
                tail = factory.operation(
                    format!("count-{index}"),
                    RunningEventCountDefinition::new(),
                    [tail],
                );
            }
        }
        Scenario::Sink | Scenario::Fanout(_) => {}
    }
    let fanout = match scenario {
        Scenario::Fanout(count) => count,
        _ => 1,
    };
    for index in 0..fanout {
        factory.operation(format!("sink-{index}"), DiscardDefinition::new(), [tail]);
    }
    factory.build().unwrap()
}
fn advance(flow: &mut Flow) {
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
}
fn stop_source(path: &Path, captured: u64) {
    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(captured - 1)
    );
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    transaction.commit().unwrap();
}
fn verify_when_drained(
    path: &Path,
    scenario: Scenario,
    completed: u64,
    source_position: u64,
) -> bool {
    let store = Store::open(path).unwrap();
    let read = store.read_transaction();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    assert_eq!(
        position.read(read.access()).unwrap().get().unwrap(),
        Some(source_position)
    );
    let published: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    if !published.read(read.access()).unwrap().is_empty().unwrap() {
        return false;
    }
    if let Scenario::CountChain(count) = scenario {
        for index in 1..=count {
            let counter: Cell<u64> = store
                .open_data(&format!("operation/{index:08x}/running_event_count.count"))
                .unwrap();
            assert_eq!(
                counter.read(read.access()).unwrap().get().unwrap(),
                Some(completed)
            );
        }
    }
    true
}
