use std::{ops::Range, path::Path};

use dogpaddle_flow::{AdvanceOutcome, Flow, FlowError, FlowFactory, FlowRunError};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, Store};

#[test]
fn advance_exposes_one_bounded_scheduling_round() {
    let _: fn(&mut Flow) -> Result<AdvanceOutcome, FlowRunError> = Flow::advance;
    assert_ne!(AdvanceOutcome::Idle, AdvanceOutcome::Progressed);
}

#[test]
fn advance_runs_one_real_topological_round_and_reopens_at_the_next_source_position() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);
    let mut flow = builder.build().unwrap();

    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);
    let first = execution_state(&path);

    let mut reopened = FlowFactory::open(&path).unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(reopened);
    let second = execution_state(&path);

    assert_eq!(first.source_output, 1..1);
    assert_eq!(first.count_output, 0..1);
    assert_eq!(second.source_output, 2..2);
    assert_eq!(second.count_output, 0..2);
    assert!(second.source_position > first.source_position);
    assert!(second.count > first.count);
    assert_eq!(first.count, first.source_position + 1);
    assert_eq!(second.count, second.source_position + 1);
}

struct ExecutionState {
    source_position: u64,
    count: u64,
    source_output: Range<u64>,
    count_output: Range<u64>,
}

fn execution_state(path: &Path) -> ExecutionState {
    let store = Store::open(path).unwrap();
    let source_position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let count: Cell<u64> = store.open_data("station/00000001/operation/count").unwrap();
    let source_output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let count_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    ExecutionState {
        source_position: source_position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap(),
        count: count
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap(),
        source_output: source_output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        count_output: count_output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
    }
}

#[test]
fn build_freezes_and_open_rematerializes_a_real_flow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(100));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);

    let flow = builder.build().unwrap();
    assert_eq!(flow.path(), path);
    assert_eq!(flow.station_count(), 2);
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.station_count(), 2);
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["source", "count"]);
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    builder.station("source", SequenceSourceDefinition::new(0));
    let flow = builder.build().unwrap();

    assert!(matches!(FlowFactory::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    assert!(FlowFactory::open(&path).is_ok());
}

#[test]
fn build_and_open_support_many_station_output_logs() {
    const STATION_COUNT: usize = 65;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let mut previous = builder.station("source", SequenceSourceDefinition::new(0));
    for index in 1..STATION_COUNT {
        let current = builder.station(format!("count-{index}"), CountDefinition::new());
        builder.connect([previous], current);
        previous = current;
    }

    let flow = builder.build().unwrap();
    assert_eq!(flow.station_count(), STATION_COUNT);
    drop(flow);

    let reopened = FlowFactory::open(path).unwrap();
    assert_eq!(reopened.station_count(), STATION_COUNT);
}
