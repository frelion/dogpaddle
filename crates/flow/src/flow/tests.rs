use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, OrderedMap, Small, Store};

use crate::{build::FlowFactory, station::ProcessOutcome};

#[test]
fn build_and_open_derive_a_stable_layered_topological_schedule() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_target = builder.station("first-target", CountDefinition::new());
    let second_target = builder.station("second-target", CountDefinition::new());
    let second_source = builder.station("second-source", SequenceSourceDefinition::new(0));
    let first_source = builder.station("first-source", SequenceSourceDefinition::new(0));
    let first_sink = builder.station("first-sink", DiscardDefinition::new());
    let second_sink = builder.station("second-sink", DiscardDefinition::new());
    builder.connect([first_source], first_target);
    builder.connect([second_source], second_target);
    builder.connect([first_target], first_sink);
    builder.connect([second_target], second_sink);

    let flow = builder.build().unwrap();
    assert_eq!(flow.topology.schedule, [2, 3, 0, 1, 4, 5]);
    assert_eq!(
        flow.topology.gc_upstreams,
        [vec![3], vec![2], vec![], vec![], vec![0], vec![1]]
    );
    drop(flow);

    let reopened = FlowFactory::open(path).unwrap();
    assert_eq!(reopened.topology.schedule, [2, 3, 0, 1, 4, 5]);
    assert_eq!(
        reopened.topology.gc_upstreams,
        [vec![3], vec![2], vec![], vec![], vec![0], vec![1]]
    );
}

#[test]
fn open_rematerializes_state_under_the_flow_owned_transaction_capability() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    drop(builder.build().unwrap());

    let store = Store::open(&path).unwrap();
    let state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.open_data("flow/state").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        state
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"flow".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.stations.len(), 2);
    let transaction = flow.transactions.begin().unwrap();
    assert_eq!(
        flow.state
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"flow".to_vec())
    );
}

#[test]
fn advance_runs_upstream_gc_after_an_idle_station_turn() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], count);
    builder.connect([count], sink);
    let mut flow = builder.build().unwrap();

    assert_eq!(
        flow.stations[0]
            .advance(&flow.reads, &mut flow.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );
    assert_eq!(
        flow.stations[1]
            .advance(&flow.reads, &mut flow.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );

    flow.topology.schedule = vec![1];
    assert_eq!(flow.advance().unwrap(), super::AdvanceOutcome::Idle);
    drop(flow);

    let store = Store::open(path).unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        1..1
    );
}
