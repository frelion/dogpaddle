use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{OrderedMap, Small, Store};

use crate::build::FlowFactory;

#[test]
fn build_and_open_derive_a_stable_layered_topological_schedule() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_target = builder.station("first-target", CountDefinition::new());
    let second_target = builder.station("second-target", CountDefinition::new());
    let second_source = builder.station("second-source", SequenceSourceDefinition::new(0));
    let first_source = builder.station("first-source", SequenceSourceDefinition::new(0));
    builder.connect([first_source], first_target);
    builder.connect([second_source], second_target);

    let flow = builder.build().unwrap();
    assert_eq!(flow.schedule, [2, 3, 0, 1]);
    drop(flow);

    let reopened = FlowFactory::open(path).unwrap();
    assert_eq!(reopened.schedule, [2, 3, 0, 1]);
}

#[test]
fn open_rematerializes_state_under_the_flow_owned_transaction_capability() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    builder.station("source", SequenceSourceDefinition::new(0));
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
    assert_eq!(flow.stations.len(), 1);
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
