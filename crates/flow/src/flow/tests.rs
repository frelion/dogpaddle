use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{OrderedMap, Small, Store};

use super::Flow;

#[test]
fn open_rematerializes_flow_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
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

    let mut flow = Flow::open(&path).unwrap();
    assert_eq!(flow.stages.len(), 1);
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
fn build_and_open_inject_the_later_declared_source_output_as_read_only_input() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    let count = builder.stage("count", CountDefinition::new());
    let source = builder.stage("source", SequenceSourceDefinition::new(0));
    builder.connect([source], count);

    let mut flow = builder.build().unwrap();
    assert_stage_wiring(&mut flow);
    drop(flow);

    let mut reopened = Flow::open(&path).unwrap();
    assert_stage_wiring(&mut reopened);
}

fn assert_stage_wiring(flow: &mut Flow) {
    assert_eq!(flow.stages.len(), 2);
    assert_eq!(flow.stages[0].inputs.len(), 1);
    assert!(flow.stages[0].output.is_some());
    assert!(flow.stages[1].inputs.is_empty());
    assert!(flow.stages[1].output.is_some());

    {
        let source = &mut flow.stages[1];
        let transaction = source.transactions.begin().unwrap();
        let mut output = source
            .output
            .as_ref()
            .expect("source produces output")
            .access(transaction.access())
            .unwrap();
        if output.bounds().unwrap().is_empty() {
            output.append(&b"change".to_vec()).unwrap();
        }
        transaction.commit().unwrap();
    }

    {
        let count = &mut flow.stages[0];
        let transaction = count.transactions.begin().unwrap();
        assert_eq!(
            count.inputs[0]
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
            0..1
        );
    }
}
