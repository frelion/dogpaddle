use dogpaddle_flow::{Flow, FlowError};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};

#[test]
fn build_freezes_and_open_rematerializes_a_real_flow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    let source = builder.stage("source", SequenceSourceDefinition::new(100));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([source], count);

    let flow = builder.build().unwrap();
    assert_eq!(flow.path(), path);
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let flow = Flow::open(&path).unwrap();
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
    let flow = builder.build().unwrap();

    assert!(matches!(Flow::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    assert!(Flow::open(&path).is_ok());
}
