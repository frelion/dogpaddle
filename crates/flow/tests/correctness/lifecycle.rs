use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};

#[test]
fn build_freezes_and_open_rematerializes_a_real_flow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.stage("source", SequenceSourceDefinition::new(100));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([source], count);

    let flow = builder.build().unwrap();
    assert_eq!(flow.path(), path);
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
    let flow = builder.build().unwrap();

    assert!(matches!(FlowFactory::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    assert!(FlowFactory::open(&path).is_ok());
}

#[test]
fn build_and_open_support_many_stage_output_logs() {
    const STAGE_COUNT: usize = 65;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let mut previous = builder.stage("source", SequenceSourceDefinition::new(0));
    for index in 1..STAGE_COUNT {
        let current = builder.stage(format!("count-{index}"), CountDefinition::new());
        builder.connect([previous], current);
        previous = current;
    }

    let flow = builder.build().unwrap();
    assert_eq!(flow.stage_count(), STAGE_COUNT);
    drop(flow);

    let reopened = FlowFactory::open(path).unwrap();
    assert_eq!(reopened.stage_count(), STAGE_COUNT);
}
