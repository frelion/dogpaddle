use dogpaddle_operation::operation::source::SequenceSourceDefinition;

use crate::build::FlowFactory;

#[test]
fn open_restores_the_persisted_flow_definition() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    factory.stage("source", SequenceSourceDefinition::new(0));
    drop(factory.build().unwrap());

    let flow = FlowFactory::open(&path).unwrap();

    assert_eq!(flow.path(), path);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source"]);
}
