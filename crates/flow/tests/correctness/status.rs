use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::SqliteSinkDefinition,
    transform::RunningEventCountDefinition,
};

#[test]
fn status_is_read_only_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("sink.sqlite");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(u64::MAX - 3), []);
    let count = factory.operation("count", RunningEventCountDefinition::new(), [scan]);
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, "events").unwrap(),
        [count],
    );
    let mut flow = factory.build().unwrap();
    let initial = flow.status().unwrap();
    assert_eq!(initial.depth, 0);
    assert_eq!(initial.active_operation, None);
    assert!(!initial.needs_reopen);
    assert_eq!(flow.status().unwrap(), initial);
    assert!(!target.exists());
    flow.advance().unwrap();
    let current = flow.status().unwrap();
    assert_eq!(flow.status().unwrap(), current);
    drop(flow);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.status().unwrap(), current);
    super::support::run_until_idle(&mut flow);
    assert_eq!(flow.status().unwrap().depth, 0);
}
