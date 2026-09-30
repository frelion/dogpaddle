use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::{
    col,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{RunningEventCountDefinition, SelectDefinition, UnionAllDefinition},
    },
};
use dogpaddle_store::{Cell, Store};
use std::num::NonZeroU32;

#[test]
fn fusion_preserves_every_logical_identity_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let project = factory.operation(
        "project",
        SelectDefinition::try_new([("value", col("value"))]).unwrap(),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [project]);
    let flow = factory.build().unwrap();
    assert_eq!(
        flow.operation_ids().collect::<Vec<_>>(),
        ["scan", "project", "sink"]
    );
    drop(flow);
    let flow = FlowFactory::new(path).open().unwrap();
    assert_eq!(
        flow.operation_ids().collect::<Vec<_>>(),
        ["scan", "project", "sink"]
    );
}
#[test]
fn repeated_producer_ports_call_each_input_once() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(u64::MAX), []);
    let union = factory.operation(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
        [scan, scan],
    );
    let count = factory.operation("count", RunningEventCountDefinition::new(), [union]);
    factory.operation("sink", DiscardDefinition::new(), [count]);
    let mut flow = factory.build().unwrap();
    flow.advance().unwrap();
    drop(flow);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    super::support::run_until_idle(&mut flow);
    drop(flow);
    let store = Store::open(path).unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
        .unwrap();
    assert_eq!(
        count
            .read(store.read_transaction().access())
            .unwrap()
            .get()
            .unwrap(),
        Some(2)
    );
}
#[test]
fn unexpected_resource_on_fused_tail_is_rejected_before_creating_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let project = factory.operation(
        "project",
        SelectDefinition::try_new([("value", col("value"))]).unwrap(),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [project]);
    factory.resource("project", 42_u64).unwrap();
    assert!(
        matches!(factory.build(),Err(FlowError::RuntimeResource{operation_id,..}) if operation_id=="project")
    );
    assert!(!path.exists());
}
