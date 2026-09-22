use dogpaddle_operation::operation::transform::SelectDefinition;
use std::num::{NonZeroU32, NonZeroU64};

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::UnionAllDefinition,
};

#[test]
fn default_fusion_and_capacity_survive_open_without_replanning() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    let project = factory.operation(
        "project",
        Box::new(
            SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        ),
        [scan],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [project]);
    let flow = factory.build().unwrap();
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["scan", "sink"]);
    assert_eq!(
        flow.status().unwrap()[0]
            .output
            .as_ref()
            .unwrap()
            .capacity_bytes,
        64 * 1024 * 1024
    );
    drop(flow);
    let mut opener = FlowFactory::new(&path);
    opener.output_capacity_bytes(NonZeroU64::MIN);
    let flow = opener.open().unwrap();
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["scan", "sink"]);
    assert_eq!(
        flow.status().unwrap()[0]
            .output
            .as_ref()
            .unwrap()
            .capacity_bytes,
        64 * 1024 * 1024
    );
}

#[test]
fn repeated_producer_ports_have_independent_subscriptions() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(u64::MAX)), []);
    let union = factory.operation(
        "union",
        Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        [scan, scan],
    );
    let project = factory.operation(
        "project",
        Box::new(
            SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        ),
        [union],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [project]);
    let mut flow = factory.build().unwrap();
    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        ["scan", "union", "sink"]
    );
    flow.advance().unwrap();
    let status = flow.status().unwrap();
    assert_eq!(status[1].inputs.len(), 2);
    assert_eq!(
        status[1]
            .inputs
            .iter()
            .map(|input| input.position)
            .sum::<u64>(),
        1
    );
    drop(flow);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    flow.advance().unwrap();
    assert!(
        flow.status().unwrap()[1]
            .inputs
            .iter()
            .all(|input| input.position == 1)
    );
}

#[test]
fn resource_on_absorbed_operation_is_rejected_before_creating_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    let project = factory.operation(
        "project",
        Box::new(
            SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        ),
        [scan],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [project]);
    factory.resource("project", 42_u64).unwrap();
    assert!(
        matches!(factory.build(), Err(FlowError::UnknownRuntimeResource { station_id }) if station_id == "project")
    );
    assert!(!path.exists());
}
