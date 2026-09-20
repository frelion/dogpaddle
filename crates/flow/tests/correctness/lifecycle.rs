use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, Store, SubscribedLog};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();
const OWNER_IDENTITY: [u8; 32] = [0xa5; 32];
const OTHER_OWNER_IDENTITY: [u8; 32] = [0x5a; 32];

#[test]
fn multi_component_chain_and_fanout_survive_the_complete_build_run_reopen_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let chain_scan = builder.operation(
        "chain-scan",
        Box::new(SequenceScanDefinition::new(u64::MAX - 1)),
        [],
    );
    let count = builder.operation(
        "count",
        Box::new(RunningEventCountDefinition::new()),
        [chain_scan],
    );
    builder.operation("chain-sink", Box::new(DiscardDefinition::new()), [count]);
    let fanout_scan = builder.operation(
        "fanout-scan",
        Box::new(SequenceScanDefinition::new(u64::MAX)),
        [],
    );
    builder.operation(
        "first-fanout-sink",
        Box::new(DiscardDefinition::new()),
        [fanout_scan],
    );
    builder.operation(
        "second-fanout-sink",
        Box::new(DiscardDefinition::new()),
        [fanout_scan],
    );
    for station in [chain_scan, count, fanout_scan] {
        builder.materialize(station, OUTPUT_CAPACITY_BYTES);
    }

    let flow = builder.build().unwrap();
    assert_eq!(
        (flow.path(), flow.station_ids().collect::<Vec<_>>()),
        (
            path.as_path(),
            vec![
                "chain-scan",
                "count",
                "chain-sink",
                "fanout-scan",
                "first-fanout-sink",
                "second-fanout-sink",
            ]
        )
    );
    drop(flow);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    drop(flow);
    assert_completed_state(&path);
}

fn assert_completed_state(path: &Path) {
    let store = Store::open(path).unwrap();
    let positions: [Cell<u64>; 2] = [
        store
            .open_data("station/00000000/operation/00000000/sequence_scan.position")
            .unwrap(),
        store
            .open_data("station/00000003/operation/00000000/sequence_scan.position")
            .unwrap(),
    ];
    let count: Cell<u64> = store
        .open_data("station/00000001/operation/00000000/running_event_count.count")
        .unwrap();
    let outputs: [SubscribedLog<Vec<u8>>; 3] = [
        store.open_data("station/00000000/output").unwrap(),
        store.open_data("station/00000001/output").unwrap(),
        store.open_data("station/00000003/output").unwrap(),
    ];
    let transaction = store.read_transaction();
    let access = transaction.access();
    assert_eq!(
        positions.map(|position| position.read(access).unwrap().get().unwrap()),
        [Some(u64::MAX), Some(u64::MAX)]
    );
    assert_eq!(count.read(access).unwrap().get().unwrap(), Some(2));
    for (output, position) in outputs.iter().zip([2, 2, 1]) {
        let output = output.writer().status(access).unwrap();
        assert_eq!(
            (output.head, output.tail, output.retained_bytes),
            (position, position, 0)
        );
    }
    let expected_positions: [&[u64]; 3] = [&[2], &[2], &[1, 1]];
    for (output, positions) in outputs.iter().zip(expected_positions) {
        for (subscriber, &expected) in positions.iter().enumerate() {
            let status = output
                .subscription(u64::try_from(subscriber).unwrap())
                .status(access)
                .unwrap();
            assert_eq!((status.position, status.tail), (expected, expected));
        }
    }
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation("sink", Box::new(DiscardDefinition::new()), [scan]);
    builder.materialize(scan, OUTPUT_CAPACITY_BYTES);

    let flow = builder.build().unwrap();

    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::Store(_))
    ));
    drop(flow);
    assert!(FlowFactory::new(&path).open().is_ok());
}

#[test]
fn open_requires_the_exact_owner_identity_before_binding_runtime_resources() {
    let root = tempfile::tempdir().unwrap();
    let identified_path = root.path().join("identified");
    let mut builder = FlowFactory::new(&identified_path);
    builder.owner_identity(OWNER_IDENTITY);
    let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation("sink", Box::new(DiscardDefinition::new()), [scan]);
    builder.materialize(scan, OUTPUT_CAPACITY_BYTES);

    drop(builder.build().unwrap());

    assert!(matches!(
        FlowFactory::new(&identified_path).open(),
        Err(FlowError::OwnerIdentityMismatch)
    ));

    let mut wrong_owner = FlowFactory::new(&identified_path);
    wrong_owner.owner_identity(OTHER_OWNER_IDENTITY);
    wrong_owner.resource("unknown", ()).unwrap();
    assert!(matches!(
        wrong_owner.open(),
        Err(FlowError::OwnerIdentityMismatch)
    ));

    let mut matching_owner = FlowFactory::new(&identified_path);
    matching_owner.owner_identity(OWNER_IDENTITY);
    drop(matching_owner.open().unwrap());

    let anonymous_path = root.path().join("anonymous");
    let mut builder = FlowFactory::new(&anonymous_path);
    let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation("sink", Box::new(DiscardDefinition::new()), [scan]);
    builder.materialize(scan, OUTPUT_CAPACITY_BYTES);

    drop(builder.build().unwrap());

    let mut unexpected_owner = FlowFactory::new(&anonymous_path);
    unexpected_owner.owner_identity(OWNER_IDENTITY);
    assert!(matches!(
        unexpected_owner.open(),
        Err(FlowError::OwnerIdentityMismatch)
    ));
    drop(FlowFactory::new(anonymous_path).open().unwrap());
}

#[test]
fn build_and_open_support_many_station_output_logs() {
    const OUTPUT_STATION_COUNT: usize = 65;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let mut previous = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.materialize(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..OUTPUT_STATION_COUNT {
        let current = builder.operation(
            format!("count-{index}"),
            Box::new(RunningEventCountDefinition::new()),
            [previous],
        );
        builder.materialize(current, OUTPUT_CAPACITY_BYTES);

        previous = current;
    }
    builder.operation("sink", Box::new(DiscardDefinition::new()), [previous]);

    let flow = builder.build().unwrap();
    assert_eq!(flow.station_count(), OUTPUT_STATION_COUNT + 1);
    drop(flow);
    assert_eq!(
        FlowFactory::new(path).open().unwrap().station_count(),
        OUTPUT_STATION_COUNT + 1
    );
}
