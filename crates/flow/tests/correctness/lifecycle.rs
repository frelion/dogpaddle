use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, Store, SubscribedLog};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();

#[test]
fn multi_component_chain_and_fanout_survive_the_complete_build_run_reopen_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let chain_scan = builder.station("chain-scan", SequenceScanDefinition::new(u64::MAX - 1));
    let count = builder.station("count", RunningEventCountDefinition::new());
    let chain_sink = builder.station("chain-sink", DiscardDefinition::new());
    let fanout_scan = builder.station("fanout-scan", SequenceScanDefinition::new(u64::MAX));
    let first_sink = builder.station("first-fanout-sink", DiscardDefinition::new());
    let second_sink = builder.station("second-fanout-sink", DiscardDefinition::new());
    for station in [chain_scan, count, fanout_scan] {
        builder.output_capacity_bytes(station, OUTPUT_CAPACITY_BYTES);
    }
    builder.connect([chain_scan], count);
    builder.connect([count], chain_sink);
    builder.connect([fanout_scan], first_sink);
    builder.connect([fanout_scan], second_sink);
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
            .open_data("station/00000000/operation/sequence_scan.position")
            .unwrap(),
        store
            .open_data("station/00000003/operation/sequence_scan.position")
            .unwrap(),
    ];
    let count: Cell<u64> = store
        .open_data("station/00000001/operation/running_event_count.count")
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
    let scan = builder.station("scan", SequenceScanDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(scan, OUTPUT_CAPACITY_BYTES);
    builder.connect([scan], sink);
    let flow = builder.build().unwrap();

    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::Store(_))
    ));
    drop(flow);
    assert!(FlowFactory::new(&path).open().is_ok());
}

#[test]
fn build_and_open_support_many_station_output_logs() {
    const OUTPUT_STATION_COUNT: usize = 65;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let mut previous = builder.station("scan", SequenceScanDefinition::new(0));
    builder.output_capacity_bytes(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..OUTPUT_STATION_COUNT {
        let current = builder.station(format!("count-{index}"), RunningEventCountDefinition::new());
        builder.output_capacity_bytes(current, OUTPUT_CAPACITY_BYTES);
        builder.connect([previous], current);
        previous = current;
    }
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([previous], sink);

    let flow = builder.build().unwrap();
    assert_eq!(flow.station_count(), OUTPUT_STATION_COUNT + 1);
    drop(flow);
    assert_eq!(
        FlowFactory::new(path).open().unwrap().station_count(),
        OUTPUT_STATION_COUNT + 1
    );
}
