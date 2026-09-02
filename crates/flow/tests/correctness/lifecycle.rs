use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).unwrap();

#[test]
fn multi_component_chain_and_fanout_survive_the_complete_build_run_reopen_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let chain_source = builder.station("chain-source", SequenceSourceDefinition::new(u64::MAX - 1));
    let count = builder.station("count", CountDefinition::new());
    let chain_sink = builder.station("chain-sink", DiscardDefinition::new());
    let fanout_source = builder.station("fanout-source", SequenceSourceDefinition::new(u64::MAX));
    let first_sink = builder.station("first-fanout-sink", DiscardDefinition::new());
    let second_sink = builder.station("second-fanout-sink", DiscardDefinition::new());
    for station in [chain_source, count, fanout_source] {
        builder.output_capacity_bytes(station, OUTPUT_CAPACITY_BYTES);
    }
    builder.connect([chain_source], count);
    builder.connect([count], chain_sink);
    builder.connect([fanout_source], first_sink);
    builder.connect([fanout_source], second_sink);
    let flow = builder.build().unwrap();
    assert_eq!(
        (flow.path(), flow.station_ids().collect::<Vec<_>>()),
        (
            path.as_path(),
            vec![
                "chain-source",
                "count",
                "chain-sink",
                "fanout-source",
                "first-fanout-sink",
                "second-fanout-sink",
            ]
        )
    );
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    drop(flow);
    assert_completed_state(&path);
}

fn assert_completed_state(path: &Path) {
    let store = Store::open(path).unwrap();
    let positions: [Cell<u64>; 2] = [
        store
            .open_data("station/00000000/operation/sequence_source.position")
            .unwrap(),
        store
            .open_data("station/00000003/operation/sequence_source.position")
            .unwrap(),
    ];
    let count: Cell<u64> = store.open_data("station/00000001/operation/count").unwrap();
    let outputs: [AppendLog<Vec<u8>>; 3] = [
        store.open_data("station/00000000/output").unwrap(),
        store.open_data("station/00000001/output").unwrap(),
        store.open_data("station/00000003/output").unwrap(),
    ];
    let fanout_states: [OrderedMap<Vec<u8>, Vec<u8>, Small>; 2] = [
        store.open_data("station/00000004/state").unwrap(),
        store.open_data("station/00000005/state").unwrap(),
    ];
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    assert_eq!(
        positions.map(|position| position.access(access).unwrap().get().unwrap()),
        [Some(u64::MAX), Some(u64::MAX)]
    );
    assert_eq!(count.access(access).unwrap().get().unwrap(), Some(2));
    for (output, bounds) in outputs.iter().zip([2..2, 2..2, 1..1]) {
        let output = output.access(access).unwrap();
        assert_eq!(
            (output.bounds().unwrap(), output.retained_bytes().unwrap()),
            (bounds, 0)
        );
    }
    for state in fanout_states {
        let encoded = state
            .access(access)
            .unwrap()
            .get(&b"input/00000000/cursor".to_vec())
            .unwrap()
            .unwrap();
        assert_eq!(u64::from_be_bytes(encoded.try_into().unwrap()), 1);
    }
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(source, OUTPUT_CAPACITY_BYTES);
    builder.connect([source], sink);
    let flow = builder.build().unwrap();

    assert!(matches!(FlowFactory::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    assert!(FlowFactory::open(&path).is_ok());
}

#[test]
fn build_and_open_support_many_station_output_logs() {
    const OUTPUT_STATION_COUNT: usize = 65;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let mut previous = builder.station("source", SequenceSourceDefinition::new(0));
    builder.output_capacity_bytes(previous, OUTPUT_CAPACITY_BYTES);
    for index in 1..OUTPUT_STATION_COUNT {
        let current = builder.station(format!("count-{index}"), CountDefinition::new());
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
        FlowFactory::open(path).unwrap().station_count(),
        OUTPUT_STATION_COUNT + 1
    );
}
