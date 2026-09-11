use std::{
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::UnionAllDefinition,
};
use dogpaddle_store::{Cell, Store, SubscribedLog, SubscribedLogStatus, SubscriptionStatus};

#[derive(Debug, Eq, PartialEq)]
struct DurableInputState {
    scan_position: Option<u64>,
    output: SubscribedLogStatus,
    input: SubscriptionStatus,
    encoded_entry: Option<Vec<u8>>,
}

#[test]
fn open_rejects_missing_or_out_of_range_multi_input_active_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let cases = [
        (
            "missing",
            None,
            "station has inputs but no durable active input",
        ),
        (
            "out-of-range",
            Some(2),
            "station durable active input 2 is outside input count 2",
        ),
    ];
    for (case, value, reason) in cases {
        let path = root.path().join(case);
        build_two_input_union(&path);
        write_union_active(&path, value);
        let before = read_union_active(&path);

        let Err(error) = FlowFactory::new(&path).open() else {
            panic!("case {case} unexpectedly opened");
        };
        assert!(matches!(
            error,
            FlowError::InvalidRuntimeState { station_id, reason: actual }
                if station_id == "union" && actual == reason
        ));
        assert_eq!(read_union_active(&path), before, "case {case}");
    }
}

#[test]
fn advance_rejects_an_invalid_encoded_change_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    publish_pending_input(&path, b"not an Arrow IPC stream");
    let before = durable_input_state(&path);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    let error = flow.advance().unwrap_err();
    assert_eq!(error.station_id(), "sink");
    assert!(
        error
            .to_string()
            .starts_with("station \"sink\" failed: station input 0 contains an invalid Change:")
    );
    drop(flow);
    assert_eq!(durable_input_state(&path), before);
}

#[test]
fn advance_rejects_a_valid_change_with_the_wrong_bound_schema_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let encoded = encode_change(&count_change(7)).unwrap();
    publish_pending_input(&path, &encoded);
    let before = durable_input_state(&path);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    let error = flow.advance().unwrap_err();
    assert_eq!(error.station_id(), "sink");
    assert!(
        error
            .to_string()
            .contains("station input 0 Schema does not match its binding")
    );
    drop(flow);

    assert_eq!(durable_input_state(&path), before);
}

fn build_two_input_union(path: &Path) {
    let mut builder = FlowFactory::new(path);
    let left = builder.station("left", SequenceScanDefinition::new(0));
    let right = builder.station("right", SequenceScanDefinition::new(0));
    let union = builder.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = builder.station("sink", DiscardDefinition::new());
    for station in [left, right, union] {
        builder.output_capacity_bytes(station, NonZeroU64::MAX);
    }
    builder.connect([left, right], union);
    builder.connect([union], sink);
    drop(builder.build().unwrap());
}

fn write_union_active(path: &Path, value: Option<u32>) {
    let store = Store::open(path).unwrap();
    let active: Cell<u32> = store.open_data("station/00000002/active-input").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut active = active.access(transaction.access()).unwrap();
    match value {
        Some(value) => active.set(&value).unwrap(),
        None => assert!(active.clear().unwrap()),
    }
    transaction.commit().unwrap();
}

fn read_union_active(path: &Path) -> Option<u32> {
    let store = Store::open(path).unwrap();
    let active: Cell<u32> = store.open_data("station/00000002/active-input").unwrap();
    let transaction = store.read_transaction();
    let active = active.read(transaction.access()).unwrap();
    active.get().unwrap()
}

fn count_change(value: u64) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}

fn publish_pending_input(path: &Path, encoded: &[u8]) {
    let mut builder = FlowFactory::new(path);
    let scan = builder.station("scan", SequenceScanDefinition::new(u64::MAX));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(scan, NonZeroU64::MAX);
    builder.connect([scan], sink);
    drop(builder.build().unwrap());

    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_scan.position")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let writer = output.writer();
    let encoded = encoded.to_vec();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert!(
        writer
            .try_append(&encoded, NonZeroU64::MAX, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
}

fn durable_input_state(path: &Path) -> DurableInputState {
    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_scan.position")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let writer = output.writer();
    let input = output.subscription(0);
    let transaction = store.read_transaction();
    DurableInputState {
        scan_position: position.read(transaction.access()).unwrap().get().unwrap(),
        output: writer.status(transaction.access()).unwrap(),
        input: input.status(transaction.access()).unwrap(),
        encoded_entry: input
            .peek(transaction.access())
            .unwrap()
            .map(|(_, encoded)| encoded),
    }
}
