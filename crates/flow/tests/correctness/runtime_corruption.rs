use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
    path::Path,
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    Action, Operation,
    sink::DiscardDefinition,
    source::{SequenceSourceDefinition, SequenceSourceOperation},
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, ScanLimit, Small, Store, StoreError};

const ACTIVE: &[u8] = b"input/active";
const CURSOR: &[u8] = b"input/00000000/cursor";

#[derive(Debug, Eq, PartialEq)]
struct DurableInputState {
    source_position: Option<u64>,
    active: Option<Vec<u8>>,
    cursor: Option<Vec<u8>>,
    output_bounds: Range<u64>,
    output_retained_bytes: u64,
    encoded_entry: Option<Vec<u8>>,
}

#[test]
fn open_rejects_missing_malformed_and_out_of_range_cursors_without_writes() {
    let root = tempfile::tempdir().unwrap();
    #[rustfmt::skip]
    let cases = [
        ("missing", None, false, "output consumer 0 has no durable cursor"),
        ("malformed", Some(vec![0; 7]), false, "output consumer 0 has a malformed durable cursor"),
        ("above-tail", Some(2_u64.to_be_bytes().to_vec()), false, "output consumer 0 cursor 2 is outside retained range [0, 1]"),
        ("below-head", Some(0_u64.to_be_bytes().to_vec()), true, "output consumer 0 cursor 0 is outside retained range [1, 2]"),
        ("head-behind", Some(1_u64.to_be_bytes().to_vec()), false, "output retention head 0 does not equal minimum consumer cursor 1"),
    ];
    for (case, value, shift_head, reason) in cases {
        let path = root.path().join(case);
        publish_pending_input(&path, None, shift_head);
        write_sink_state(&path, CURSOR, value);
        let before = durable_input_state(&path);
        let Err(error) = FlowFactory::open(&path) else {
            panic!("case {case} unexpectedly opened");
        };
        assert!(matches!(
            error,
            FlowError::InvalidRuntimeState { station_id, reason: actual }
                if station_id == "source" && actual == reason
        ));
        assert_eq!(durable_input_state(&path), before, "case {case}");
    }
}

#[test]
fn advance_rejects_every_invalid_active_input_without_writes() {
    let root = tempfile::tempdir().unwrap();
    #[rustfmt::skip]
    let cases = [
        ("missing", None, "station \"sink\" failed: station has inputs but no durable active input"),
        ("malformed", Some(vec![0; 3]), "station \"sink\" failed: station durable active input is malformed"),
        ("out-of-range", Some(1_u32.to_be_bytes().to_vec()), "station \"sink\" failed: station durable active input 1 is outside input count 1"),
    ];
    for (case, value, message) in cases {
        let path = root.path().join(case);
        publish_pending_input(&path, None, false);
        write_sink_state(&path, ACTIVE, value);
        let before = durable_input_state(&path);
        let mut flow = FlowFactory::open(&path).unwrap();
        let error = flow.advance().unwrap_err();
        assert_eq!(
            (error.station_id(), error.to_string().as_str()),
            ("sink", message)
        );
        drop(flow);
        assert_eq!(durable_input_state(&path), before, "case {case}");
    }
}

#[test]
fn advance_rejects_an_invalid_encoded_change_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    publish_pending_input(&path, Some(b"not an Arrow IPC stream"), false);
    let before = durable_input_state(&path);
    let mut flow = FlowFactory::open(&path).unwrap();
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
    publish_pending_input(&path, Some(&encoded), false);
    let before = durable_input_state(&path);

    let mut flow = FlowFactory::open(&path).unwrap();
    let error = flow.advance().unwrap_err();
    assert_eq!(error.station_id(), "sink");
    assert!(
        error
            .to_string()
            .contains("station input 0 Schema does not match its bound output")
    );
    drop(flow);

    assert_eq!(durable_input_state(&path), before);
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

fn publish_pending_input(path: &Path, encoded: Option<&[u8]>, shift_head: bool) {
    let mut builder = FlowFactory::new(path);
    let source = builder.station("source", SequenceSourceDefinition::new(u64::MAX));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(source, NonZeroU64::MAX);
    builder.connect([source], sink);
    drop(builder.build().unwrap());

    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let encoded = encoded.map_or_else(
        || {
            let operation = SequenceSourceOperation::new(u64::MAX, position.clone());
            let Action::Commit(Some(change)) = operation.turn(None, transaction.access()).unwrap()
            else {
                panic!("final source did not commit one Change");
            };
            encode_change(&change).unwrap()
        },
        |encoded| {
            position
                .access(transaction.access())
                .unwrap()
                .set(&u64::MAX)
                .unwrap();
            encoded.to_vec()
        },
    );
    let mut output = output.access(transaction.access()).unwrap();
    assert_eq!(output.append(&encoded).unwrap(), 0);
    if shift_head {
        assert_eq!(output.append(&encoded).unwrap(), 1);
        assert_eq!(output.truncate_before(1, NonZeroUsize::MIN).unwrap(), 1);
    }
    transaction.commit().unwrap();
}

fn write_sink_state(path: &Path, key: &[u8], value: Option<Vec<u8>>) {
    let store = Store::open(path).unwrap();
    let state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut state = state.access(transaction.access()).unwrap();
    match value {
        Some(value) => state.put(&key.to_vec(), &value).unwrap(),
        None => assert!(state.remove(&key.to_vec()).unwrap()),
    }
    transaction.commit().unwrap();
}

fn durable_input_state(path: &Path) -> DurableInputState {
    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    let source_position = position.access(access).unwrap().get().unwrap();
    let state = state.access(access).unwrap();
    let active = state.get(&ACTIVE.to_vec()).unwrap();
    let cursor = state.get(&CURSOR.to_vec()).unwrap();
    let output = output.access(access).unwrap();
    let output_bounds = output.bounds().unwrap();
    let mut encoded_entry = None;
    output
        .scan(
            output_bounds.start,
            ScanLimit::new(1, usize::MAX).unwrap(),
            |entry| {
                encoded_entry = Some(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    DurableInputState {
        source_position,
        active,
        cursor,
        output_bounds,
        output_retained_bytes: output.retained_bytes().unwrap(),
        encoded_entry,
    }
}
