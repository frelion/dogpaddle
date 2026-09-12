use std::{num::NonZeroU64, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition,
    sink::DiscardDefinition,
    transform::{DistinctDefinition, RunningEventCountDefinition},
};
use dogpaddle_store::{Cell, Store, SubscribedLog};

const DISTINCT_OUTPUT: &str = "station/00000001/output";

#[test]
fn distinct_retries_the_same_input_after_backpressure_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(u64::MAX));
    let distinct = factory.station("distinct", DistinctDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, NonZeroU64::MAX);
    factory.output_capacity_bytes(distinct, NonZeroU64::MIN);
    factory.connect([scan], distinct);
    factory.connect([distinct], sink);
    drop(factory.build().unwrap());

    let blocker = encode_change(&value_change(41)).unwrap();
    let store = Store::open(&path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data(DISTINCT_OUTPUT).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert!(
        output
            .writer()
            .try_append(&blocker, NonZeroU64::MAX, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
    drop(transactions);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    let pressured = flow.status().unwrap();
    assert_eq!(
        pressured[1].last_outcome,
        Some(AdvanceOutcome::Backpressured)
    );
    assert_eq!(
        (pressured[1].inputs[0].position, pressured[1].inputs[0].tail),
        (0, 1)
    );
    assert_eq!(pressured[1].output.as_ref().unwrap().tail, 1);
    drop(flow);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    let completed = reopened.status().unwrap();
    assert_eq!(
        (completed[1].inputs[0].position, completed[1].inputs[0].tail),
        (1, 1)
    );
    assert_eq!(completed[1].output.as_ref().unwrap().tail, 2);
}

#[test]
fn fused_stateful_operations_roll_back_together_when_final_output_is_backpressured() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("fused");
    let mut factory = FlowFactory::new(&path);
    let compute = factory.station("compute", SequenceScanDefinition::new(u64::MAX));
    factory
        .append(compute, RunningEventCountDefinition::new())
        .unwrap();
    factory.append(compute, DistinctDefinition::new()).unwrap();
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(compute, NonZeroU64::MIN);
    factory.connect([compute], sink);
    drop(factory.build().unwrap());

    let blocker = encode_change(&count_change(41)).unwrap();
    let store = Store::open(&path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert!(
        output
            .writer()
            .try_append(&blocker, NonZeroU64::MAX, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
    drop(transactions);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(
        flow.status().unwrap()[0].last_outcome,
        Some(AdvanceOutcome::Backpressured)
    );
    drop(flow);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000000/operation/00000001/running_event_count.count")
        .unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        position.read(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        count.read(transaction.access()).unwrap().get().unwrap(),
        None
    );
    drop(transaction);
    drop(store);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(reopened);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000000/operation/00000001/running_event_count.count")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        position.read(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        count.read(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
    assert_eq!(
        output.writer().status(transaction.access()).unwrap().tail,
        2
    );
}

fn value_change(value: u64) -> Change {
    uint64_change("value", value)
}

fn count_change(value: u64) -> Change {
    uint64_change("count", value)
}

fn uint64_change(name: &str, value: u64) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}
