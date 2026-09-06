use std::{num::NonZeroU64, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::DistinctDefinition,
};
use dogpaddle_store::{AppendLog, Store};

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
    let output: AppendLog<Vec<u8>> = store.open_data(DISTINCT_OUTPUT).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .append(&blocker)
            .unwrap(),
        0
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
        (pressured[1].inputs[0].cursor, pressured[1].inputs[0].tail),
        (0, 1)
    );
    assert_eq!(pressured[1].output.as_ref().unwrap().tail, 1);
    drop(flow);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    let completed = reopened.status().unwrap();
    assert_eq!(
        (completed[1].inputs[0].cursor, completed[1].inputs[0].tail),
        (1, 1)
    );
    assert_eq!(completed[1].output.as_ref().unwrap().tail, 2);
}

fn value_change(value: u64) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}
