use std::{num::NonZeroU64, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowFactory};
use dogpaddle_operation::{
    col,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{
            AsOfDirection, AsOfJoinDefinition, AsOfJoinKind, AsOfOrderKey, AsOfTieFallback,
            RunningEventCountDefinition,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, Store, SubscribedLog};

const CAPACITY: NonZeroU64 = NonZeroU64::MAX;
const ROW_COUNT: usize = 1_025;
const LEFT_ROWS: &str = "station/00000002/operation/00000000/asof_join.left_rows";
const RIGHT_ROWS: &str = "station/00000002/operation/00000000/asof_join.right_rows";
const CONTINUATION: &str = "station/00000002/operation/00000000/asof_join.continuation";
const EVENT_COUNT: &str = "station/00000003/operation/00000000/running_event_count.count";

#[test]
fn asof_right_rematch_continuation_survives_reopen_in_a_real_two_input_flow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_fixture(&path);
    publish_input_changes(&path);

    let mut opened = FlowFactory::new(&path).open().unwrap();
    finish_left_claim(&mut opened);
    assert_eq!(opened.advance().unwrap(), AdvanceOutcome::Progressed);
    let suspended = opened.status().unwrap();
    assert_eq!(suspended[2].active_input, Some(1));
    assert_eq!(
        (suspended[2].inputs[1].position, suspended[2].inputs[1].tail),
        (0, 1)
    );
    drop(opened);

    assert_probe_continuation(&path);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    run_until_idle(&mut reopened);
    let completed = reopened.status().unwrap();
    assert_eq!(
        completed[2]
            .inputs
            .iter()
            .map(|input| (input.position, input.tail))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 1)]
    );
    let output = completed[2].output.as_ref().unwrap();
    assert!(
        output.tail > 1,
        "the rematch must emit multiple bounded pages"
    );
    assert_eq!(completed[3].inputs[0].position, output.tail);
    drop(reopened);

    assert_completed_resources(&path);
}

#[test]
fn asof_emit_backpressure_rolls_back_and_reopens_the_correction_exactly_once() {
    const BACKPRESSURE_ROW_COUNT: usize = 1_024;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("backpressure");
    build_backpressure_fixture(&path);
    publish_changes(
        &path,
        &value_change((0..BACKPRESSURE_ROW_COUNT).map(|value| value as u64)),
        &value_change([0]),
    );

    let mut seeded = FlowFactory::new(&path).open().unwrap();
    run_until_idle(&mut seeded);
    drop(seeded);
    assert_eq!(read_event_count(&path), Some(BACKPRESSURE_ROW_COUNT as u64));
    assert_eq!(read_map_len(&path, RIGHT_ROWS, 2), 1);

    publish_right_change(&path, &value_change([1]));
    let emit_continuation = drive_right_rematch_to_emit(&path);
    let right_rows_before_pressure = read_map_len(&path, RIGHT_ROWS, 3);
    let event_count_before_pressure = read_event_count(&path).unwrap();
    let blocker_tail = append_join_output_blocker(&path);

    let mut pressured = FlowFactory::new(&path).open().unwrap();
    assert_eq!(pressured.advance().unwrap(), AdvanceOutcome::Progressed);
    let status = pressured.status().unwrap();
    assert_eq!(status[2].last_outcome, Some(AdvanceOutcome::Backpressured));
    assert_eq!(
        (status[2].inputs[1].position, status[2].inputs[1].tail),
        (1, 2)
    );
    assert_eq!(status[2].output.as_ref().unwrap().tail, blocker_tail);
    drop(pressured);

    assert_eq!(
        read_map_len(&path, RIGHT_ROWS, 3),
        right_rows_before_pressure,
        "the backpressured Emit transaction must not change the right index"
    );
    assert_eq!(read_continuation(&path), Some(emit_continuation));
    assert_eq!(
        read_event_count(&path),
        Some(event_count_before_pressure + 1),
        "the downstream counter must consume only the injected blocker"
    );

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    run_until_idle(&mut reopened);
    let completed = reopened.status().unwrap();
    assert_eq!(
        (completed[2].inputs[1].position, completed[2].inputs[1].tail),
        (2, 2)
    );
    drop(reopened);

    assert_eq!(read_map_len(&path, RIGHT_ROWS, 3), 2);
    assert_eq!(read_continuation(&path), None);
    assert_eq!(
        read_event_count(&path),
        Some((BACKPRESSURE_ROW_COUNT + 1 + 2 * (BACKPRESSURE_ROW_COUNT - 1)) as u64),
        "one blocker plus every old/new rematch pair must be observed exactly once"
    );

    let mut reopened_again = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened_again.advance().unwrap(), AdvanceOutcome::Idle);
    drop(reopened_again);
    assert_eq!(
        read_event_count(&path),
        Some((BACKPRESSURE_ROW_COUNT + 1 + 2 * (BACKPRESSURE_ROW_COUNT - 1)) as u64)
    );
}

fn build_fixture(path: &Path) {
    let mut factory = FlowFactory::new(path);
    let left = factory.station("left", SequenceScanDefinition::new(u64::MAX));
    let right = factory.station("right", SequenceScanDefinition::new(u64::MAX));
    let join = factory.station(
        "asof",
        AsOfJoinDefinition::try_new(
            AsOfJoinKind::Inner,
            AsOfDirection::Backward { allow_exact: true },
            [],
            [AsOfOrderKey::new(col("value"), col("value"))],
            [],
            AsOfTieFallback::CanonicalAscending,
            None,
            ["left_value", "right_value"],
            None,
        )
        .unwrap(),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left, right, join] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([left, right], join);
    factory.connect([join], sink);

    let built = factory.build().unwrap();
    assert_eq!(
        built.station_ids().collect::<Vec<_>>(),
        ["left", "right", "asof", "sink"]
    );
}

fn build_backpressure_fixture(path: &Path) {
    let mut factory = FlowFactory::new(path);
    let left = factory.station("left", SequenceScanDefinition::new(u64::MAX));
    let right = factory.station("right", SequenceScanDefinition::new(u64::MAX));
    let join = factory.station(
        "asof",
        AsOfJoinDefinition::try_new(
            AsOfJoinKind::Inner,
            AsOfDirection::Backward { allow_exact: true },
            [],
            [AsOfOrderKey::new(col("value"), col("value"))],
            [],
            AsOfTieFallback::CanonicalAscending,
            None,
            ["left_value", "right_value"],
            None,
        )
        .unwrap(),
    );
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(left, CAPACITY);
    factory.output_capacity_bytes(right, CAPACITY);
    factory.output_capacity_bytes(join, NonZeroU64::MIN);
    factory.output_capacity_bytes(count, CAPACITY);
    factory.connect([left, right], join);
    factory.connect([join], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());
}

fn assert_probe_continuation(path: &Path) {
    let store = Store::open(path).unwrap();
    let left_rows: OrderedMap<Vec<u8>, u64> = store.open_data(LEFT_ROWS).unwrap();
    let right_rows: OrderedMap<Vec<u8>, u64> = store.open_data(RIGHT_ROWS).unwrap();
    let continuation: Cell<Vec<u8>> = store.open_data(CONTINUATION).unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        left_rows
            .read(transaction.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(ROW_COUNT + 1, usize::MAX).unwrap(),
            )
            .unwrap()
            .entries
            .len(),
        ROW_COUNT
    );
    assert!(
        right_rows
            .read(transaction.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(1, usize::MAX).unwrap(),
            )
            .unwrap()
            .entries
            .is_empty()
    );
    let encoded = continuation
        .read(transaction.access())
        .unwrap()
        .get()
        .unwrap()
        .expect("the right rematch must persist its unfinished Probe cursor");
    assert_eq!(&encoded[..3], &[1, 1, 0]);
}

fn assert_completed_resources(path: &Path) {
    let store = Store::open(path).unwrap();
    let continuation: Cell<Vec<u8>> = store.open_data(CONTINUATION).unwrap();
    let right_rows: OrderedMap<Vec<u8>, u64> = store.open_data(RIGHT_ROWS).unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        continuation
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        None
    );
    assert_eq!(
        right_rows
            .read(transaction.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(2, usize::MAX).unwrap(),
            )
            .unwrap()
            .entries
            .len(),
        1
    );
}

fn publish_input_changes(path: &Path) {
    publish_changes(
        path,
        &value_change((0..ROW_COUNT).map(|value| value as u64)),
        &value_change([0]),
    );
}

fn publish_changes(path: &Path, left_change: &Change, right_change: &Change) {
    let left_change = encode_change(left_change).unwrap();
    let right_change = encode_change(right_change).unwrap();
    let store = Store::open(path).unwrap();
    let left_position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let right_position: Cell<u64> = store
        .open_data("station/00000001/operation/00000000/sequence_scan.position")
        .unwrap();
    let left_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let right_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    left_position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    right_position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert!(
        left_output
            .writer()
            .try_append(&left_change, CAPACITY, transaction.access())
            .unwrap()
    );
    assert!(
        right_output
            .writer()
            .try_append(&right_change, CAPACITY, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
}

fn publish_right_change(path: &Path, change: &Change) {
    let change = encode_change(change).unwrap();
    let store = Store::open(path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert!(
        output
            .writer()
            .try_append(&change, CAPACITY, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
}

fn drive_right_rematch_to_emit(path: &Path) -> Vec<u8> {
    for _ in 0..64 {
        let mut flow = FlowFactory::new(path).open().unwrap();
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
        drop(flow);
        if let Some(encoded) = read_continuation(path)
            && encoded.starts_with(&[1, 1, 1])
        {
            return encoded;
        }
    }
    panic!("ASOF Flow did not persist the bounded right rematch Emit phase");
}

fn append_join_output_blocker(path: &Path) -> u64 {
    let blocker = encode_change(&pair_change(0, 0)).unwrap();
    let store = Store::open(path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert!(
        output
            .writer()
            .try_append(&blocker, CAPACITY, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let transaction = store.read_transaction();
    output.writer().status(transaction.access()).unwrap().tail
}

fn read_continuation(path: &Path) -> Option<Vec<u8>> {
    let store = Store::open(path).unwrap();
    let continuation: Cell<Vec<u8>> = store.open_data(CONTINUATION).unwrap();
    let transaction = store.read_transaction();
    continuation
        .read(transaction.access())
        .unwrap()
        .get()
        .unwrap()
}

fn read_event_count(path: &Path) -> Option<u64> {
    let store = Store::open(path).unwrap();
    let count: Cell<u64> = store.open_data(EVENT_COUNT).unwrap();
    let transaction = store.read_transaction();
    count.read(transaction.access()).unwrap().get().unwrap()
}

fn read_map_len(path: &Path, resource: &str, limit: usize) -> usize {
    let store = Store::open(path).unwrap();
    let rows: OrderedMap<Vec<u8>, u64> = store.open_data(resource).unwrap();
    let transaction = store.read_transaction();
    rows.read(transaction.access())
        .unwrap()
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(limit, usize::MAX).unwrap(),
        )
        .unwrap()
        .entries
        .len()
}

fn value_change(values: impl IntoIterator<Item = u64>) -> Change {
    let values = values.into_iter().collect::<Vec<_>>();
    let row_count = values.len();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1; row_count])).unwrap()
}

fn pair_change(left: u64, right: u64) -> Change {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left_value", DataType::UInt64, false),
        Field::new("right_value", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![left])),
            Arc::new(UInt64Array::from(vec![right])),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}

fn finish_left_claim(flow: &mut Flow) {
    for _ in 0..64 {
        let status = flow.status().unwrap();
        if status[2].inputs[0].position == 1 {
            return;
        }
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    panic!("ASOF Flow did not finish its bounded left fixture");
}

fn run_until_idle(flow: &mut Flow) {
    for _ in 0..64 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            return;
        }
    }
    panic!("ASOF Flow did not become idle within its bounded fixture");
}
