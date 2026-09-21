use std::{num::NonZeroU64, ops::Range, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::SqliteSinkDefinition,
        transform::{
            EquiJoinDefinition, EquiJoinError, EquiJoinKind, ExtendDefinition, FilterDefinition,
            SelectDefinition,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store, SubscribedLog};
use rusqlite::{Connection, OpenFlags};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::MAX;
const TABLE: &str = "events";

#[test]
fn transform_chain_materializes_filtered_rows_through_the_public_flow_api() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let scan_start = u64::MAX - 2;

    // SequenceScan becomes idle after u64::MAX, so this emits exactly three rows.
    let mut factory = FlowFactory::new(&flow_path);
    let scan = factory.operation(
        "scan",
        Box::new(SequenceScanDefinition::new(scan_start)),
        [],
    );
    let extend = factory.operation(
        "extend",
        Box::new(ExtendDefinition::try_new("offset", col("value") - lit(scan_start)).unwrap()),
        [scan],
    );
    let filter = factory.operation(
        "filter",
        Box::new(FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap()),
        [extend],
    );
    let select = factory.operation(
        "select",
        Box::new(
            SelectDefinition::try_new([("scan_value", col("value")), ("offset", col("offset"))])
                .unwrap(),
        ),
        [filter],
    );
    factory.operation(
        "sqlite",
        Box::new(SqliteSinkDefinition::try_new(&sqlite_path, TABLE).unwrap()),
        [select],
    );
    for station in [scan, extend, filter, select] {
        factory.materialize(station, OUTPUT_CAPACITY_BYTES);
    }

    let mut flow = factory.build().unwrap();

    assert!(
        !sqlite_path.exists(),
        "Flow build eagerly created the SQLite database"
    );

    let mut outcomes = Vec::new();
    for _ in 0..32 {
        let outcome = flow.advance().unwrap();
        outcomes.push(outcome);
        if outcome == AdvanceOutcome::Idle {
            break;
        }
    }
    let (last, progressed) = outcomes.split_last().expect("advance ran at least once");
    assert_eq!(*last, AdvanceOutcome::Idle);
    assert!(
        progressed
            .iter()
            .all(|outcome| *outcome == AdvanceOutcome::Progressed)
    );

    let rows = sqlite_u64_rows(&sqlite_path);
    assert_eq!(rows, [(1, u64::MAX - 1, 1, 16), (2, u64::MAX, 2, 16),]);
    println!("advance outcomes: {outcomes:?}");
    println!("SQLite rows (technical_id, scan_value, offset, hash_bytes): {rows:?}");
}

#[test]
fn sqlite_sink_releases_input_after_buffering_and_replays_each_fixed_target_batch() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    drop(build_sqlite_flow(&flow_path, &sqlite_path));
    drop(FlowFactory::new(&flow_path).open().unwrap());
    assert!(!sqlite_path.exists(), "build/open must not create SQLite");

    let encoded_change = encode_change(&multiplicity_change(7, 1_025)).unwrap();
    publish_scan_change(&flow_path, &encoded_change);
    let mut prepared = None;

    // Stop after the target transaction, before local settlement; reopen must
    // replay the same fixed IDs without allocating or deleting another row.
    for replay in 0..3 {
        let mut flow = FlowFactory::new(&flow_path).open().unwrap();
        for _ in 0..16 {
            assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
            if sqlite_rows(&sqlite_path) == Some(1_024) {
                break;
            }
        }
        drop(flow);
        assert_eq!(sqlite_rows(&sqlite_path), Some(1_024), "replay {replay}");
        let snapshot = sink_snapshot(&flow_path);
        assert_eq!(snapshot.input_position, 1);
        assert_eq!(snapshot.output_bounds, 1..1);
        assert_eq!(
            snapshot.encoded_entry.as_deref(),
            Some(encoded_change.as_slice())
        );
        assert!(snapshot.state.is_some());
        if replay == 0 {
            prepared = snapshot.state;
        } else {
            assert_eq!(snapshot.state, prepared);
        }
    }

    let mut flow = FlowFactory::new(&flow_path).open().unwrap();
    let mut outcome = AdvanceOutcome::Progressed;
    for _ in 0..16 {
        outcome = flow.advance().unwrap();
        if outcome == AdvanceOutcome::Idle {
            break;
        }
    }
    assert_eq!(outcome, AdvanceOutcome::Idle);
    drop(flow);

    let snapshot = sink_snapshot(&flow_path);
    assert_eq!(snapshot.input_position, 1);
    assert_eq!(snapshot.output_bounds, 1..1);
    assert_eq!(snapshot.encoded_entry, None);
    assert_ne!(snapshot.state, prepared);
    let connection = sqlite_connection(&sqlite_path);
    let rows = connection
        .prepare("SELECT \"$dogpaddle.id\", value FROM events ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, decode_u64_blob(row.get(1)?)))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, (1..=1_025).map(|id| (id, 7)).collect::<Vec<_>>());

    let mut reopened = FlowFactory::new(&flow_path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
    assert_eq!(sqlite_rows(&sqlite_path), Some(1_025));
}

fn build_sqlite_flow(flow_path: &Path, sqlite_path: &Path) -> dogpaddle_flow::Flow {
    let mut factory = FlowFactory::new(flow_path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(u64::MAX)), []);
    factory.operation(
        "sqlite",
        Box::new(SqliteSinkDefinition::try_new(sqlite_path, TABLE).unwrap()),
        [scan],
    );
    factory.materialize(scan, OUTPUT_CAPACITY_BYTES);

    factory.build().unwrap()
}

fn multiplicity_change(value: u64, diff: i64) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![diff])).unwrap()
}

fn publish_scan_change(flow_path: &Path, encoded_change: &[u8]) {
    let store = Store::open(flow_path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let writer = output.writer();
    let encoded_change = encoded_change.to_vec();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert!(
        writer
            .try_append(&encoded_change, NonZeroU64::MAX, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
}

#[derive(Debug)]
struct SinkSnapshot {
    input_position: u64,
    output_bounds: Range<u64>,
    encoded_entry: Option<Vec<u8>>,
    state: Option<Vec<u8>>,
}

fn sink_snapshot(flow_path: &Path) -> SinkSnapshot {
    let store = Store::open(flow_path).unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let writer = output.writer();
    let input = output.subscription(0);
    let sink_state: Cell<Vec<u8>> = store
        .open_data("station/00000001/operation/00000000/sink.control")
        .unwrap();
    let sink_buffer: OrderedMap<u64, Vec<u8>> = store
        .open_data("station/00000001/operation/00000000/sink.buffer")
        .unwrap();
    let transaction = store.read_transaction();
    let access = transaction.access();
    let input_status = input.status(access).unwrap();
    let output_status = writer.status(access).unwrap();
    SinkSnapshot {
        input_position: input_status.position,
        output_bounds: output_status.head..output_status.tail,
        encoded_entry: sink_buffer.read(access).unwrap().get(&0).unwrap(),
        state: sink_state.read(access).unwrap().get().unwrap(),
    }
}

fn sqlite_object_count(sqlite_path: &Path) -> i64 {
    sqlite_connection(sqlite_path)
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name IN (?1, ?2)",
            [TABLE, "$dogpaddle.hash_index.events"],
            |row| row.get(0),
        )
        .unwrap()
}

fn sqlite_row_count(sqlite_path: &Path) -> i64 {
    sqlite_connection(sqlite_path)
        .query_row("SELECT COUNT(*) FROM \"events\"", [], |row| row.get(0))
        .unwrap()
}

fn sqlite_u64_rows(sqlite_path: &Path) -> Vec<(i64, u64, u64, i64)> {
    let connection = sqlite_connection(sqlite_path);
    let mut statement = connection
        .prepare(
            "SELECT \"$dogpaddle.id\", \"scan_value\", \"offset\", \
                    length(\"$dogpaddle.hash\") \
             FROM \"events\" ORDER BY \"$dogpaddle.id\"",
        )
        .unwrap();
    statement
        .query_map([], |row| {
            let scan_value: Vec<u8> = row.get(1)?;
            let offset: Vec<u8> = row.get(2)?;
            Ok((
                row.get(0)?,
                decode_u64_blob(scan_value),
                decode_u64_blob(offset),
                row.get(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn decode_u64_blob(value: Vec<u8>) -> u64 {
    u64::from_be_bytes(value.try_into().expect("UInt64 uses an 8-byte BLOB"))
}

fn sqlite_rows(sqlite_path: &Path) -> Option<i64> {
    sqlite_path
        .exists()
        .then(|| sqlite_object_count(sqlite_path))
        .filter(|count| *count == 2)
        .map(|_| sqlite_row_count(sqlite_path))
}

fn sqlite_connection(sqlite_path: &Path) -> Connection {
    Connection::open_with_flags(sqlite_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

#[test]
fn late_join_failure_preserves_delivered_sqlite_rows_and_reopens_at_the_failed_page() {
    const RIGHT_ROWS: u64 = 4_096;
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("join-flow");
    let sqlite_path = root.path().join("join.sqlite");
    build_failing_join_flow(&flow_path, &sqlite_path);

    // Seed the right relation before admitting the driving Claim. Both genuine
    // SequenceScan sources are exhausted; only the published fixture inputs run.
    publish_join_input(&flow_path, 1, 0..RIGHT_ROWS);
    let mut flow = FlowFactory::new(&flow_path).open().unwrap();
    let mut idle = false;
    for _ in 0..128 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            idle = true;
            break;
        }
    }
    assert!(idle, "right-side seeding did not finish");
    drop(flow);
    publish_join_input(&flow_path, 0, [1, 0]);

    // The first row emits enough bounded pages for the real sink to publish a
    // target transaction. The next row deterministically divides by zero.
    let mut flow = FlowFactory::new(&flow_path).open().unwrap();
    let mut failed = false;
    let mut failure_message = String::new();
    for _ in 0..128 {
        let before = flow.status().unwrap();
        if let Err(error) = flow.advance() {
            assert_eq!(error.station_id(), "join");
            let mut source: &(dyn std::error::Error + 'static) = &error;
            while source.downcast_ref::<EquiJoinError>().is_none() {
                source = source
                    .source()
                    .expect("Join failure retains its error chain");
            }
            assert!(matches!(
                source.downcast_ref::<EquiJoinError>(),
                Some(EquiJoinError::ResidualExpression { .. })
            ));
            failure_message = error.to_string();
            let after = flow.status().unwrap();
            assert_eq!(after[2].inputs, before[2].inputs);
            assert_eq!(after[2].output, before[2].output);
            assert_eq!(after[2].active_input, before[2].active_input);
            failed = true;
            break;
        }
    }
    assert!(failed, "the late residual error was not reached");
    let suspended = flow.status().unwrap();
    assert_eq!(suspended[2].active_input, Some(0));
    assert_eq!(suspended[2].inputs[0].position, 0);
    assert_eq!(suspended[2].inputs[0].tail, 1);
    assert!(suspended[2].output.as_ref().unwrap().tail > 1);
    let delivered = sqlite_join_rows(&sqlite_path);
    assert!(!delivered.is_empty(), "earlier output never reached SQLite");
    assert!(delivered.len() <= usize::try_from(RIGHT_ROWS).unwrap());
    assert!(
        delivered
            .iter()
            .all(|&(left, right)| left == 1 && right < RIGHT_ROWS)
    );
    assert_eq!(
        delivered
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        delivered.len(),
        "the target contains duplicate Join output"
    );
    drop(flow);
    let continuation = join_continuation(&flow_path).expect("unfinished Claim retains its cursor");

    for _ in 0..2 {
        let mut reopened = FlowFactory::new(&flow_path).open().unwrap();
        let restored = reopened.status().unwrap();
        assert_eq!(restored[2].inputs, suspended[2].inputs);
        assert_eq!(restored[2].output, suspended[2].output);
        assert_eq!(restored[2].active_input, suspended[2].active_input);
        let error = reopened.advance().unwrap_err();
        assert_eq!(error.station_id(), "join");
        assert_eq!(error.to_string(), failure_message);
        let after = reopened.status().unwrap();
        assert_eq!(after[2].inputs, restored[2].inputs);
        assert_eq!(after[2].output, restored[2].output);
        assert_eq!(after[3].inputs, restored[3].inputs);
        drop(reopened);
        assert_eq!(join_continuation(&flow_path).as_ref(), Some(&continuation));
        assert_eq!(sqlite_join_rows(&sqlite_path), delivered);
    }
}

fn build_failing_join_flow(flow_path: &Path, sqlite_path: &Path) {
    let mut factory = FlowFactory::new(flow_path);
    let left = factory.operation("left", Box::new(SequenceScanDefinition::new(u64::MAX)), []);
    let right = factory.operation("right", Box::new(SequenceScanDefinition::new(u64::MAX)), []);
    let join = factory.operation(
        "join",
        Box::new(
            EquiJoinDefinition::try_new(
                EquiJoinKind::Inner,
                [(lit(0_u64), lit(0_u64))],
                ["left_value", "right_value"],
                Some((lit(1_u64) / col("left.value")).gt(lit(0_u64))),
            )
            .unwrap(),
        ),
        [left, right],
    );
    factory.operation(
        "sqlite",
        Box::new(SqliteSinkDefinition::try_new(sqlite_path, TABLE).unwrap()),
        [join],
    );
    drop(factory.build().unwrap());
}

fn publish_join_input(flow_path: &Path, station: usize, values: impl IntoIterator<Item = u64>) {
    let values = values.into_iter().collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let row_count = values.len();
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1; row_count])).unwrap();
    let encoded = encode_change(&change).unwrap();
    let store = Store::open(flow_path).unwrap();
    let positions = (0..2)
        .map(|index| {
            store
                .open_data::<Cell<u64>>(&format!(
                    "station/{index:08x}/operation/00000000/sequence_scan.position"
                ))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let output: SubscribedLog<Vec<u8>> = store
        .open_data(&format!("station/{station:08x}/output"))
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    for position in positions {
        position
            .access(transaction.access())
            .unwrap()
            .set(&u64::MAX)
            .unwrap();
    }
    assert!(
        output
            .writer()
            .try_append(&encoded, NonZeroU64::MAX, transaction.access())
            .unwrap()
    );
    transaction.commit().unwrap();
}

fn join_continuation(flow_path: &Path) -> Option<Vec<u8>> {
    let store = Store::open(flow_path).unwrap();
    let continuation: Cell<Vec<u8>> = store
        .open_data("station/00000002/operation/00000000/equi_join.continuation")
        .unwrap();
    let transaction = store.read_transaction();
    continuation
        .read(transaction.access())
        .unwrap()
        .get()
        .unwrap()
}

fn sqlite_join_rows(path: &Path) -> Vec<(u64, u64)> {
    sqlite_connection(path)
        .prepare("SELECT left_value, right_value FROM events ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| {
            Ok((decode_u64_blob(row.get(0)?), decode_u64_blob(row.get(1)?)))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
