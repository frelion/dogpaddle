use std::{num::NonZeroU64, ops::Range, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::{
    col, lit,
    operation::{
        sink::SqliteSinkDefinition,
        source::SequenceSourceDefinition,
        transform::{ExtendDefinition, FilterDefinition, SelectDefinition},
    },
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, ScanLimit, Small, Store, StoreError};
use rusqlite::{Connection, OpenFlags};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::MAX;
const TABLE: &str = "events";
const CURSOR: &[u8] = b"input/00000000/cursor";

#[test]
fn transform_chain_materializes_filtered_rows_through_the_public_flow_api() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let source_start = u64::MAX - 2;

    // SequenceSource becomes idle after u64::MAX, so this emits exactly three rows.
    let mut factory = FlowFactory::new(&flow_path);
    let source = factory.station("source", SequenceSourceDefinition::new(source_start));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("offset", col("value") - lit(source_start)).unwrap(),
    );
    let filter = factory.station(
        "filter",
        FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap(),
    );
    let select = factory.station(
        "select",
        SelectDefinition::try_new([("source_value", col("value")), ("offset", col("offset"))])
            .unwrap(),
    );
    let sqlite = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(&sqlite_path, TABLE).unwrap(),
    );
    for station in [source, extend, filter, select] {
        factory.output_capacity_bytes(station, OUTPUT_CAPACITY_BYTES);
    }
    factory.connect([source], extend);
    factory.connect([extend], filter);
    factory.connect([filter], select);
    factory.connect([select], sqlite);
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
    println!("SQLite rows (technical_id, source_value, offset, hash_bytes): {rows:?}");
}

#[test]
fn sqlite_sink_retains_one_change_until_all_1025_mutations_complete_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    drop(build_sqlite_flow(&flow_path, &sqlite_path));
    assert!(
        !sqlite_path.exists(),
        "Flow build eagerly created the SQLite database"
    );
    drop(FlowFactory::new(&flow_path).open().unwrap());
    assert!(
        !sqlite_path.exists(),
        "Flow open eagerly created the SQLite database"
    );

    let encoded_change = encode_change(&multiplicity_change(7, 1_025)).unwrap();
    publish_source_change(&flow_path, &encoded_change);

    for round in 1..=6 {
        let mut flow = FlowFactory::new(&flow_path).open().unwrap();
        assert_eq!(
            flow.advance().unwrap(),
            AdvanceOutcome::Progressed,
            "round {round}"
        );
        drop(flow);

        if round == 1 {
            assert!(sqlite_path.is_file());
            assert_eq!(sqlite_object_count(&sqlite_path), 0);
        }
        if round == 2 {
            assert_eq!(sqlite_object_count(&sqlite_path), 2);
            assert_eq!(sqlite_row_count(&sqlite_path), 0);
        }

        let snapshot = sink_snapshot(&flow_path);
        if round < 6 {
            assert_eq!(snapshot.cursor, Some(0), "round {round}");
            assert_eq!(snapshot.output_bounds, 0..1, "round {round}");
            assert_eq!(
                snapshot.encoded_entry.as_deref(),
                Some(encoded_change.as_slice()),
                "round {round} did not retain the complete input Change"
            );
        } else {
            assert_eq!(snapshot.cursor, Some(1));
            assert_eq!(snapshot.output_bounds, 1..1);
            assert_eq!(snapshot.encoded_entry, None);
        }

        let expected = match round {
            1 => (None, true, None),
            2 => (Some(1), false, Some(0)),
            3 => (Some(1_025), true, Some(0)),
            4 => (Some(1_025), true, Some(1_024)),
            5 => (Some(1_026), true, Some(1_024)),
            6 => (Some(1_026), false, Some(1_025)),
            _ => unreachable!(),
        };
        assert_eq!(
            (
                snapshot.next_id,
                snapshot.has_pending,
                sqlite_rows(&sqlite_path)
            ),
            expected,
            "round {round}"
        );
    }
}

fn build_sqlite_flow(flow_path: &Path, sqlite_path: &Path) -> dogpaddle_flow::Flow {
    let mut factory = FlowFactory::new(flow_path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX));
    let sink = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(sqlite_path, TABLE).unwrap(),
    );
    factory.output_capacity_bytes(source, OUTPUT_CAPACITY_BYTES);
    factory.connect([source], sink);
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

fn publish_source_change(flow_path: &Path, encoded_change: &[u8]) {
    let store = Store::open(flow_path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert_eq!(
        output
            .access(transaction.access())
            .unwrap()
            .append(&encoded_change.to_vec())
            .unwrap(),
        0
    );
    transaction.commit().unwrap();
}

#[derive(Debug)]
struct SinkSnapshot {
    cursor: Option<u64>,
    output_bounds: Range<u64>,
    encoded_entry: Option<Vec<u8>>,
    next_id: Option<u64>,
    has_pending: bool,
}

fn sink_snapshot(flow_path: &Path) -> SinkSnapshot {
    let store = Store::open(flow_path).unwrap();
    let output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let next_id: Cell<u64> = store
        .open_data("station/00000001/operation/sqlite_sink.next_id")
        .unwrap();
    let pending: Cell<Vec<u8>> = store
        .open_data("station/00000001/operation/sqlite_sink.pending")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    let cursor = state
        .access(access)
        .unwrap()
        .get(&CURSOR.to_vec())
        .unwrap()
        .map(|encoded| u64::from_be_bytes(encoded.try_into().unwrap()));
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
    SinkSnapshot {
        cursor,
        output_bounds,
        encoded_entry,
        next_id: next_id.access(access).unwrap().get().unwrap(),
        has_pending: pending.access(access).unwrap().get().unwrap().is_some(),
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
            "SELECT \"$dogpaddle.id\", \"source_value\", \"offset\", \
                    length(\"$dogpaddle.hash\") \
             FROM \"events\" ORDER BY \"$dogpaddle.id\"",
        )
        .unwrap();
    statement
        .query_map([], |row| {
            let source_value: Vec<u8> = row.get(1)?;
            let offset: Vec<u8> = row.get(2)?;
            Ok((
                row.get(0)?,
                decode_u64_blob(source_value),
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
