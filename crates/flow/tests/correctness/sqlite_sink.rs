use super::support::{run_until_idle, seed_source, values_change};
use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::SqliteSinkDefinition,
        transform::{EquiJoinDefinition, EquiJoinKind, FilterDefinition, SelectDefinition},
    },
};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
const TABLE: &str = "events";

#[test]
fn transform_chain_materializes_filtered_rows_through_the_public_flow_api() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let scan_start = u64::MAX - 2;

    // SequenceScan becomes idle after u64::MAX, so this emits exactly three rows.
    let mut factory = FlowFactory::new(&flow_path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(scan_start), []);
    let extend = factory.operation(
        "extend",
        SelectDefinition::try_new([
            ("value", col("value")),
            ("offset", col("value") - lit(scan_start)),
        ])
        .unwrap(),
        [scan],
    );
    let filter = factory.operation(
        "filter",
        FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap(),
        [extend],
    );
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([("scan_value", col("value")), ("offset", col("offset"))])
            .unwrap(),
        [filter],
    );
    factory.operation(
        "sqlite",
        SqliteSinkDefinition::try_new(&sqlite_path, TABLE).unwrap(),
        [select],
    );

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
    assert_eq!(
        rows,
        [
            (i64::MIN + 1, u64::MAX - 1, 1, 16),
            (i64::MIN + 2, u64::MAX, 2, 16)
        ]
    );
    println!("advance outcomes: {outcomes:?}");
    println!("SQLite rows (technical_id, scan_value, offset, hash_bytes): {rows:?}");
}

#[test]
fn a_sink_batch_settles_and_reopens_without_reusing_technical_ids() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("target.sqlite");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, TABLE).unwrap(),
        [source],
    );
    drop(factory.build().unwrap());
    seed_source(&path, 0, &values_change([7], 1025));
    let mut flow = FlowFactory::new(&path).open().unwrap();
    for _ in 0..4 {
        flow.advance().unwrap();
        if target.exists()
            && sqlite_connection(&target)
                .query_row("SELECT count(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
                == 1024
        {
            break;
        }
    }
    drop(flow);
    assert_eq!(
        sqlite_connection(&target)
            .query_row("SELECT count(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1024
    );
    let mut flow = FlowFactory::new(&path).open().unwrap();
    run_until_idle(&mut flow);
    drop(flow);
    let rows = sqlite_connection(&target)
        .prepare("SELECT \"$dogpaddle.id\",value FROM events ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, decode_u64_blob(row.get(1)?)))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        (1..=1025).map(|id| (i64::MIN + id, 7)).collect::<Vec<_>>()
    );
    for _ in 0..2 {
        let mut flow = FlowFactory::new(&path).open().unwrap();
        run_until_idle(&mut flow);
    }
    assert_eq!(
        sqlite_connection(&target)
            .query_row("SELECT count(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1025
    );
}

#[test]
fn a_late_join_error_preserves_delivered_pages_and_the_failed_position_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("target.sqlite");
    let mut factory = FlowFactory::new(&path);
    let right = factory.operation("right", SequenceScanDefinition::new(u64::MAX), []);
    let left = factory.operation("left", SequenceScanDefinition::new(u64::MAX), []);
    let join = factory.operation(
        "join",
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(lit(0_u64), lit(0_u64))],
            ["left_value", "right_value"],
            Some((lit(1_u64) / col("left.value")).gt_eq(lit(0_u64))),
        )
        .unwrap(),
        [left, right],
    );
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, TABLE).unwrap(),
        [join],
    );
    drop(factory.build().unwrap());
    seed_source(&path, 0, &values_change(1..=128, 1));
    seed_source(&path, 1, &values_change((1..=512).chain([0]), 1));
    let mut flow = FlowFactory::new(&path).open().unwrap();
    let message = (0..2000)
        .find_map(|_| {
            flow.advance().err().map(|error| {
                assert!(!error.requires_reopen());
                error.to_string()
            })
        })
        .expect("last invalid event must fail");
    let status = flow.status().unwrap();
    assert!(status.depth > 0);
    drop(flow);
    let rows = sqlite_join_rows(&target);
    assert!(!rows.is_empty());
    assert!(rows.len() <= 512 * 128);
    assert_eq!(
        rows.iter().collect::<std::collections::BTreeSet<_>>().len(),
        rows.len()
    );
    for _ in 0..2 {
        let mut flow = FlowFactory::new(&path).open().unwrap();
        assert_eq!(flow.status().unwrap(), status);
        assert_eq!(flow.advance().unwrap_err().to_string(), message);
        drop(flow);
        assert_eq!(sqlite_join_rows(&target), rows);
    }
}
fn sqlite_join_rows(path: &Path) -> Vec<(u64, u64)> {
    sqlite_connection(path)
        .prepare("SELECT left_value,right_value FROM events ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| {
            Ok((decode_u64_blob(row.get(0)?), decode_u64_blob(row.get(1)?)))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
fn sqlite_connection(path: &Path) -> Connection {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
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

#[test]
fn target_draining_follows_declaration_order_across_unequal_depths_and_reopen() {
    use dogpaddle_operation::operation::transform::RunningEventCountDefinition;

    let root = tempfile::tempdir().unwrap();
    for reopen in [false, true] {
        let path = root.path().join(format!("flow-{reopen}"));
        let first_target = root.path().join(format!("first-{reopen}.sqlite"));
        let second_target = root.path().join(format!("second-{reopen}.sqlite"));
        let mut factory = FlowFactory::new(&path);
        let first = factory.operation("first-source", SequenceScanDefinition::new(u64::MAX), []);
        let counted = factory.operation("count", RunningEventCountDefinition::new(), [first]);
        factory.operation(
            "first-sink",
            SqliteSinkDefinition::try_new(&first_target, TABLE).unwrap(),
            [counted],
        );
        let second = factory.operation("second-source", SequenceScanDefinition::new(u64::MAX), []);
        factory.operation(
            "second-sink",
            SqliteSinkDefinition::try_new(&second_target, TABLE).unwrap(),
            [second],
        );
        let mut flow = factory.build().unwrap();
        if reopen {
            drop(flow);
            flow = FlowFactory::new(&path).open().unwrap();
        }
        assert!(!first_target.exists());
        assert!(!second_target.exists());
        flow.advance().unwrap();
        // The first declared sink is deeper than the second. Its external
        // initialization still comes first, without a separate breadth-first order.
        assert!(first_target.exists());
        assert!(!second_target.exists());
        flow.advance().unwrap();
        assert!(second_target.exists());
        run_until_idle(&mut flow);
        drop(flow);
        for target in [&first_target, &second_target] {
            let connection =
                Connection::open_with_flags(target, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM events", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                1,
            );
        }
    }
}
