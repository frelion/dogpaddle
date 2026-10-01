use std::num::NonZeroU32;

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition},
        transform::{
            EquiJoinDefinition, EquiJoinKind, FilterDefinition, RunningEventCountDefinition,
            SelectDefinition, SelectField, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store};
use rusqlite::{Connection, OpenFlags};

#[test]
fn five_atomic_transforms_run_in_one_fused_tail_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let start = u64::MAX - 2;

    let mut factory = FlowFactory::new(&flow_path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(start), []);
    let scan = factory.operation(
        "scan/tail-1",
        SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-2",
        SelectDefinition::try_new([
            ("value", col("value")),
            ("offset", col("value") - lit(start)),
        ])
        .unwrap(),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-3",
        FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap(),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-4",
        SelectDefinition::try_new([("scan_value", col("value")), ("offset", col("offset"))])
            .unwrap(),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-5",
        SelectDefinition::try_new([
            SelectField {
                name: "scan_value".into(),
                expression: col("scan_value"),
                nullable: Some(false),
                metadata: Some(arrow_schema::Metadata::new()),
            },
            SelectField {
                name: "offset".into(),
                expression: col("offset"),
                nullable: Some(false),
                metadata: Some(arrow_schema::Metadata::new()),
            },
        ])
        .unwrap()
        .with_metadata(arrow_schema::Metadata::new()),
        [scan],
    );
    factory.operation(
        "sqlite",
        SqliteSinkDefinition::try_new(&sqlite_path, "events").unwrap(),
        [scan],
    );

    let mut flow = factory.build().unwrap();
    assert_eq!(flow.operation_count(), 7);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&flow_path).open().unwrap();
    assert_eq!(flow.operation_count(), 7);
    run_until_idle(&mut flow);
    drop(flow);

    let connection =
        Connection::open_with_flags(&sqlite_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows = connection
        .prepare(
            "SELECT \"$dogpaddle.id\", scan_value, offset FROM events ORDER BY \"$dogpaddle.id\"",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                decode_u64(row.get(1)?),
                decode_u64(row.get(2)?),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        [(i64::MIN + 1, u64::MAX - 1, 1), (i64::MIN + 2, u64::MAX, 2)]
    );

    let store = Store::open(&flow_path).unwrap();
    let _: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let _: Cell<Vec<u8>> = store.open_data("operation/00000006/sink.control").unwrap();
    let _: OrderedMap<u64, Vec<u8>> = store.open_data("operation/00000006/sink.buffer").unwrap();
}

#[test]
fn an_empty_intermediate_result_commits_prior_state_and_skips_the_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let compute = factory.operation("compute", SequenceScanDefinition::new(u64::MAX - 1), []);
    let compute = factory.operation(
        "compute/tail-1",
        FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap(),
        [compute],
    );
    let compute = factory.operation(
        "compute/tail-2",
        RunningEventCountDefinition::new(),
        [compute],
    );
    factory.operation("sink", DiscardDefinition::new(), [compute]);

    let mut flow = factory.build().unwrap();
    run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
        .unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        position.read(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        count.read(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
    drop(transaction);
    drop(store);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn a_late_stateful_failure_rolls_back_the_entire_computation_page() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("rollback");
    let mut factory = FlowFactory::new(&path);
    let compute = factory.operation("compute", SequenceScanDefinition::new(u64::MAX), []);
    let compute = factory.operation(
        "compute/tail-1",
        RunningEventCountDefinition::new(),
        [compute],
    );
    let compute = factory.operation(
        "compute/tail-2",
        RunningEventCountDefinition::new(),
        [compute],
    );
    factory.operation("sink", DiscardDefinition::new(), [compute]);

    drop(factory.build().unwrap());

    let store = Store::open(&path).unwrap();
    let second: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    second
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    let error = flow.advance().unwrap_err();
    assert_eq!(error.operation_id(), "compute");
    assert!(!error.requires_reopen());
    drop(flow);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let first: Cell<u64> = store
        .open_data("operation/00000001/running_event_count.count")
        .unwrap();
    let second: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
        .unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        position.read(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        first.read(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        second.read(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
}

#[test]
fn a_multi_input_head_preserves_ports_before_its_atomic_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let left = factory.operation("left", SequenceScanDefinition::new(u64::MAX - 1), []);
    let right = factory.operation("right", SequenceScanDefinition::new(u64::MAX), []);
    let union = factory.operation(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
        [left, right],
    );
    let union = factory.operation(
        "union/tail-1",
        FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap(),
        [union],
    );
    let count = factory.operation("count", RunningEventCountDefinition::new(), [union]);
    factory.operation("sink", DiscardDefinition::new(), [count]);

    let mut flow = factory.build().unwrap();
    run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000004/running_event_count.count")
        .unwrap();
    assert_eq!(
        count
            .read(store.read_transaction().access())
            .unwrap()
            .get()
            .unwrap(),
        Some(2)
    );
}

#[test]
fn binding_failure_reports_the_logical_operation_id_without_creating_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let scan = factory.operation(
        "scan/tail-1",
        SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-2",
        SelectDefinition::try_new([("missing", dogpaddle_operation::col("other"))]).unwrap(),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [scan]);

    let Err(FlowError::Schema {
        operation_id,
        source: _,
    }) = factory.build()
    else {
        panic!("invalid intermediate Schema unexpectedly built");
    };
    assert_eq!(operation_id, "scan/tail-2");
    assert!(!path.exists());
}

#[test]
fn automatic_planning_keeps_a_paged_transform_as_head_with_an_atomic_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let left = factory.operation("left", SequenceScanDefinition::new(0), []);
    let right = factory.operation("right", SequenceScanDefinition::new(0), []);
    let join_definition = || {
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("value"), col("value"))],
            ["left_value", "right_value"],
            None,
        )
        .unwrap()
    };

    let join = factory.operation("join", join_definition(), [left, right]);
    let join = factory.operation("join/tail-2", RunningEventCountDefinition::new(), [join]);
    factory.operation("sink", DiscardDefinition::new(), [join]);

    let mut flow = factory.build().unwrap();
    assert_eq!(
        flow.operation_ids().collect::<Vec<_>>(),
        ["left", "right", "join", "join/tail-2", "sink"]
    );
    for _ in 0..8 {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(flow);

    let before_reopen = {
        let store = Store::open(&path).unwrap();
        let count: Cell<u64> = store
            .open_data("operation/00000003/running_event_count.count")
            .unwrap();
        let transaction = store.read_transaction();
        count
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap()
    };
    assert!(before_reopen > 0);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(
        reopened.operation_ids().collect::<Vec<_>>(),
        ["left", "right", "join", "join/tail-2", "sink"]
    );
    for _ in 0..4 {
        assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(reopened);

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000003/running_event_count.count")
        .unwrap();
    let transaction = store.read_transaction();
    assert!(
        count
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap()
            > before_reopen
    );
}

#[test]
fn nested_join_output_shrinks_pages_and_completes_across_reopen() {
    use arrow_schema::DataType;
    use dogpaddle_operation::ScalarValue;

    for residual in [None, Some(lit(true))] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("flow");
        let mut factory = FlowFactory::new(&path);
        let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX - 15), []);
        let left = factory.operation(
            "left/row",
            SelectDefinition::try_new([
                ("key", lit(1_u64)),
                ("id", col("value")),
                (
                    "items",
                    lit(ScalarValue::List(ScalarValue::new_list(
                        &vec![ScalarValue::Null; 8192],
                        &DataType::Null,
                        true,
                    ))),
                ),
            ])
            .unwrap(),
            [source],
        );
        let right = factory.operation(
            "right/last",
            FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap(),
            [source],
        );
        let right = factory.operation(
            "right/row",
            SelectDefinition::try_new([
                ("key", lit(1_u64)),
                ("id", col("value")),
                (
                    "items",
                    lit(ScalarValue::List(ScalarValue::new_list(
                        &[],
                        &DataType::Null,
                        true,
                    ))),
                ),
            ])
            .unwrap(),
            [right],
        );
        let join = factory.operation(
            "join",
            EquiJoinDefinition::try_new(
                EquiJoinKind::Inner,
                [(col("key"), col("key"))],
                [
                    "left_key",
                    "left_id",
                    "left_items",
                    "right_key",
                    "right_id",
                    "right_items",
                ],
                residual,
            )
            .unwrap(),
            [left, right],
        );
        let count = factory.operation("count", RunningEventCountDefinition::new(), [join]);
        factory.operation("sink", DiscardDefinition::new(), [count]);
        drop(factory.build().unwrap());

        let mut idle = 0;
        for _ in 0..100 {
            let mut flow = FlowFactory::new(&path).open().unwrap();
            if flow.advance().unwrap() == AdvanceOutcome::Idle {
                idle += 1;
            } else {
                idle = 0;
            }
            if idle > flow.operation_count() {
                break;
            }
        }
        assert!(idle > 7, "finite nested Join must drain");
        let store = Store::open(&path).unwrap();
        let count: Cell<u64> = store
            .open_data("operation/00000005/running_event_count.count")
            .unwrap();
        assert_eq!(
            count
                .read(store.read_transaction().access())
                .unwrap()
                .get()
                .unwrap(),
            Some(16)
        );
    }
}

fn run_until_idle(flow: &mut dogpaddle_flow::Flow) {
    super::support::run_until_idle(flow);
}

fn decode_u64(value: Vec<u8>) -> u64 {
    u64::from_be_bytes(value.try_into().unwrap())
}
