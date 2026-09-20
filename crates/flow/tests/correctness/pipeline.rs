use std::num::{NonZeroU32, NonZeroU64};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory, FlowSchemaError};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition},
        transform::{
            EquiJoinDefinition, EquiJoinKind, ExtendDefinition, FilterDefinition,
            ProjectDefinition, RunningEventCountDefinition, SchemaAlignDefinition,
            SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::{Cell, OrderedMap, Store, SubscribedLog};
use rusqlite::{Connection, OpenFlags};

const CAPACITY: NonZeroU64 = NonZeroU64::MAX;

#[test]
fn five_atomic_transforms_run_in_one_station_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let start = u64::MAX - 2;

    let mut factory = FlowFactory::new(&flow_path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(start)), []);
    let scan = factory.operation("scan/tail-1", Box::new(ProjectDefinition::new([0])), [scan]);
    let scan = factory.operation(
        "scan/tail-2",
        Box::new(ExtendDefinition::try_new("offset", col("value") - lit(start)).unwrap()),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-3",
        Box::new(FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap()),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-4",
        Box::new(
            SelectDefinition::try_new([("scan_value", col("value")), ("offset", col("offset"))])
                .unwrap(),
        ),
        [scan],
    );
    let scan = factory.operation(
        "scan/tail-5",
        Box::new(
            SchemaAlignDefinition::try_new([
                SchemaAlignField::try_new("scan_value", col("scan_value"), false).unwrap(),
                SchemaAlignField::try_new("offset", col("offset"), false).unwrap(),
            ])
            .unwrap(),
        ),
        [scan],
    );
    factory.operation(
        "sqlite",
        Box::new(SqliteSinkDefinition::try_new(&sqlite_path, "events").unwrap()),
        [scan],
    );
    factory.materialize(scan, CAPACITY);

    let mut flow = factory.build().unwrap();
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["scan", "sqlite"]);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&flow_path).open().unwrap();
    assert_eq!(flow.station_count(), 2);
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
    assert_eq!(rows, [(1, u64::MAX - 1, 1), (2, u64::MAX, 2)]);

    let store = Store::open(&flow_path).unwrap();
    let _: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let _: Cell<Vec<u8>> = store
        .open_data("station/00000001/operation/00000000/sink.control")
        .unwrap();
    let _: OrderedMap<u64, Vec<u8>> = store
        .open_data("station/00000001/operation/00000000/sink.buffer")
        .unwrap();
    let transaction = store.read_transaction();
    let status = output.writer().status(transaction.access()).unwrap();
    assert_eq!((status.head, status.tail), (2, 2));
}

#[test]
fn an_empty_intermediate_result_commits_prior_state_and_skips_the_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let compute = factory.operation(
        "compute",
        Box::new(SequenceScanDefinition::new(u64::MAX - 1)),
        [],
    );
    let compute = factory.operation(
        "compute/tail-1",
        Box::new(FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap()),
        [compute],
    );
    let compute = factory.operation(
        "compute/tail-2",
        Box::new(RunningEventCountDefinition::new()),
        [compute],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [compute]);
    factory.materialize(compute, CAPACITY);

    let mut flow = factory.build().unwrap();
    run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000000/operation/00000002/running_event_count.count")
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
        1
    );
    drop(transaction);
    drop(store);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn a_late_stateful_failure_rolls_back_the_entire_station_program() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("rollback");
    let mut factory = FlowFactory::new(&path);
    let compute = factory.operation(
        "compute",
        Box::new(SequenceScanDefinition::new(u64::MAX)),
        [],
    );
    let compute = factory.operation(
        "compute/tail-1",
        Box::new(RunningEventCountDefinition::new()),
        [compute],
    );
    let compute = factory.operation(
        "compute/tail-2",
        Box::new(RunningEventCountDefinition::new()),
        [compute],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [compute]);
    factory.materialize(compute, CAPACITY);

    drop(factory.build().unwrap());

    let store = Store::open(&path).unwrap();
    let second: Cell<u64> = store
        .open_data("station/00000000/operation/00000002/running_event_count.count")
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
    assert_eq!(error.station_id(), "compute");
    assert!(!error.requires_reopen());
    drop(flow);

    let store = Store::open(&path).unwrap();
    let position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let first: Cell<u64> = store
        .open_data("station/00000000/operation/00000001/running_event_count.count")
        .unwrap();
    let second: Cell<u64> = store
        .open_data("station/00000000/operation/00000002/running_event_count.count")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        position.read(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        first.read(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        second.read(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        output.writer().status(transaction.access()).unwrap().tail,
        0
    );
}

#[test]
fn a_multi_input_head_preserves_ports_before_its_atomic_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let left = factory.operation(
        "left",
        Box::new(SequenceScanDefinition::new(u64::MAX - 1)),
        [],
    );
    let right = factory.operation("right", Box::new(SequenceScanDefinition::new(u64::MAX)), []);
    let union = factory.operation(
        "union",
        Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        [left, right],
    );
    let union = factory.operation(
        "union/tail-1",
        Box::new(FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap()),
        [union],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [union]);
    for station in [left, right, union] {
        factory.materialize(station, CAPACITY);
    }

    let mut flow = factory.build().unwrap();
    run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let active: Cell<u32> = store.open_data("station/00000002/active-input").unwrap();
    let left_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let right_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let union_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        active.read(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
    assert_eq!(
        left_output
            .subscription(0)
            .status(transaction.access())
            .unwrap()
            .position,
        2
    );
    assert_eq!(
        right_output
            .subscription(0)
            .status(transaction.access())
            .unwrap()
            .position,
        1
    );
    assert_eq!(
        union_output
            .writer()
            .status(transaction.access())
            .unwrap()
            .tail,
        2
    );
}

#[test]
fn binding_failure_reports_the_operation_ordinal_without_creating_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    let scan = factory.operation("scan/tail-1", Box::new(ProjectDefinition::new([0])), [scan]);
    let scan = factory.operation("scan/tail-2", Box::new(ProjectDefinition::new([1])), [scan]);
    factory.operation("sink", Box::new(DiscardDefinition::new()), [scan]);
    factory.materialize(scan, CAPACITY);

    let Err(FlowError::Schema(FlowSchemaError::Operation {
        station_id,
        operation,
        source: _,
    })) = factory.build()
    else {
        panic!("invalid intermediate Schema unexpectedly built");
    };
    assert_eq!((station_id.as_str(), operation), ("scan", 2));
    assert!(!path.exists());
}

#[test]
fn automatic_planning_keeps_a_turn_transform_as_head_with_an_atomic_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let left = factory.operation("left", Box::new(SequenceScanDefinition::new(0)), []);
    let right = factory.operation("right", Box::new(SequenceScanDefinition::new(0)), []);
    let join_definition = || {
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("value"), col("value"))],
            ["left_value", "right_value"],
            None,
        )
        .unwrap()
    };

    let join = factory.operation("join", Box::new(join_definition()), [left, right]);
    let join = factory.operation(
        "join/tail-2",
        Box::new(RunningEventCountDefinition::new()),
        [join],
    );
    factory.operation("sink", Box::new(DiscardDefinition::new()), [join]);
    for station in [left, right, join] {
        factory.materialize(station, CAPACITY);
    }

    let mut flow = factory.build().unwrap();
    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        ["left", "right", "join", "sink"]
    );
    for _ in 0..8 {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(flow);

    let before_reopen = {
        let store = Store::open(&path).unwrap();
        let count: Cell<u64> = store
            .open_data("station/00000002/operation/00000001/running_event_count.count")
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
        reopened.station_ids().collect::<Vec<_>>(),
        ["left", "right", "join", "sink"]
    );
    for _ in 0..4 {
        assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(reopened);

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000002/operation/00000001/running_event_count.count")
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

fn run_until_idle(flow: &mut dogpaddle_flow::Flow) {
    for _ in 0..64 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            return;
        }
    }
    panic!("Flow did not become idle within its bounded fixture");
}

fn decode_u64(value: Vec<u8>) -> u64 {
    u64::from_be_bytes(value.try_into().unwrap())
}
