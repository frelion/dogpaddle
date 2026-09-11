use std::num::{NonZeroU32, NonZeroU64};

use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory, FlowSchemaError};
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition},
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, RunningEventCountDefinition,
            SchemaAlignDefinition, SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::{Cell, Store, SubscribedLog};
use rusqlite::{Connection, OpenFlags};

const CAPACITY: NonZeroU64 = NonZeroU64::MAX;

#[test]
fn output_pipeline_runs_five_pure_stages_in_one_station_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let start = u64::MAX - 2;

    let mut factory = FlowFactory::new(&flow_path);
    let scan = factory.station("scan", SequenceScanDefinition::new(start));
    let sink = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(&sqlite_path, "events").unwrap(),
    );
    factory.output_capacity_bytes(scan, CAPACITY);
    factory.connect([scan], sink);
    factory
        .inline_output(scan, ProjectDefinition::new([0]))
        .unwrap()
        .inline_output(
            scan,
            ExtendDefinition::try_new("offset", col("value") - lit(start)).unwrap(),
        )
        .unwrap()
        .inline_output(
            scan,
            FilterDefinition::try_new(col("offset").gt(lit(0_u64))).unwrap(),
        )
        .unwrap()
        .inline_output(
            scan,
            SelectDefinition::try_new([("scan_value", col("value")), ("offset", col("offset"))])
                .unwrap(),
        )
        .unwrap()
        .inline_output(
            scan,
            SchemaAlignDefinition::try_new([
                SchemaAlignField::try_new("scan_value", col("scan_value"), false).unwrap(),
                SchemaAlignField::try_new("offset", col("offset"), false).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();

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
        .open_data("station/00000000/operation/sequence_scan.position")
        .unwrap();
    let output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let _: Cell<Vec<u8>> = store
        .open_data("station/00000001/operation/relation_sink.state")
        .unwrap();
    let transaction = store.read_transaction();
    let status = output.writer().status(transaction.access()).unwrap();
    assert_eq!((status.head, status.tail), (2, 2));
}

#[test]
fn dropped_input_commits_only_ack_before_the_stateful_core_sees_a_later_change() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(u64::MAX - 1));
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, CAPACITY);
    factory.output_capacity_bytes(count, CAPACITY);
    factory.connect([scan], count);
    factory.connect([count], sink);
    factory
        .inline_input(
            count,
            0,
            FilterDefinition::try_new(col("value").eq(lit(u64::MAX))).unwrap(),
        )
        .unwrap();

    let mut flow = factory.build().unwrap();
    run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000001/operation/running_event_count.count")
        .unwrap();
    let scan_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let count_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        count.read(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
    assert_eq!(
        scan_output
            .subscription(0)
            .status(transaction.access())
            .unwrap()
            .position,
        2
    );
    assert_eq!(
        count_output
            .writer()
            .status(transaction.access())
            .unwrap()
            .tail,
        1
    );
    drop(transaction);
    drop(store);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn dropped_multi_input_rotates_the_durable_active_port() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let left = factory.station("left", SequenceScanDefinition::new(u64::MAX));
    let right = factory.station("right", SequenceScanDefinition::new(u64::MAX));
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left, right, union] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([left, right], union);
    factory.connect([union], sink);
    factory
        .inline_input(union, 0, FilterDefinition::try_new(lit(false)).unwrap())
        .unwrap();

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
        Some(0)
    );
    assert_eq!(
        left_output
            .subscription(0)
            .status(transaction.access())
            .unwrap()
            .position,
        1
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
        1
    );
    drop(transaction);
    drop(store);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn inline_binding_errors_report_direction_port_and_stage_without_creating_store() {
    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("input");
    let mut input = FlowFactory::new(&input_path);
    let scan = input.station("scan", SequenceScanDefinition::new(0));
    let sink = input.station("sink", DiscardDefinition::new());
    input.output_capacity_bytes(scan, CAPACITY);
    input.connect([scan], sink);
    input
        .inline_input(sink, 0, ProjectDefinition::new([1]))
        .unwrap();
    let Err(FlowError::Schema(FlowSchemaError::InlineInput {
        station_id,
        port,
        stage,
        source: _,
    })) = input.build()
    else {
        panic!("invalid input inline Schema unexpectedly built");
    };
    assert_eq!((station_id.as_str(), port, stage), ("sink", 0, 0));
    assert!(!input_path.exists());

    let output_path = root.path().join("output");
    let mut output = FlowFactory::new(&output_path);
    let scan = output.station("scan", SequenceScanDefinition::new(0));
    let sink = output.station("sink", DiscardDefinition::new());
    output.output_capacity_bytes(scan, CAPACITY);
    output.connect([scan], sink);
    output
        .inline_output(scan, ProjectDefinition::new([0]))
        .unwrap()
        .inline_output(scan, ProjectDefinition::new([1]))
        .unwrap();
    let Err(FlowError::Schema(FlowSchemaError::InlineOutput {
        station_id,
        stage,
        source: _,
    })) = output.build()
    else {
        panic!("invalid output inline Schema unexpectedly built");
    };
    assert_eq!((station_id.as_str(), stage), ("scan", 1));
    assert!(!output_path.exists());
}

#[test]
fn inline_topology_rejects_invalid_ports_and_sink_outputs_without_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let input_path = root.path().join("port");
    let mut input = FlowFactory::new(&input_path);
    let scan = input.station("scan", SequenceScanDefinition::new(0));
    let sink = input.station("sink", DiscardDefinition::new());
    input.output_capacity_bytes(scan, CAPACITY);
    input.connect([scan], sink);
    input
        .inline_input(sink, 1, ProjectDefinition::new([0]))
        .unwrap();
    assert!(matches!(
        input.build(),
        Err(FlowError::Topology(
            dogpaddle_flow::TopologyError::InlineInputPortOutOfRange {
                station,
                port: 1,
                input_count: 1,
            }
        )) if station == "sink"
    ));
    assert!(!input_path.exists());

    let output_path = root.path().join("sink-output");
    let mut output = FlowFactory::new(&output_path);
    let scan = output.station("scan", SequenceScanDefinition::new(0));
    let sink = output.station("sink", DiscardDefinition::new());
    output.output_capacity_bytes(scan, CAPACITY);
    output.connect([scan], sink);
    output
        .inline_output(sink, ProjectDefinition::new([0]))
        .unwrap();
    assert!(matches!(
        output.build(),
        Err(FlowError::Topology(
            dogpaddle_flow::TopologyError::InlineOutputOnSink(station)
        )) if station == "sink"
    ));
    assert!(!output_path.exists());
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
