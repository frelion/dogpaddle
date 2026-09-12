use std::num::{NonZeroU32, NonZeroU64};

use arrow_schema::{DataType, TimeUnit};
use dogpaddle_change::ProjectionError;
use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory, FlowSchemaError};
use dogpaddle_operation::{
    OperationBindError, ScalarValue, cast, col, encode_definition, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition, SqliteSinkSchemaError},
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, ProjectSchemaError,
            RunningEventCountDefinition, SchemaAlignDefinition, SchemaAlignField, SelectDefinition,
            UnionAllDefinition, UnionAllSchemaError,
        },
    },
};
use dogpaddle_store::{Cell, Store, StoreError, SubscribedLog};

use super::support::{read_published_definition, rewrite_checksum};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024 * 1_024).unwrap();

#[test]
fn build_reports_the_exact_project_schema_rejection_without_creating_a_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([1]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([scan], project);
    factory.connect([project], sink);

    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("schema-incompatible Flow did not return FlowError::Schema");
    };
    assert_project_field_rejection(&error);
    assert!(!path.exists(), "Schema rejection created the Store path");
}

#[test]
fn build_reports_a_multi_input_schema_rejection_without_store_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let union_path = root.path().join("union");
    let mut factory = FlowFactory::new(&union_path);
    let left = factory.station("left", SequenceScanDefinition::new(0));
    let right_scan = factory.station("right-scan", SequenceScanDefinition::new(0));
    let right = factory.station("right", RunningEventCountDefinition::new());
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left, right_scan, right, union] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([right_scan], right);
    factory.connect([left, right], union);
    factory.connect([union], sink);

    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("schema-incompatible UnionAll Flow unexpectedly built");
    };
    assert_union_schema_mismatch(&error, 1);
    assert!(
        !union_path.exists(),
        "UnionAll rejection created the Store path"
    );
}

#[test]
fn build_reports_sqlite_identifier_collisions_without_creating_either_database() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let select =
        SelectDefinition::try_new([("Name", col("value")), ("name", col("value"))]).unwrap();

    let Err(FlowError::Schema(error)) = build_select_sqlite_flow(&flow_path, &sqlite_path, select)
    else {
        panic!("SQLite-incompatible output Schema unexpectedly built");
    };
    assert_sqlite_identifier_collision(&error);
    assert!(
        !flow_path.exists(),
        "Schema rejection created the Store path"
    );
    assert!(
        !sqlite_path.exists(),
        "Schema rejection created the SQLite database"
    );
}

#[test]
fn open_rebinds_the_decoded_project_definition_before_opening_runtime_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([0]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(scan, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([scan], project);
    factory.connect([project], sink);
    drop(factory.build().unwrap());

    let mut definition = read_published_definition(&path);
    let valid_project = encode_definition(&ProjectDefinition::new([0]));
    let offset = definition
        .windows(valid_project.len())
        .position(|candidate| candidate == valid_project)
        .expect("published Flow contains the Project definition");
    let encoded_index = offset + valid_project.len() - size_of::<u32>();
    definition[encoded_index..encoded_index + size_of::<u32>()]
        .copy_from_slice(&1_u32.to_be_bytes());
    rewrite_checksum(&mut definition);
    replace_published_definition(&path, &definition);

    let Err(FlowError::Schema(error)) = FlowFactory::new(&path).open() else {
        panic!("open did not rebind the decoded schema-incompatible Project");
    };
    assert_project_field_rejection(&error);
    assert_eq!(read_published_definition(&path), definition);
}

#[test]
fn open_rebinds_decoded_multi_input_definitions() {
    let root = tempfile::tempdir().unwrap();
    let union_path = root.path().join("union");
    let valid_select = SelectDefinition::try_new([("a", col("value"))]).unwrap();
    let invalid_select = SelectDefinition::try_new([("b", col("value"))]).unwrap();
    let valid_operation = encode_definition(&valid_select);
    let invalid_operation = encode_definition(&invalid_select);
    assert_eq!(valid_operation.len(), invalid_operation.len());

    let mut factory = FlowFactory::new(&union_path);
    let left_scan = factory.station("left-scan", SequenceScanDefinition::new(0));
    let left = factory.station("left", valid_select.clone());
    let right_scan = factory.station("right-scan", SequenceScanDefinition::new(0));
    let right = factory.station("right", valid_select);
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left_scan, left, right_scan, right, union] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([left_scan], left);
    factory.connect([right_scan], right);
    factory.connect([left, right], union);
    factory.connect([union], sink);
    drop(factory.build().unwrap());

    let mut definition = read_published_definition(&union_path);
    let offset = definition
        .windows(valid_operation.len())
        .rposition(|candidate| candidate == valid_operation)
        .expect("published Flow contains the second Select definition");
    definition[offset..offset + valid_operation.len()].copy_from_slice(&invalid_operation);
    rewrite_checksum(&mut definition);
    replace_published_definition(&union_path, &definition);

    let Err(FlowError::Schema(error)) = FlowFactory::new(&union_path).open() else {
        panic!("open did not rebind the decoded schema-incompatible UnionAll");
    };
    assert_union_schema_mismatch(&error, 1);
    assert_eq!(read_published_definition(&union_path), definition);
}

#[test]
fn open_rebinds_the_decoded_sqlite_sink_input_before_opening_its_database() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("sink.sqlite");
    let valid_select =
        SelectDefinition::try_new([("Name", col("value")), ("Nome", col("value"))]).unwrap();
    let invalid_select =
        SelectDefinition::try_new([("Name", col("value")), ("name", col("value"))]).unwrap();
    let valid_operation = encode_definition(&valid_select);
    let invalid_operation = encode_definition(&invalid_select);
    assert_eq!(valid_operation.len(), invalid_operation.len());
    drop(build_select_sqlite_flow(&flow_path, &sqlite_path, valid_select).unwrap());

    let mut definition = read_published_definition(&flow_path);
    let offset = definition
        .windows(valid_operation.len())
        .position(|candidate| candidate == valid_operation)
        .expect("published Flow contains the valid Select definition");
    definition[offset..offset + valid_operation.len()].copy_from_slice(&invalid_operation);
    rewrite_checksum(&mut definition);
    replace_published_definition(&flow_path, &definition);

    let Err(FlowError::Schema(error)) = FlowFactory::new(&flow_path).open() else {
        panic!("open did not rebind the SQLite-incompatible output Schema");
    };
    assert_sqlite_identifier_collision(&error);
    assert_eq!(read_published_definition(&flow_path), definition);
    assert!(
        !sqlite_path.exists(),
        "failed open eagerly created the SQLite database"
    );
}

#[test]
fn select_and_repeated_input_union_run_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(u64::MAX));
    let select = factory.station(
        "select",
        SelectDefinition::try_new([("is_max", col("value").eq(lit(u64::MAX)))]).unwrap(),
    );
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [scan, select, union, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([scan], select);
    factory.connect([select, select], union);
    factory.connect([union], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let select_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let union_active: Cell<u32> = store.open_data("station/00000002/active-input").unwrap();
    let union_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000003/operation/00000000/running_event_count.count")
        .unwrap();
    assert!(matches!(
        store.open_data::<Cell<u32>>("station/00000001/active-input"),
        Err(StoreError::DataNotFound(name)) if name == "station/00000001/active-input"
    ));
    let transaction = store.read_transaction();
    let access = transaction.access();
    assert_eq!(count.read(access).unwrap().get().unwrap(), Some(2));
    assert_eq!(union_active.read(access).unwrap().get().unwrap(), Some(0));
    let select_status = select_output.writer().status(access).unwrap();
    assert_eq!((select_status.head, select_status.tail), (1, 1));
    for subscriber in 0..2 {
        let status = select_output
            .subscription(subscriber)
            .status(access)
            .unwrap();
        assert_eq!((status.position, status.tail), (1, 1));
    }
    let union_status = union_output.writer().status(access).unwrap();
    assert_eq!((union_status.head, union_status.tail), (2, 2));
    let count_input = union_output.subscription(0).status(access).unwrap();
    assert_eq!((count_input.position, count_input.tail), (2, 2));
}

#[test]
fn temporal_and_decimal_schema_chain_builds_runs_and_rebinds_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(1));
    let align = factory.station(
        "schema-align",
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new(
                "event_date",
                cast(cast(col("value"), DataType::Int32), DataType::Date32),
                false,
            )
            .unwrap(),
            SchemaAlignField::try_new(
                "event_time",
                cast(
                    cast(col("value"), DataType::Int64),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ),
                false,
            )
            .unwrap(),
            SchemaAlignField::try_new(
                "amount",
                cast(col("value"), DataType::Decimal128(10, 2)),
                false,
            )
            .unwrap(),
        ])
        .unwrap(),
    );
    let project = factory.station("project", ProjectDefinition::new([0, 1, 2]));
    let select = factory.station(
        "select",
        SelectDefinition::try_new([
            ("date", col("event_date")),
            ("time", col("event_time")),
            ("amount", col("amount")),
        ])
        .unwrap(),
    );
    let predicate = col("date")
        .gt_eq(lit(ScalarValue::Date32(Some(0))))
        .and(col("time").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(0), None))))
        .and(col("amount").gt(lit(ScalarValue::Decimal128(Some(0), 10, 2))));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("keep", predicate).unwrap(),
    );
    let filter = factory.station("filter", FilterDefinition::try_new(col("keep")).unwrap());
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [scan, align, project, select, extend, filter, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([scan], align);
    factory.connect([align], project);
    factory.connect([project], select);
    factory.connect([select], extend);
    factory.connect([extend], filter);
    factory.connect([filter], count);
    factory.connect([count], sink);

    let mut flow = factory.build().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    for _ in 0..2 {
        let mut reopened = FlowFactory::new(&path).open().unwrap();
        assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    }

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000006/operation/00000000/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(3)
    );
}

#[test]
fn empty_project_schema_runs_through_count_and_discard_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(u64::MAX));
    let project = factory.station("project", ProjectDefinition::new([]));
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [scan, project, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([scan], project);
    factory.connect([project], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    let mut reached_idle = false;
    for _ in 0..4 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            reached_idle = true;
            break;
        }
    }
    assert!(reached_idle, "finite Project Flow did not become idle");
    drop(flow);

    let store = Store::open(&path).unwrap();
    let project_output: SubscribedLog<Vec<u8>> =
        store.open_data("station/00000001/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000002/operation/00000000/running_event_count.count")
        .unwrap();
    assert!(matches!(
        store.open_data::<Cell<u32>>("station/00000001/active-input"),
        Err(StoreError::DataNotFound(name)) if name == "station/00000001/active-input"
    ));
    let transaction = store.read_transaction();
    assert_eq!(
        count.read(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
    let project_output = project_output
        .writer()
        .status(transaction.access())
        .unwrap();
    assert_eq!((project_output.head, project_output.tail), (1, 1));
}

fn assert_project_field_rejection(error: &FlowSchemaError) {
    assert_eq!(error.station_id(), "project");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("Project returned a non-concrete Schema binding error");
    };
    assert!(matches!(
        source.downcast_ref::<ProjectSchemaError>(),
        Some(ProjectSchemaError::Projection(
            ProjectionError::FieldOutOfBounds {
                index: 1,
                fields: 1
            }
        ))
    ));
}

fn assert_union_schema_mismatch(error: &FlowSchemaError, input: usize) {
    assert_eq!(error.station_id(), "union");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("UnionAll returned a non-concrete Schema binding error");
    };
    assert!(matches!(
        source.downcast_ref::<UnionAllSchemaError>(),
        Some(UnionAllSchemaError::InputSchemaMismatch { input: actual, .. })
            if *actual == input
    ));
}

fn assert_sqlite_identifier_collision(error: &FlowSchemaError) {
    assert_eq!(error.station_id(), "sqlite");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("SQLite identifier collision returned the wrong binding error");
    };
    assert!(matches!(
        source.downcast_ref::<SqliteSinkSchemaError>(),
        Some(SqliteSinkSchemaError::CaseInsensitiveFieldCollision {
            first: 0,
            second: 1,
        })
    ));
}

fn build_select_sqlite_flow(
    flow_path: &std::path::Path,
    sqlite_path: &std::path::Path,
    select: SelectDefinition,
) -> Result<dogpaddle_flow::Flow, FlowError> {
    let mut factory = FlowFactory::new(flow_path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let select = factory.station("select", select);
    let sqlite = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(sqlite_path, "events").unwrap(),
    );
    factory.output_capacity_bytes(scan, CAPACITY);
    factory.output_capacity_bytes(select, CAPACITY);
    factory.connect([scan], select);
    factory.connect([select], sqlite);
    factory.build()
}

fn replace_published_definition(path: &std::path::Path, definition: &[u8]) {
    let store = Store::open(path).unwrap();
    let published: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    published
        .access(transaction.access())
        .unwrap()
        .set(&definition.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}
