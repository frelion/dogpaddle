use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
};

use arrow_schema::{DataType, TimeUnit};
use dogpaddle_change::{ProjectionError, SchemaError};
use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory, FlowSchemaError};
use dogpaddle_operation::{
    ExpressionBindError, OperationBindError, OperationDefinition, ScalarValue, cast, col,
    encode_definition, lit,
    operation::{
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{
            ExtendDefinition, FilterDefinition, FilterSchemaError, ProjectDefinition,
            ProjectSchemaError, RunningEventCountDefinition, SchemaAlignDefinition,
            SchemaAlignField, SchemaAlignSchemaError, SelectDefinition, SelectSchemaError,
            UnionAllDefinition, UnionAllSchemaError,
        },
    },
    try_cast,
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

use super::support::{read_published_definition, rewrite_checksum};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024 * 1_024).unwrap();

#[test]
fn build_reports_the_exact_project_schema_rejection_without_creating_a_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([1]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([source], project);
    factory.connect([project], sink);

    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("schema-incompatible Flow did not return FlowError::Schema");
    };
    assert_project_field_rejection(&error);
    assert!(!path.exists(), "Schema rejection created the Store path");
}

#[test]
fn build_reports_filter_and_extend_schema_rejections_without_store_side_effects() {
    let root = tempfile::tempdir().unwrap();

    let filter_path = root.path().join("filter");
    let error = build_schema_error(
        &filter_path,
        "filter",
        FilterDefinition::try_new(col("value")).unwrap(),
    );
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("non-Boolean Filter returned the wrong binding error");
    };
    assert!(matches!(
        source.downcast_ref::<FilterSchemaError>(),
        Some(FilterSchemaError::PredicateType {
            actual: arrow_schema::DataType::UInt64
        })
    ));

    let extend_path = root.path().join("extend");
    let error = build_schema_error(
        &extend_path,
        "extend",
        ExtendDefinition::try_new("value", col("value")).unwrap(),
    );
    assert!(matches!(
        error.operation_error(),
        OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { name, .. }
        } if name == "value"
    ));
}

#[test]
fn build_reports_select_and_union_schema_rejections_without_store_side_effects() {
    let root = tempfile::tempdir().unwrap();

    let select_path = root.path().join("select");
    let error = build_schema_error(
        &select_path,
        "select",
        SelectDefinition::try_new([("copy", col("other"))]).unwrap(),
    );
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("invalid Select returned the wrong binding error");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression {
            field: 0,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let union_path = root.path().join("union");
    let mut factory = FlowFactory::new(&union_path);
    let left = factory.station("left", SequenceSourceDefinition::new(0));
    let right_source = factory.station("right-source", SequenceSourceDefinition::new(0));
    let right = factory.station("right", RunningEventCountDefinition::new());
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left, right_source, right, union] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([right_source], right);
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
fn build_reports_schema_align_narrowing_without_creating_a_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("schema-align");
    let align = SchemaAlignDefinition::try_new([SchemaAlignField::try_new(
        "value",
        try_cast(col("value"), DataType::Int64),
        false,
    )
    .unwrap()])
    .unwrap();
    let error = build_schema_error(&path, "schema-align", align);
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("nullable-to-non-null SchemaAlign returned the wrong binding error");
    };
    assert!(matches!(
        source.downcast_ref::<SchemaAlignSchemaError>(),
        Some(SchemaAlignSchemaError::NullabilityNarrowing { field: 0 })
    ));
    assert!(
        !path.exists(),
        "SchemaAlign rejection created the Store path"
    );
}

#[test]
fn open_rebinds_the_decoded_project_definition_before_opening_runtime_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([0]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([source], project);
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

    let Err(FlowError::Schema(error)) = FlowFactory::open(&path) else {
        panic!("open did not rebind the decoded schema-incompatible Project");
    };
    assert_project_field_rejection(&error);
    assert_eq!(read_published_definition(&path), definition);
}

#[test]
fn open_rebinds_decoded_filter_and_extend_definitions() {
    let root = tempfile::tempdir().unwrap();

    let filter_path = root.path().join("filter");
    let valid_filter = FilterDefinition::try_new(col("value").is_null()).unwrap();
    let invalid_filter = FilterDefinition::try_new(col("other").is_null()).unwrap();
    build_and_replace_operation(&filter_path, "filter", valid_filter, &invalid_filter);
    let Err(FlowError::Schema(error)) = FlowFactory::open(&filter_path) else {
        panic!("open did not rebind the decoded schema-incompatible Filter");
    };
    assert_eq!(error.station_id(), "filter");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("invalid Filter returned the wrong open binding error");
    };
    assert!(matches!(
        source.downcast_ref::<FilterSchemaError>(),
        Some(FilterSchemaError::Expression(
            ExpressionBindError::DataFusion(_)
        ))
    ));

    let extend_path = root.path().join("extend");
    build_and_replace_operation(
        &extend_path,
        "extend",
        ExtendDefinition::try_new("other", col("value")).unwrap(),
        &ExtendDefinition::try_new("value", col("value")).unwrap(),
    );
    let Err(FlowError::Schema(error)) = FlowFactory::open(&extend_path) else {
        panic!("open did not rebind the decoded schema-incompatible Extend");
    };
    assert_eq!(error.station_id(), "extend");
    assert!(matches!(
        error.operation_error(),
        OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { name, .. }
        } if name == "value"
    ));
}

#[test]
fn open_rebinds_decoded_select_and_union_definitions() {
    let root = tempfile::tempdir().unwrap();

    let select_path = root.path().join("select");
    build_and_replace_operation(
        &select_path,
        "select",
        SelectDefinition::try_new([("copy", col("value"))]).unwrap(),
        &SelectDefinition::try_new([("copy", col("other"))]).unwrap(),
    );
    let Err(FlowError::Schema(error)) = FlowFactory::open(&select_path) else {
        panic!("open did not rebind the decoded schema-incompatible Select");
    };
    assert_eq!(error.station_id(), "select");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("invalid Select returned the wrong open binding error");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression {
            field: 0,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let union_path = root.path().join("union");
    let valid_select = SelectDefinition::try_new([("a", col("value"))]).unwrap();
    let invalid_select = SelectDefinition::try_new([("b", col("value"))]).unwrap();
    let valid_operation = encode_definition(&valid_select);
    let invalid_operation = encode_definition(&invalid_select);
    assert_eq!(valid_operation.len(), invalid_operation.len());

    let mut factory = FlowFactory::new(&union_path);
    let left_source = factory.station("left-source", SequenceSourceDefinition::new(0));
    let left = factory.station("left", valid_select.clone());
    let right_source = factory.station("right-source", SequenceSourceDefinition::new(0));
    let right = factory.station("right", valid_select);
    let union = factory.station(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
    );
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [left_source, left, right_source, right, union] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([left_source], left);
    factory.connect([right_source], right);
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

    let Err(FlowError::Schema(error)) = FlowFactory::open(&union_path) else {
        panic!("open did not rebind the decoded schema-incompatible UnionAll");
    };
    assert_union_schema_mismatch(&error, 1);
    assert_eq!(read_published_definition(&union_path), definition);
}

#[test]
fn select_and_repeated_input_union_run_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX));
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
    for station in [source, select, union, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], select);
    factory.connect([select, select], union);
    factory.connect([union], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let _select_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let select_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let union_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000002/state").unwrap();
    let union_output: AppendLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000003/operation/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    assert_eq!(count.access(access).unwrap().get().unwrap(), Some(2));
    assert_eq!(
        select_output.access(access).unwrap().bounds().unwrap(),
        1..1
    );
    assert_eq!(union_output.access(access).unwrap().bounds().unwrap(), 2..2);
    let state = union_state.access(access).unwrap();
    for input in 0..2_u32 {
        let key = format!("input/{input:08x}/cursor").into_bytes();
        let cursor = state.get(&key).unwrap().unwrap();
        assert_eq!(u64::from_be_bytes(cursor.try_into().unwrap()), 1);
    }
}

#[test]
fn extend_filter_project_chain_runs_and_reopens_with_derived_schemas() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX - 1));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("keep", col("value").eq(lit(u64::MAX - 1))).unwrap(),
    );
    let filter = factory.station("filter", FilterDefinition::try_new(col("keep")).unwrap());
    let project = factory.station("project", ProjectDefinition::new([0]));
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [source, extend, filter, project, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], extend);
    factory.connect([extend], filter);
    factory.connect([filter], project);
    factory.connect([project], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    let mut reached_idle = false;
    for _ in 0..4 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            reached_idle = true;
            break;
        }
    }
    assert!(reached_idle, "finite expression Flow did not become idle");
    drop(flow);

    let store = Store::open(&path).unwrap();
    let _extend_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let _extend_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let _filter_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000002/state").unwrap();
    let _filter_output: AppendLog<Vec<u8>> = store.open_data("station/00000002/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000004/operation/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
}

#[test]
fn schema_align_runs_and_reopens_with_explicit_schema_contract() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX - 1));
    let align = factory.station(
        "schema-align",
        SchemaAlignDefinition::try_new_with_metadata(
            [
                SchemaAlignField::try_new_with_metadata(
                    "renamed",
                    col("value"),
                    true,
                    HashMap::from([("role".to_owned(), "identifier".to_owned())]),
                )
                .unwrap(),
                SchemaAlignField::try_new("text", cast(col("value"), DataType::Utf8), false)
                    .unwrap(),
            ],
            HashMap::from([("normalized".to_owned(), "v1".to_owned())]),
        )
        .unwrap(),
    );
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [source, align, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], align);
    factory.connect([align], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    let mut reached_idle = false;
    for _ in 0..4 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            reached_idle = true;
            break;
        }
    }
    assert!(reached_idle, "finite SchemaAlign Flow did not become idle");
    drop(flow);

    let store = Store::open(&path).unwrap();
    let _align_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let align_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000002/operation/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        align_output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        2..2
    );
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(2)
    );
}

#[test]
fn temporal_and_decimal_schema_chain_builds_runs_and_rebinds_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(1));
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
    for station in [source, align, project, select, extend, filter, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], align);
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
        let mut reopened = FlowFactory::open(&path).unwrap();
        assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    }

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000006/operation/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
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
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX));
    let project = factory.station("project", ProjectDefinition::new([]));
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [source, project, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], project);
    factory.connect([project], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
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
    let _project_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let _project_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let count: Cell<u64> = store
        .open_data("station/00000002/operation/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
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

fn replace_published_definition(path: &std::path::Path, definition: &[u8]) {
    let store = Store::open(path).unwrap();
    let published: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    published
        .access(transaction.access())
        .unwrap()
        .set(&definition.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}

fn build_schema_error<D>(path: &std::path::Path, station_id: &str, definition: D) -> FlowSchemaError
where
    D: OperationDefinition,
{
    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let operation = factory.station(station_id, definition);
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(operation, CAPACITY);
    factory.connect([source], operation);
    factory.connect([operation], sink);
    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("schema-incompatible Flow did not return FlowError::Schema");
    };
    assert_eq!(error.station_id(), station_id);
    assert!(!path.exists(), "Schema rejection created the Store path");
    error
}

fn build_and_replace_operation<D>(path: &std::path::Path, station_id: &str, valid: D, invalid: &D)
where
    D: OperationDefinition,
{
    let valid_operation = encode_definition(&valid);
    let invalid_operation = encode_definition(invalid);
    assert_eq!(valid_operation.len(), invalid_operation.len());

    let mut factory = FlowFactory::new(path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let operation = factory.station(station_id, valid);
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(operation, CAPACITY);
    factory.connect([source], operation);
    factory.connect([operation], sink);
    drop(factory.build().unwrap());

    let mut definition = read_published_definition(path);
    let offset = definition
        .windows(valid_operation.len())
        .position(|candidate| candidate == valid_operation)
        .expect("published Flow contains the valid Operation definition");
    definition[offset..offset + valid_operation.len()].copy_from_slice(&invalid_operation);
    rewrite_checksum(&mut definition);
    replace_published_definition(path, &definition);
}
