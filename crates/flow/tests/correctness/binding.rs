use std::num::NonZeroU32;

use arrow_schema::{DataType, TimeUnit};
use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::{
    OperationBindError, ScalarValue, cast, col, encode_definition, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition, SqliteSinkSchemaError},
        transform::{
            FilterDefinition, RunningEventCountDefinition, SelectDefinition, SelectField,
            SelectSchemaError, UnionAllDefinition, UnionAllSchemaError,
        },
    },
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::{read_published_definition, rewrite_checksum};

const OWNER_IDENTITY: [u8; 32] = [0xa5; 32];

#[test]
fn build_reports_the_exact_projection_schema_rejection_without_creating_a_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let project = factory.operation(
        "project",
        SelectDefinition::try_new([("missing", dogpaddle_operation::col("other"))]).unwrap(),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [project]);

    let Err(error @ FlowError::Schema { .. }) = factory.build() else {
        panic!("schema-incompatible Flow did not return FlowError::Schema");
    };
    assert_projection_field_rejection(&error);
    assert!(!path.exists(), "Schema rejection created the Store path");
}

#[test]
fn build_reports_a_multi_input_schema_rejection_without_store_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let union_path = root.path().join("union");
    let mut factory = FlowFactory::new(&union_path);
    let left = factory.operation("left", SequenceScanDefinition::new(0), []);
    let right_scan = factory.operation("right-scan", SequenceScanDefinition::new(0), []);
    let right = factory.operation("right", RunningEventCountDefinition::new(), [right_scan]);
    let union = factory.operation(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
        [left, right],
    );
    factory.operation("sink", DiscardDefinition::new(), [union]);

    let Err(error @ FlowError::Schema { .. }) = factory.build() else {
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

    let Err(error @ FlowError::Schema { .. }) =
        build_select_sqlite_flow(&flow_path, &sqlite_path, select)
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
fn open_rebinds_the_decoded_select_definition_before_opening_runtime_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    factory.owner_identity(OWNER_IDENTITY);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let project = factory.operation(
        "project",
        SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [project]);

    drop(factory.build().unwrap());

    let mut definition = read_published_definition(&path);
    let valid_project = encode_definition(
        &SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))])
            .unwrap()
            .into(),
    );
    let offset = definition
        .windows(valid_project.len())
        .position(|candidate| candidate == valid_project)
        .expect("published Flow contains the Project definition");
    let invalid_projection = encode_definition(
        &SelectDefinition::try_new([("value", col("other"))])
            .unwrap()
            .into(),
    );
    assert_eq!(invalid_projection.len(), valid_project.len());
    definition[offset..offset + valid_project.len()].copy_from_slice(&invalid_projection);
    rewrite_checksum(&mut definition);
    replace_published_definition(&path, &definition);

    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::OwnerIdentityMismatch)
    ));

    let mut open = FlowFactory::new(&path);
    open.owner_identity(OWNER_IDENTITY);
    let Err(error @ FlowError::Schema { .. }) = open.open() else {
        panic!("open did not rebind the decoded schema-incompatible Project");
    };
    assert_projection_field_rejection(&error);
    assert_eq!(read_published_definition(&path), definition);
}

#[test]
fn open_rebinds_decoded_multi_input_definitions() {
    let root = tempfile::tempdir().unwrap();
    let union_path = root.path().join("union");
    let valid_select = SelectDefinition::try_new([("a", col("value"))]).unwrap();
    let invalid_select = SelectDefinition::try_new([("b", col("value"))]).unwrap();
    let valid_operation = encode_definition(&valid_select.clone().into());
    let invalid_operation = encode_definition(&invalid_select.into());
    assert_eq!(valid_operation.len(), invalid_operation.len());

    let mut factory = FlowFactory::new(&union_path);
    let left_scan = factory.operation("left-scan", SequenceScanDefinition::new(0), []);
    let left = factory.operation("left", valid_select.clone(), [left_scan]);
    let right_scan = factory.operation("right-scan", SequenceScanDefinition::new(0), []);
    let right = factory.operation("right", valid_select, [right_scan]);
    let union = factory.operation(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
        [left, right],
    );
    factory.operation("sink", DiscardDefinition::new(), [union]);

    drop(factory.build().unwrap());

    let mut definition = read_published_definition(&union_path);
    let offset = definition
        .windows(valid_operation.len())
        .rposition(|candidate| candidate == valid_operation)
        .expect("published Flow contains the second Select definition");
    definition[offset..offset + valid_operation.len()].copy_from_slice(&invalid_operation);
    rewrite_checksum(&mut definition);
    replace_published_definition(&union_path, &definition);

    let Err(error @ FlowError::Schema { .. }) = FlowFactory::new(&union_path).open() else {
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
    let valid_operation = encode_definition(&valid_select.clone().into());
    let invalid_operation = encode_definition(&invalid_select.into());
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

    let Err(error @ FlowError::Schema { .. }) = FlowFactory::new(&flow_path).open() else {
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
    let scan = factory.operation("scan", SequenceScanDefinition::new(u64::MAX), []);
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([("is_max", col("value").eq(lit(u64::MAX)))]).unwrap(),
        [scan],
    );
    let union = factory.operation(
        "union",
        UnionAllDefinition::new(NonZeroU32::new(2).unwrap()),
        [select, select],
    );
    let count = factory.operation("count", RunningEventCountDefinition::new(), [union]);
    factory.operation("sink", DiscardDefinition::new(), [count]);

    drop(factory.build().unwrap());

    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::new(&path).open().unwrap();
    super::support::run_until_idle(&mut flow);
    drop(flow);

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000003/running_event_count.count")
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
fn temporal_and_decimal_schema_chain_builds_runs_and_rebinds_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(1), []);
    let align = factory.operation(
        "schema-align",
        SelectDefinition::try_new(
            [
                (
                    "event_date",
                    cast(cast(col("value"), DataType::Int32), DataType::Date32),
                ),
                (
                    "event_time",
                    cast(
                        cast(col("value"), DataType::Int64),
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                    ),
                ),
                ("amount", cast(col("value"), DataType::Decimal128(10, 2))),
            ]
            .map(|(name, expression)| SelectField {
                name: name.into(),
                expression,
                nullable: Some(false),
                metadata: Some(arrow_schema::Metadata::new()),
            }),
        )
        .unwrap()
        .with_metadata(arrow_schema::Metadata::new()),
        [scan],
    );
    let project = factory.operation(
        "project",
        SelectDefinition::try_new(
            ["event_date", "event_time", "amount"].map(|name| (name, col(name))),
        )
        .unwrap(),
        [align],
    );
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([
            ("date", col("event_date")),
            ("time", col("event_time")),
            ("amount", col("amount")),
        ])
        .unwrap(),
        [project],
    );
    let predicate = col("date")
        .gt_eq(lit(ScalarValue::Date32(Some(0))))
        .and(col("time").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(0), None))))
        .and(col("amount").gt(lit(ScalarValue::Decimal128(Some(0), 10, 2))));
    let extend = factory.operation(
        "extend",
        SelectDefinition::try_new([
            ("date", col("date")),
            ("time", col("time")),
            ("amount", col("amount")),
            ("keep", predicate),
        ])
        .unwrap(),
        [select],
    );
    let filter = factory.operation(
        "filter",
        FilterDefinition::try_new(col("keep")).unwrap(),
        [extend],
    );
    let count = factory.operation("count", RunningEventCountDefinition::new(), [filter]);
    factory.operation("sink", DiscardDefinition::new(), [count]);

    let mut flow = factory.build().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    for _ in 0..2 {
        let mut reopened = FlowFactory::new(&path).open().unwrap();
        assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Progressed);
    }

    let store = Store::open(&path).unwrap();
    let count: Cell<u64> = store
        .open_data("operation/00000006/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(3)
    );
}

#[test]
fn empty_projection_schema_runs_through_count_and_discard_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(u64::MAX), []);
    let project = factory.operation(
        "project",
        SelectDefinition::try_new(Vec::<(&str, dogpaddle_operation::Expr)>::new()).unwrap(),
        [scan],
    );
    let count = factory.operation("count", RunningEventCountDefinition::new(), [project]);
    factory.operation("sink", DiscardDefinition::new(), [count]);

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
    let count: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
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
}

fn assert_projection_field_rejection(error: &FlowError) {
    let FlowError::Schema {
        operation_id,
        source,
        ..
    } = error
    else {
        panic!("expected Flow Schema error");
    };
    assert_eq!(operation_id, "project");
    let OperationBindError::Rejected { source } = source else {
        panic!("Project returned a non-concrete Schema binding error");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression { field: 0, .. })
    ));
}

fn assert_union_schema_mismatch(error: &FlowError, input: usize) {
    let FlowError::Schema {
        operation_id,
        source,
        ..
    } = error
    else {
        panic!("expected Flow Schema error");
    };
    assert_eq!(operation_id, "union");
    let OperationBindError::Rejected { source } = source else {
        panic!("UnionAll returned a non-concrete Schema binding error");
    };
    assert!(matches!(
        source.downcast_ref::<UnionAllSchemaError>(),
        Some(UnionAllSchemaError::InputSchemaMismatch { input: actual, .. })
            if *actual == input
    ));
}

fn assert_sqlite_identifier_collision(error: &FlowError) {
    let FlowError::Schema {
        operation_id,
        source,
        ..
    } = error
    else {
        panic!("expected Flow Schema error");
    };
    assert_eq!(operation_id, "sqlite");
    let OperationBindError::Rejected { source } = source else {
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
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let select = factory.operation("select", select, [scan]);
    factory.operation(
        "sqlite",
        SqliteSinkDefinition::try_new(sqlite_path, "events").unwrap(),
        [select],
    );

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
