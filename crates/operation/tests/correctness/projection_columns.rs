use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, ProjectionError, RuntimeResource, col, decode_definition,
    encode_definition,
    operation::{Action, OperationInput, transform::SelectDefinition},
};
use dogpaddle_store::{Store, StoreSetup};

use super::support::{
    TestStore, change, commit_ready, project_input_schema, rollback_ready, turn_input,
};

fn decoded_definition() -> Box<dyn OperationDefinition> {
    let definition =
        SelectDefinition::try_new([("id", col("id")), ("score", col("score"))]).unwrap();
    decode_definition(&encode_definition(&definition)).unwrap()
}

fn project_change() -> Change {
    Change::try_new(
        RecordBatch::try_new(
            project_input_schema(),
            vec![
                Arc::new(UInt64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, -1]),
    )
    .unwrap()
}

#[test]
fn project_rejects_invalid_port_and_schema_drift() {
    let input = project_change();
    let fixture = TestStore::new();
    let mut setup = StoreSetup::new();
    let definition = decoded_definition();
    let constructed = definition
        .construct(
            &[input.schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap();
    let (mut project, _) = constructed.into_parts();
    let mut transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    let invalid_port = rollback_ready(
        &mut project,
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        invalid_port.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InvalidInputPort { port: 1 })
    ));

    let drifted = change(&[1]);
    let error =
        rollback_ready(&mut project, Some(turn_input(&drifted)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InputSchemaMismatch)
    ));
}

#[test]
fn project_preserves_rows_diffs_and_selected_arrow_buffers_without_store_state() {
    let input = project_change();
    let fixture = TestStore::new();
    let mut setup = StoreSetup::new();
    let definition = decoded_definition();
    let constructed = definition
        .construct(
            &[input.schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap();
    let (mut operation, _) = constructed.into_parts();
    let mut transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("Project did not complete with one output Change");
    };
    assert_eq!(output.num_rows(), 2);
    assert_eq!(output.diffs(), input.diffs());
    assert_eq!(output.schema().fields().len(), 2);
    assert_eq!(output.schema().field(0).name(), "id");
    assert_eq!(output.schema().field(1).name(), "score");
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(0)
    ));
    assert!(Arc::ptr_eq(
        output.records().column(1),
        input.records().column(2)
    ));

    drop((operation, transactions));
    let store = Store::open(fixture.path()).unwrap();
    let definition = decoded_definition();
    let constructed = definition
        .construct(
            &[input.schema()],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap();
    let (mut operation, _) = constructed.into_parts();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(reopened_output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("reopened Project did not complete with one output Change");
    };
    assert_eq!(reopened_output.diffs(), input.diffs());
    assert_eq!(reopened_output.schema().field(0).name(), "id");
    assert_eq!(reopened_output.schema().field(1).name(), "score");
    assert!(Arc::ptr_eq(
        reopened_output.records().column(0),
        input.records().column(0)
    ));
    assert!(Arc::ptr_eq(
        reopened_output.records().column(1),
        input.records().column(2)
    ));
}
