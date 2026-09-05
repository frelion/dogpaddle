use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use dogpaddle_change::{Change, ProjectionError};
use dogpaddle_operation::{
    DataInstances, OperationBindError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition,
    operation::{
        Action, OperationInput,
        transform::{ProjectDefinition, ProjectError, ProjectSchemaError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, commit_ready, data_names, decode_hex,
    project_input_schema, rollback_ready, turn_input,
};

const PROJECT_V1: &str = include_str!("../fixtures/v1/project_fields_0_2.hex");

fn decoded_definition() -> Box<dyn OperationDefinition> {
    decode_definition(&decode_hex(PROJECT_V1)).unwrap()
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
fn definition_has_stable_v1_literal_and_binds_projection_exactly() {
    let input = project_input_schema();
    let definition = ProjectDefinition::new([0, 2]);
    let decoded = assert_literal_definition(
        &definition,
        PROJECT_V1,
        4,
        OperationKind::Transform(NonZeroU32::MIN),
    );
    assert_eq!(definition.field_indices(), [0, 2]);
    assert!(data_names(&definition).is_empty());
    let expected = Arc::new(input.project(&[0, 2]).unwrap());
    assert_eq!(
        bind(decoded.as_ref(), std::slice::from_ref(&input))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    for (indices, previous, current) in [([0, 0], 0, 0), ([1, 0], 1, 0)] {
        let Err(OperationBindError::Rejected { source }) = bind(
            &ProjectDefinition::new(indices),
            std::slice::from_ref(&input),
        ) else {
            panic!("unordered Project indices unexpectedly bound");
        };
        assert!(matches!(
            source.downcast_ref::<ProjectSchemaError>(),
            Some(ProjectSchemaError::Projection(
                ProjectionError::FieldsNotStrictlyIncreasing {
                    previous: actual_previous,
                    current: actual_current,
                }
            )) if (*actual_previous, *actual_current) == (previous, current)
        ));
    }

    let Err(OperationBindError::Rejected { source }) =
        bind(&ProjectDefinition::new([3]), std::slice::from_ref(&input))
    else {
        panic!("out-of-bounds Project unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<ProjectSchemaError>(),
        Some(ProjectSchemaError::Projection(
            ProjectionError::FieldOutOfBounds {
                index: 3,
                fields: 3
            }
        ))
    ));
}

#[test]
fn project_input_protocol_errors_are_exact() {
    let input = project_change();
    let mut project = decoded_definition()
        .bind(&[input.schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none())
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let missing = rollback_ready(project.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        missing.downcast_ref::<ProjectError>(),
        Some(ProjectError::MissingInput)
    ));
    let invalid_port = rollback_ready(
        project.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        invalid_port.downcast_ref::<ProjectError>(),
        Some(ProjectError::InvalidInputPort { port: 1 })
    ));

    let drifted = change(&[1]);
    let error = rollback_ready(
        project.as_mut(),
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectError>(),
        Some(ProjectError::Projection(ProjectionError::SchemaMismatch))
    ));
}

#[test]
fn project_preserves_rows_diffs_and_selected_arrow_buffers_without_store_state() {
    let input = project_change();
    let mut operation = decoded_definition()
        .bind(&[input.schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none())
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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
    let mut transactions = store.into_transactions();
    let mut operation = decoded_definition()
        .bind(&[input.schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none())
        .unwrap();
    let Action::Complete(Some(reopened_output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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
