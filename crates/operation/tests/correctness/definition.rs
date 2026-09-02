use std::sync::Arc;

use arrow_array::UInt64Array;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::ProjectionError;
use dogpaddle_operation::{
    DataDeclaration, DataInstances, OperationBindError, OperationBinding, OperationDefinition,
    OperationKind,
    operation::{
        Action, Operation, OperationInput,
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ProjectDefinition, ProjectSchemaError},
    },
};
use dogpaddle_store::{Cell, Store};

use super::support::TestStore;

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

fn names(definition: &dyn OperationDefinition) -> Vec<&'static str> {
    definition
        .data()
        .iter()
        .map(DataDeclaration::name)
        .collect()
}

fn materialize(
    definition: &dyn OperationDefinition,
    input_schemas: &[SchemaRef],
    store: &Store,
    physical_names: &[&str],
) -> Box<dyn Operation> {
    assert_eq!(definition.data().len(), physical_names.len());
    let mut data = DataInstances::new();
    for (declaration, physical_name) in definition.data().iter().zip(physical_names) {
        data.insert(declaration.open(store, physical_name).unwrap())
            .unwrap();
    }
    let operation = definition
        .bind(input_schemas)
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    operation
}

fn bind(
    definition: &dyn OperationDefinition,
    input_schemas: &[SchemaRef],
) -> Result<OperationBinding, OperationBindError> {
    definition.bind(input_schemas)
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn project_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ]))
}

#[test]
fn definitions_expose_their_stable_public_contracts() {
    let source = SequenceSourceDefinition::new(42);
    assert_eq!(source.kind(), OperationKind::Source);
    assert_eq!(source.start(), 42);
    assert_eq!(names(&source), ["sequence_source.position"]);

    let count = CountDefinition::new();
    assert_eq!(
        count.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert_eq!(names(&count), ["count"]);

    let project = ProjectDefinition::new([0, 2]);
    assert_eq!(
        project.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert_eq!(project.field_indices(), [0, 2]);
    assert!(names(&project).is_empty());

    let discard = DiscardDefinition::new();
    assert_eq!(
        discard.kind(),
        OperationKind::Sink(std::num::NonZeroU32::MIN)
    );
    assert!(names(&discard).is_empty());
}

#[test]
fn definitions_bind_their_complete_logical_schema_contracts() {
    let source = SequenceSourceDefinition::new(42);
    let source_binding = bind(&source, &[]).unwrap();
    assert_eq!(source_binding.output_schema(), Some(&value_schema()));

    let arbitrary_input = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        true,
    )]));
    let count = CountDefinition::new();
    let count_binding = bind(&count, std::slice::from_ref(&arbitrary_input)).unwrap();
    assert_eq!(count_binding.output_schema(), Some(&count_schema()));

    let project_input = project_input_schema();
    let project = ProjectDefinition::new([0, 2]);
    let project_binding = bind(&project, std::slice::from_ref(&project_input)).unwrap();
    let expected_project = Arc::new(project_input.project(&[0, 2]).unwrap());
    assert_eq!(project_binding.output_schema(), Some(&expected_project));

    let discard = DiscardDefinition::new();
    assert!(
        bind(&discard, &[arbitrary_input])
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn binding_rejects_wrong_arity_and_invalid_logical_input_schemas() {
    let source = SequenceSourceDefinition::new(42);
    assert!(matches!(
        bind(&source, &[value_schema()]),
        Err(OperationBindError::InputCount {
            expected: 0,
            actual: 1
        })
    ));

    let invalid = Arc::new(Schema::new(vec![Field::new(
        "$dogpaddle.reserved",
        DataType::UInt64,
        false,
    )]));
    assert!(matches!(
        bind(&CountDefinition::new(), &[invalid]),
        Err(OperationBindError::InvalidInputSchema { input: 0, .. })
    ));

    let Err(error) = bind(
        &ProjectDefinition::new([1]),
        std::slice::from_ref(&value_schema()),
    ) else {
        panic!("out-of-bounds Project unexpectedly bound");
    };
    let OperationBindError::Rejected { source } = error else {
        panic!("out-of-bounds Project returned the wrong binding error");
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

    for (indices, previous, current) in [([0, 0], 0, 0), ([1, 0], 1, 0)] {
        let Err(error) = bind(
            &ProjectDefinition::new(indices),
            std::slice::from_ref(&project_input_schema()),
        ) else {
            panic!("unordered Project indices unexpectedly bound");
        };
        let OperationBindError::Rejected { source } = error else {
            panic!("unordered Project returned the wrong binding error");
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
}

#[test]
fn declarations_create_reopen_and_materialize_their_exact_data_classes() {
    assert_send_sync_static::<Box<dyn Operation>>();
    assert_send_sync_static::<Box<dyn OperationDefinition>>();

    let fixture = TestStore::new();
    let source_definition = SequenceSourceDefinition::new(42);
    let count_definition = CountDefinition::new();
    let project_definition = ProjectDefinition::new([0]);
    let discard_definition = DiscardDefinition::new();

    let mut store = Store::create(fixture.path()).unwrap();
    source_definition.data()[0]
        .create(&mut store, "source-position")
        .unwrap();
    count_definition.data()[0]
        .create(&mut store, "count")
        .unwrap();
    drop(store);

    let store = Store::open(fixture.path()).unwrap();
    let source_position = store.open_data::<Cell<u64>>("source-position").unwrap();
    let source = materialize(&source_definition, &[], &store, &["source-position"]);
    let count = materialize(&count_definition, &[value_schema()], &store, &["count"]);
    let project = materialize(&project_definition, &[value_schema()], &store, &[]);
    let discard = materialize(&discard_definition, &[value_schema()], &store, &[]);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let Action::Commit(Some(output)) = source.turn(None, transaction.access()).unwrap() else {
        panic!("materialized SequenceSource did not commit one output Change");
    };
    assert_eq!(output.num_rows(), 1);
    let values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.value(0), 42);
    assert_eq!(
        source_position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(42)
    );
    let Action::Complete(Some(count_output)) = count
        .turn(
            Some(OperationInput {
                port: 0,
                change: &output,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("materialized Count did not complete one input with output");
    };
    assert_eq!(count_output.num_rows(), 1);
    let Action::Complete(Some(project_output)) = project
        .turn(
            Some(OperationInput {
                port: 0,
                change: &output,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("materialized Project did not complete with one output Change");
    };
    assert_eq!(project_output.schema(), output.schema());
    let Action::Complete(None) = discard
        .turn(
            Some(OperationInput {
                port: 0,
                change: &output,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("materialized Discard did not complete one input without output");
    };
    transaction.commit().unwrap();
    drop((source, count, project, discard));
}
