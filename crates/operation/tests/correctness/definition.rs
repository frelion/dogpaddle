use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow_array::UInt64Array;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::metadata::FieldMetadata;
use dogpaddle_change::{ProjectionError, SchemaError};
use dogpaddle_operation::{
    DataDeclaration, DataInstances, Expr, ExpressionBindError, ExpressionDefinitionError,
    OperationBindError, OperationBinding, OperationDefinition, OperationKind, ScalarValue, cast,
    col, lit,
    operation::{
        Action, Operation, OperationInput,
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{
            CountDefinition, ExtendDefinition, ExtendSchemaError, FilterDefinition,
            FilterSchemaError, ProjectDefinition, ProjectSchemaError, SelectDefinition,
            SelectSchemaError, UnionAllDefinition, UnionAllSchemaError,
        },
    },
    try_cast,
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

fn equal(left: Expr, right: Expr) -> Expr {
    left.eq(right)
}

fn filter(predicate: Expr) -> FilterDefinition {
    FilterDefinition::try_new(predicate).unwrap()
}

fn extend(field_name: &str, expression: Expr) -> ExtendDefinition {
    ExtendDefinition::try_new(field_name, expression).unwrap()
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

    let filter_expression = equal(col("id"), lit(7_u64));
    let filter = filter(filter_expression.clone());
    assert_eq!(
        filter.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert_eq!(filter.predicate(), &filter_expression);
    assert!(names(&filter).is_empty());

    let extend_expression = col("message").is_null();
    let extend = extend("message_missing", extend_expression.clone());
    assert_eq!(
        extend.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert_eq!(extend.field_name(), "message_missing");
    assert_eq!(extend.expression(), &extend_expression);
    assert!(names(&extend).is_empty());

    let select_expressions = [
        ("score", col("score")),
        ("missing", col("message").is_null()),
    ];
    let select = SelectDefinition::try_new(select_expressions.clone()).unwrap();
    assert_eq!(
        select.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert!(
        select.fields().eq(select_expressions
            .iter()
            .map(|(name, expression)| (*name, expression)))
    );
    assert!(names(&select).is_empty());

    let union = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    assert_eq!(
        union.kind(),
        OperationKind::Transform(std::num::NonZeroU32::new(2).unwrap())
    );
    assert_eq!(union.input_count().get(), 2);
    assert!(names(&union).is_empty());

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

    let filter = filter(equal(col("id"), lit(7_u64)));
    let filter_binding = bind(&filter, std::slice::from_ref(&project_input)).unwrap();
    assert_eq!(filter_binding.output_schema(), Some(&project_input));

    let extend = extend("message_missing", col("message").is_null());
    let extend_binding = bind(&extend, std::slice::from_ref(&project_input)).unwrap();
    let expected_extend = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
        Field::new("message_missing", DataType::Boolean, false),
    ]));
    assert_eq!(extend_binding.output_schema(), Some(&expected_extend));

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
fn expression_binding_delegates_planning_errors_and_enforces_filter_results() {
    let input = project_input_schema();
    let Err(OperationBindError::Rejected { source }) = bind(
        &extend("copy", col("missing")),
        std::slice::from_ref(&input),
    ) else {
        panic!("out-of-bounds expression column unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<ExtendSchemaError>(),
        Some(ExtendSchemaError::Expression(
            ExpressionBindError::DataFusion(_)
        ))
    ));

    let Err(OperationBindError::Rejected { source }) =
        bind(&filter(col("id")), std::slice::from_ref(&input))
    else {
        panic!("non-Boolean filter predicate unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<FilterSchemaError>(),
        Some(FilterSchemaError::PredicateType {
            actual: DataType::UInt64
        })
    ));
}

#[test]
fn expression_constructors_accept_exactly_round_tripping_datafusion_exprs() {
    for expression in [
        lit(1_i32),
        col("scope.value"),
        col("value").alias("renamed"),
    ] {
        let definition = FilterDefinition::try_new(expression.clone()).unwrap();
        assert_eq!(definition.predicate(), &expression);
    }

    let expression = col("value").between(lit(1_u64), lit(10_u64));
    let definition = ExtendDefinition::try_new("in_range", expression.clone()).unwrap();
    assert_eq!(definition.expression(), &expression);
}

#[test]
fn expression_constructor_rejects_a_non_round_tripping_datafusion_expr() {
    let expression = Expr::Literal(
        ScalarValue::Int64(Some(7)),
        Some(FieldMetadata::new(BTreeMap::from([(
            "source".to_owned(),
            "test".to_owned(),
        )]))),
    );

    assert!(matches!(
        FilterDefinition::try_new(expression),
        Err(ExpressionDefinitionError::NonRoundTrip)
    ));
}

#[test]
fn datafusion_binding_derives_arithmetic_and_cast_output_schema() {
    let input = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, true),
    ]));

    let arithmetic = extend("next", cast(col("value"), DataType::Int64) + lit(1_i64));
    let binding = bind(&arithmetic, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.field(2).data_type(), &DataType::Int64);
    assert!(!output.field(2).is_nullable());

    let parsed = extend("parsed", try_cast(col("text"), DataType::Int64));
    let binding = bind(&parsed, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.field(2).data_type(), &DataType::Int64);
    assert!(output.field(2).is_nullable());
}

#[test]
fn extend_derives_one_valid_field_and_preserves_input_schema_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_owned(), "test".to_owned());
    let input = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("flag", DataType::Boolean, true)
                .with_metadata(HashMap::from([("meaning".to_owned(), "input".to_owned())])),
            Field::new("nothing", DataType::Null, true),
        ],
        metadata.clone(),
    ));

    let copied_flag = extend("copied_flag", col("flag"));
    let binding = bind(&copied_flag, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.metadata(), &metadata);
    assert_eq!(output.field(0), input.field(0));
    assert_eq!(output.field(2).data_type(), &DataType::Boolean);
    assert!(output.field(2).is_nullable());
    assert!(output.field(2).metadata().is_empty());

    let copied_null = extend("copied_null", col("nothing"));
    let binding = bind(&copied_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.output_schema().unwrap().field(2).is_nullable());

    let non_null = extend("constant", lit("ready"));
    let binding = bind(&non_null, std::slice::from_ref(&input)).unwrap();
    assert!(!binding.output_schema().unwrap().field(2).is_nullable());

    let typed_null = extend("missing", lit(ScalarValue::Int64(None)));
    let binding = bind(&typed_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.output_schema().unwrap().field(2).is_nullable());
}

#[test]
fn extend_output_schema_rejects_duplicate_and_reserved_names_centrally() {
    let input = project_input_schema();
    let duplicate = extend("id", col("id"));
    assert!(matches!(
        bind(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "id"
    ));

    let reserved = extend("$dogpaddle.internal", col("id"));
    assert!(matches!(
        bind(&reserved, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedFieldName { ref name, .. }
        }) if name == "$dogpaddle.internal"
    ));
}

#[test]
fn select_binds_ordered_independent_expressions_and_preserves_schema_metadata() {
    let metadata = HashMap::from([("owner".to_owned(), "test".to_owned())]);
    let input = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("message", DataType::Utf8, true),
        ],
        metadata.clone(),
    ));
    let definition = SelectDefinition::try_new([
        ("missing", col("message").is_null()),
        ("copied", col("message")),
        ("id", col("id")),
    ])
    .unwrap();

    let binding = bind(&definition, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.metadata(), &metadata);
    assert_eq!(output.fields().len(), 3);
    assert_eq!(
        output.field(0),
        &Field::new("missing", DataType::Boolean, false)
    );
    assert_eq!(output.field(1), &Field::new("copied", DataType::Utf8, true));
    assert_eq!(output.field(2), &Field::new("id", DataType::UInt64, false));
    assert!(
        output
            .fields()
            .iter()
            .all(|field| field.metadata().is_empty())
    );
}

#[test]
fn select_reports_expression_context_and_rejects_invalid_output_names_centrally() {
    let input = project_input_schema();
    let alias_reference = SelectDefinition::try_new([
        ("derived_alias", col("id")),
        ("uses_alias", col("derived_alias")),
    ])
    .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        bind(&alias_reference, std::slice::from_ref(&input))
    else {
        panic!("Select expression unexpectedly referenced an earlier output alias");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression {
            field: 1,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let duplicate =
        SelectDefinition::try_new([("same", col("id")), ("same", col("score"))]).unwrap();
    assert!(matches!(
        bind(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "same"
    ));

    let reserved = SelectDefinition::try_new([("$dogpaddle.internal", col("id"))]).unwrap();
    assert!(matches!(
        bind(&reserved, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedFieldName { ref name, .. }
        }) if name == "$dogpaddle.internal"
    ));
}

#[test]
fn union_all_requires_its_non_zero_arity_and_exact_input_schema() {
    let definition = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    let expected = value_schema();
    let mismatched = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        true,
    )]));

    let Err(OperationBindError::Rejected { source }) = bind(
        &definition,
        &[Arc::clone(&expected), Arc::clone(&mismatched)],
    ) else {
        panic!("mismatched UnionAll input unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<UnionAllSchemaError>(),
        Some(UnionAllSchemaError::InputSchemaMismatch {
            input: 1,
            expected: actual_expected,
            actual,
        }) if actual_expected == &expected && actual == &mismatched
    ));

    let binding = bind(&definition, &[Arc::clone(&expected), Arc::clone(&expected)]).unwrap();
    assert_eq!(binding.output_schema(), Some(&expected));
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
