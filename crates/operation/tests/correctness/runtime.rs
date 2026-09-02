use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, ChangeProjection, ProjectionError};
use dogpaddle_operation::{
    BinaryOperator, DataInstances, Expression, ExpressionError, Literal, OperationDefinition,
    UnaryOperator,
    operation::{
        Action, Operation, OperationInput,
        sink::{DiscardError, DiscardOperation},
        source::{SequenceSourceError, SequenceSourceOperation},
        transform::{
            CountError, CountOperation, ExtendDefinition, ExtendError, FilterDefinition,
            FilterError, ProjectError, ProjectOperation,
        },
    },
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::TestStore;

fn change(diffs: &[i64]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![7; diffs.len()]))],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn turn_input(change: &Change) -> OperationInput<'_> {
    OperationInput { port: 0, change }
}

fn equal(left: Expression, right: Expression) -> Expression {
    Expression::binary(BinaryOperator::Equal, left, right)
}

fn stateless_operation(
    definition: &dyn OperationDefinition,
    input_schema: arrow_schema::SchemaRef,
) -> Box<dyn Operation> {
    let mut data = DataInstances::new();
    let operation = definition
        .bind(&[input_schema])
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    operation
}

#[derive(Clone, Copy)]
enum ExpectedAction {
    Commit,
    Complete,
}

fn output_values(action: Action, expected_action: ExpectedAction, field_name: &str) -> Vec<u64> {
    let output = match expected_action {
        ExpectedAction::Commit => {
            let Action::Commit(Some(output)) = action else {
                panic!("Operation did not commit one output Change")
            };
            output
        }
        ExpectedAction::Complete => {
            let Action::Complete(Some(output)) = action else {
                panic!("Operation did not complete with one output Change")
            };
            output
        }
    };
    let field = output.schema().field(0).clone();
    assert_eq!(field.name(), field_name);
    assert_eq!(field.data_type(), &DataType::UInt64);
    assert!(!field.is_nullable());
    assert_eq!(
        output.diffs().values(),
        vec![1; output.num_rows()].as_slice()
    );
    let values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..output.num_rows())
        .map(|index| values.value(index))
        .collect()
}

#[test]
fn builtins_follow_one_stateful_action_trace_across_reopen() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let source = SequenceSourceOperation::new(41, position);
    let transform = CountOperation::new(count);
    let sink = DiscardOperation;
    let input = change(&[2, -1]);
    let mut transactions = store.into_transactions();

    for commit in [false, true] {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            output_values(
                source.turn(None, transaction.access()).unwrap(),
                ExpectedAction::Commit,
                "value",
            ),
            [41]
        );
        assert_eq!(
            output_values(
                transform
                    .turn(Some(turn_input(&input)), transaction.access())
                    .unwrap(),
                ExpectedAction::Complete,
                "count",
            ),
            [1, 2]
        );
        assert!(matches!(
            sink.turn(Some(turn_input(&input)), transaction.access())
                .unwrap(),
            Action::Complete(None)
        ));
        if commit {
            transaction.commit().unwrap();
        }
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let source =
        SequenceSourceOperation::new(41, store.open_data::<Cell<u64>>("position").unwrap());
    let count_state = store.open_data::<Cell<u64>>("count").unwrap();
    let transform = CountOperation::new(count_state.clone());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        output_values(
            source.turn(None, transaction.access()).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [42]
    );
    assert_eq!(
        output_values(
            transform
                .turn(Some(turn_input(&input)), transaction.access())
                .unwrap(),
            ExpectedAction::Complete,
            "count",
        ),
        [3, 4]
    );
    assert_eq!(
        count_state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(4)
    );
    transaction.commit().unwrap();
}

#[test]
fn builtin_input_protocol_errors_and_source_boundary_are_exact() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let source = SequenceSourceOperation::new(
        u64::MAX - 1,
        store.create_data::<Cell<u64>>("position").unwrap(),
    );
    let count = CountOperation::new(store.create_data::<Cell<u64>>("count").unwrap());
    let sink = DiscardOperation;
    let input = change(&[1]);
    let mut transactions = store.into_transactions();

    for expected in [u64::MAX - 1, u64::MAX] {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            output_values(
                source.turn(None, transaction.access()).unwrap(),
                ExpectedAction::Commit,
                "value",
            ),
            [expected]
        );
        transaction.commit().unwrap();
    }
    for _ in 0..2 {
        let transaction = transactions.begin().unwrap();
        assert!(matches!(
            source.turn(None, transaction.access()).unwrap(),
            Action::Idle
        ));
    }

    let transaction = transactions.begin().unwrap();
    let source_error = source
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap_err();
    assert!(matches!(
        source_error.downcast_ref::<SequenceSourceError>(),
        Some(SequenceSourceError::UnexpectedInput)
    ));

    let count_error = count.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        count_error.downcast_ref::<CountError>(),
        Some(CountError::MissingInput)
    ));
    let count_error = count
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        count_error.downcast_ref::<CountError>(),
        Some(CountError::InvalidInputPort { port: 1 })
    ));

    let sink_error = sink.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        sink_error.downcast_ref::<DiscardError>(),
        Some(DiscardError::MissingInput)
    ));
    let sink_error = sink
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        sink_error.downcast_ref::<DiscardError>(),
        Some(DiscardError::InvalidInputPort { port: 1 })
    ));

    drop(transaction);
    drop(transactions);
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = Store::create(foreign_root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign.into_transactions();
    let transaction = foreign_transactions.begin().unwrap();
    let error = source.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
    drop(transaction);

    let transaction = foreign_transactions.begin().unwrap();
    let error = count
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
}

#[test]
fn project_input_protocol_errors_are_exact() {
    let input = change(&[1]);
    let project = ProjectOperation::new(ChangeProjection::try_new(input.schema(), [0]).unwrap());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let missing = project.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        missing.downcast_ref::<ProjectError>(),
        Some(ProjectError::MissingInput)
    ));
    let invalid_port = project
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        invalid_port.downcast_ref::<ProjectError>(),
        Some(ProjectError::InvalidInputPort { port: 1 })
    ));

    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "expected",
        DataType::UInt64,
        false,
    )]));
    let mismatched =
        ProjectOperation::new(ChangeProjection::try_new(expected_schema, [0]).unwrap());
    let error = mismatched
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectError>(),
        Some(ProjectError::Projection(ProjectionError::SchemaMismatch))
    ));
}

#[test]
fn project_preserves_rows_diffs_and_selected_arrow_buffers_without_store_state() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("first", DataType::UInt64, false),
        Field::new("second", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(UInt64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let operation = ProjectOperation::new(ChangeProjection::try_new(schema, [1]).unwrap());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let Action::Complete(Some(output)) = operation
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("Project did not complete with one output Change");
    };
    assert_eq!(output.num_rows(), 2);
    assert_eq!(output.diffs(), input.diffs());
    assert_eq!(output.schema().fields().len(), 1);
    assert_eq!(output.schema().field(0).name(), "second");
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(1)
    ));
}

#[test]
fn filter_keeps_only_true_rows_with_the_same_order_records_and_diffs() {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        Some(vec![Some(2)]),
        None,
        Some(vec![Some(4), Some(5)]),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30, 40])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(StringArray::from(vec![
                Some("ten"),
                Some("twenty"),
                None,
                Some("forty"),
            ])),
            Arc::new(items),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2])).unwrap();
    let operation = stateless_operation(
        &FilterDefinition::new(Expression::column(1)),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let Action::Complete(Some(output)) = operation
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("Filter did not complete with a partial output Change");
    };
    assert_eq!(output.schema(), schema);
    assert_eq!(output.diffs().values(), &[1, -2]);
    let ids = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[10, 40]);
    let labels = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        labels.iter().collect::<Vec<_>>(),
        [Some("ten"), Some("forty")]
    );
    let items = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(1), None]
    );
    assert_eq!(
        items
            .value(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[4, 5]
    );
}

#[test]
fn filter_partially_selects_null_binary_and_struct_columns() {
    let score_field = Arc::new(Field::new("score", DataType::Int64, true));
    let object = StructArray::from(vec![(
        Arc::clone(&score_field),
        Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(40)])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Boolean, true),
        Field::new("nothing", DataType::Null, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("object", DataType::Struct(vec![score_field].into()), false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            arrow_array::new_null_array(&DataType::Null, 4),
            Arc::new(BinaryArray::from(vec![
                Some(b"ten".as_slice()),
                Some(b"twenty".as_slice()),
                None,
                Some(b"forty".as_slice()),
            ])),
            Arc::new(object),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 2, 3, 4])).unwrap();
    let operation = stateless_operation(
        &FilterDefinition::new(Expression::column(0)),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let Action::Complete(Some(output)) = operation
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("Filter did not produce its partial heterogeneous output");
    };
    assert_eq!(output.schema(), schema);
    assert_eq!(output.diffs().values(), &[1, 4]);
    assert_eq!(output.records().column(1).logical_null_count(), 2);
    let payload = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), b"ten");
    assert_eq!(payload.value(1), b"forty");
    let object = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let scores = object
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[10, 40]);
}

#[test]
fn filter_all_true_is_zero_copy_and_all_false_or_null_completes_without_output() {
    let input = change(&[1, -1, 2]);
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let all_true = stateless_operation(
        &FilterDefinition::new(Expression::literal(Literal::Boolean(Some(true)))),
        input.schema(),
    );
    let Action::Complete(Some(output)) = all_true
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("all-true Filter did not retain its complete input");
    };
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(0)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    for predicate in [
        Expression::literal(Literal::Boolean(Some(false))),
        Expression::literal(Literal::Boolean(None)),
    ] {
        let operation = stateless_operation(&FilterDefinition::new(predicate), input.schema());
        assert!(matches!(
            operation
                .turn(Some(turn_input(&input)), transaction.access())
                .unwrap(),
            Action::Complete(None)
        ));
    }
}

#[test]
fn extend_appends_one_derived_column_and_shares_every_input_buffer() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let expression = Expression::binary(
        BinaryOperator::Or,
        Expression::binary(
            BinaryOperator::And,
            Expression::column(0),
            Expression::literal(Literal::Boolean(None)),
        ),
        Expression::unary(UnaryOperator::IsNull, Expression::column(1)),
    );
    let operation = stateless_operation(
        &ExtendDefinition::new("selected", expression),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let Action::Complete(Some(output)) = operation
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("Extend did not complete with one output Change");
    };
    assert_eq!(output.schema().fields().len(), 3);
    assert_eq!(output.schema().field(2).name(), "selected");
    assert_eq!(output.schema().field(2).data_type(), &DataType::Boolean);
    assert!(output.schema().field(2).is_nullable());
    for index in 0..2 {
        assert!(Arc::ptr_eq(
            output.records().column(index),
            input.records().column(index)
        ));
    }
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let selected = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        selected.iter().collect::<Vec<_>>(),
        [None, Some(true), None]
    );

    let copy = stateless_operation(
        &ExtendDefinition::new("label_copy", Expression::column(1)),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(copied)) = copy
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap()
    else {
        panic!("column-copy Extend did not complete");
    };
    assert!(Arc::ptr_eq(
        copied.records().column(2),
        input.records().column(1)
    ));
}

#[test]
fn filter_and_extend_reject_protocol_errors_and_runtime_schema_drift() {
    let input = change(&[1]);
    let filter = stateless_operation(
        &FilterDefinition::new(Expression::literal(Literal::Boolean(Some(true)))),
        input.schema(),
    );
    let extend = stateless_operation(
        &ExtendDefinition::new("copy", Expression::column(0)),
        input.schema(),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let error = filter.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::MissingInput)
    ));
    let error = filter
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::InvalidInputPort { port: 1 })
    ));
    let error = extend.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ExtendError>(),
        Some(ExtendError::MissingInput)
    ));
    let error = extend
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ExtendError>(),
        Some(ExtendError::InvalidInputPort { port: 1 })
    ));

    let drifted_schema = Arc::new(Schema::new(vec![Field::new(
        "renamed",
        DataType::UInt64,
        false,
    )]));
    let drifted = Change::try_new(
        RecordBatch::try_new(drifted_schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    for operation in [&filter, &extend] {
        let error = operation
            .turn(Some(turn_input(&drifted)), transaction.access())
            .unwrap_err();
        let schema_mismatch = error.downcast_ref::<FilterError>().is_some_and(|error| {
            matches!(
                error,
                FilterError::Expression(ExpressionError::SchemaMismatch)
            )
        }) || error.downcast_ref::<ExtendError>().is_some_and(|error| {
            matches!(
                error,
                ExtendError::Expression(ExpressionError::SchemaMismatch)
            )
        });
        assert!(schema_mismatch);
    }
}

#[test]
fn boolean_expression_operators_follow_complete_kleene_truth_tables() {
    let values = [Some(true), Some(false), None];
    let left = values
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value, 3))
        .collect::<Vec<_>>();
    let right = values.repeat(3);
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Boolean, true),
        Field::new("right", DataType::Boolean, true),
        Field::new("nothing", DataType::Null, false),
        Field::new("number", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(left)),
            Arc::new(BooleanArray::from(right)),
            arrow_array::new_null_array(&DataType::Null, 9),
            Arc::new(UInt64Array::from(vec![1; 9])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1; 9])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    for (name, expression, expected) in kleene_cases() {
        let operation = stateless_operation(
            &ExtendDefinition::new(name, expression),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) = operation
            .turn(Some(turn_input(&input)), transaction.access())
            .unwrap()
        else {
            panic!("Boolean expression Extend returned the wrong action");
        };
        let actual = output
            .records()
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "wrong result for {name}");
    }
}

fn repeat_each(values: [Option<bool>; 3]) -> Vec<Option<bool>> {
    values
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value, 3))
        .collect()
}

fn kleene_cases() -> Vec<(&'static str, Expression, Vec<Option<bool>>)> {
    vec![
        (
            "and",
            Expression::binary(
                BinaryOperator::And,
                Expression::column(0),
                Expression::column(1),
            ),
            vec![
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
                None,
            ],
        ),
        (
            "or",
            Expression::binary(
                BinaryOperator::Or,
                Expression::column(0),
                Expression::column(1),
            ),
            vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
                None,
            ],
        ),
        (
            "and_scalar_right",
            Expression::binary(
                BinaryOperator::And,
                Expression::column(0),
                Expression::literal(Literal::Boolean(None)),
            ),
            repeat_each([None, Some(false), None]),
        ),
        (
            "or_scalar_left",
            Expression::binary(
                BinaryOperator::Or,
                Expression::literal(Literal::Boolean(Some(false))),
                Expression::column(1),
            ),
            [Some(true), Some(false), None].repeat(3),
        ),
        (
            "not",
            Expression::unary(UnaryOperator::Not, Expression::column(0)),
            repeat_each([Some(false), Some(true), None]),
        ),
        (
            "null_type_is_null",
            Expression::unary(UnaryOperator::IsNull, Expression::column(2)),
            vec![Some(true); 9],
        ),
        (
            "non_null_is_null",
            Expression::unary(UnaryOperator::IsNull, Expression::column(3)),
            vec![Some(false); 9],
        ),
    ]
}

#[test]
fn equality_operators_cover_every_v1_scalar_type_and_propagate_null() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("boolean", DataType::Boolean, true),
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int64Array::from(vec![Some(-2), Some(3), None])),
            Arc::new(UInt64Array::from(vec![Some(7), Some(8), None])),
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 1, 1])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let operands = [
        (0, Literal::Boolean(Some(true))),
        (1, Literal::Int64(Some(-2))),
        (2, Literal::UInt64(Some(7))),
        (3, Literal::Utf8(Some("x".to_owned()))),
    ];

    for (column, literal) in operands {
        for (operator, expected) in [
            (BinaryOperator::Equal, [Some(true), Some(false), None]),
            (BinaryOperator::NotEqual, [Some(false), Some(true), None]),
        ] {
            for expression in [
                Expression::binary(
                    operator,
                    Expression::column(column),
                    Expression::literal(literal.clone()),
                ),
                Expression::binary(
                    operator,
                    Expression::literal(literal.clone()),
                    Expression::column(column),
                ),
            ] {
                let operation = stateless_operation(
                    &ExtendDefinition::new("result", expression),
                    Arc::clone(&schema),
                );
                let Action::Complete(Some(output)) = operation
                    .turn(Some(turn_input(&input)), transaction.access())
                    .unwrap()
                else {
                    panic!("comparison Extend returned the wrong action");
                };
                let actual = output
                    .records()
                    .column(4)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
    }

    for (operator, expected) in [
        (BinaryOperator::Equal, [Some(true), Some(true), None]),
        (BinaryOperator::NotEqual, [Some(false), Some(false), None]),
    ] {
        let operation = stateless_operation(
            &ExtendDefinition::new(
                "array_result",
                Expression::binary(operator, Expression::column(0), Expression::column(0)),
            ),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) = operation
            .turn(Some(turn_input(&input)), transaction.access())
            .unwrap()
        else {
            panic!("array comparison Extend returned the wrong action");
        };
        let actual = output
            .records()
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

fn predicate_change(values: &[u64], keep: &[Option<bool>], diffs: &[i64]) -> Change {
    assert_eq!(values.len(), keep.len());
    assert_eq!(values.len(), diffs.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(values.to_vec())),
            Arc::new(BooleanArray::from(keep.to_vec())),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn filter_trace(
    values: &[u64],
    keep: &[Option<bool>],
    diffs: &[i64],
    batches: &[usize],
) -> Vec<(u64, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), values.len());
    let schema = predicate_change(&values[..1], &keep[..1], &diffs[..1]).schema();
    let operation = stateless_operation(&FilterDefinition::new(Expression::column(1)), schema);
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let input = predicate_change(
            &values[start..start + rows],
            &keep[start..start + rows],
            &diffs[start..start + rows],
        );
        let transaction = transactions.begin().unwrap();
        match operation
            .turn(Some(turn_input(&input)), transaction.access())
            .unwrap()
        {
            Action::Complete(Some(change)) => {
                let values = change
                    .records()
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                output.extend(
                    values
                        .values()
                        .iter()
                        .copied()
                        .zip(change.diffs().values().iter().copied()),
                );
            }
            Action::Complete(None) => {}
            Action::Idle | Action::Commit(_) => panic!("Filter returned the wrong action"),
        }
        transaction.commit().unwrap();
        start += rows;
    }
    output
}

fn extend_trace(values: &[u64], diffs: &[i64], batches: &[usize]) -> Vec<(u64, Option<bool>, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), values.len());
    assert_eq!(values.len(), diffs.len());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let operation = stateless_operation(
        &ExtendDefinition::new(
            "seven",
            equal(
                Expression::column(0),
                Expression::literal(Literal::UInt64(Some(7))),
            ),
        ),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(
                values[start..start + rows].to_vec(),
            ))],
        )
        .unwrap();
        let input = Change::try_new(
            records,
            Int64Array::from(diffs[start..start + rows].to_vec()),
        )
        .unwrap();
        let transaction = transactions.begin().unwrap();
        let Action::Complete(Some(change)) = operation
            .turn(Some(turn_input(&input)), transaction.access())
            .unwrap()
        else {
            panic!("Extend returned the wrong action");
        };
        let derived = change
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let values = change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        output.extend(
            values
                .values()
                .iter()
                .copied()
                .zip(derived.iter())
                .zip(change.diffs().values().iter().copied())
                .map(|((value, derived), diff)| (value, derived, diff)),
        );
        transaction.commit().unwrap();
        start += rows;
    }
    output
}

#[test]
fn filter_and_extend_are_rebatch_invariant() {
    let values = [5, 7, 7, 9, 9, 11];
    let keep = [Some(false), Some(true), Some(true), None, None, Some(false)];
    let diffs = [1, 2, -1, 3, -2, 1];
    for batches in [&[6][..], &[2, 4], &[1, 1, 1, 1, 1, 1]] {
        assert_eq!(
            filter_trace(&values, &keep, &diffs, batches),
            [(7, 2), (7, -1)]
        );
        assert_eq!(
            extend_trace(&values, &diffs, batches),
            [
                (5, Some(false), 1),
                (7, Some(true), 2),
                (7, Some(true), -1),
                (9, Some(false), 3),
                (9, Some(false), -2),
                (11, Some(false), 1),
            ]
        );
    }
}

fn count_trace(diffs: &[i64], batches: &[usize]) -> Vec<u64> {
    assert_eq!(batches.iter().sum::<usize>(), diffs.len());
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let operation = CountOperation::new(store.create_data::<Cell<u64>>("count").unwrap());
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let input = change(&diffs[start..start + rows]);
        let transaction = transactions.begin().unwrap();
        output.extend(output_values(
            operation
                .turn(Some(turn_input(&input)), transaction.access())
                .unwrap(),
            ExpectedAction::Complete,
            "count",
        ));
        transaction.commit().unwrap();
        start += rows;
    }
    output
}

#[test]
fn count_trace_is_rebatch_invariant_and_overflow_is_atomic() {
    let diffs = [1, 1, 1, -1, 1];
    let expected = [1, 2, 3, 4, 5];
    for batches in [&[5][..], &[2, 3], &[1, 1, 1, 1, 1]] {
        assert_eq!(count_trace(&diffs, batches), expected);
    }

    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let state = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(state.clone());
    let input = change(&[1, 1]);
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        state
            .access(transaction.access())
            .unwrap()
            .set(&(u64::MAX - 1))
            .unwrap();
        transaction.commit().unwrap();
    }
    let transaction = transactions.begin().unwrap();
    let error = operation
        .turn(Some(turn_input(&input)), transaction.access())
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<CountError>(),
        Some(CountError::Overflow)
    ));
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX - 1)
    );
    transaction.commit().unwrap();
}

#[test]
fn count_preserves_persisted_bytes_when_state_codec_is_wrong() {
    let fixture = TestStore::new();
    let persisted = "not-a-u64".to_owned();
    let mut store = Store::create(fixture.path()).unwrap();
    let raw = store.create_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        raw.access(transaction.access())
            .unwrap()
            .set(&persisted)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let operation = CountOperation::new(store.open_data::<Cell<u64>>("count").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let change = change(&[1]);
    let error = operation
        .turn(Some(turn_input(&change)), transaction.access())
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Codec(_))
    ));
    drop(transaction);
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let raw = store.open_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        raw.access(transaction.access()).unwrap().get().unwrap(),
        Some(persisted)
    );
}
