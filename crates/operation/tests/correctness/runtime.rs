use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray,
    UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use dogpaddle_change::{Change, ChangeProjection, ProjectionError};
use dogpaddle_operation::{
    DataInstances, Expr, ExpressionError, OperationDefinition, Operator, ScalarValue, cast, col,
    decode_definition, encode_definition, lit,
    operation::{
        Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
        sink::{DiscardError, DiscardOperation},
        source::{SequenceSourceError, SequenceSourceOperation},
        transform::{
            ExtendDefinition, ExtendError, FilterDefinition, FilterError, ProjectDefinition,
            ProjectError, ProjectOperation, RunningEventCountError, RunningEventCountOperation,
            SchemaAlignDefinition, SchemaAlignError, SchemaAlignField, SelectDefinition,
            SelectError, UnionAllDefinition, UnionAllError,
        },
    },
    try_cast,
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::{TestStore, commit_ready, rollback_ready};

#[test]
fn post_commit_error_accepts_an_already_erased_operation_error() {
    let source: OperationError = Box::new(std::io::Error::other("erased failure"));
    assert_eq!(PostCommitError::from(source).to_string(), "erased failure");
}

struct BorrowedDeliveryConnector {
    acknowledgements: Arc<AtomicUsize>,
}

impl BorrowedDeliveryConnector {
    fn poll(&mut self) -> BorrowedDelivery<'_> {
        BorrowedDelivery { connector: self }
    }
}

struct BorrowedDelivery<'connector> {
    connector: &'connector mut BorrowedDeliveryConnector,
}

impl BorrowedDelivery<'_> {
    fn ack(self) {
        self.connector
            .acknowledgements
            .fetch_add(1, Ordering::Relaxed);
    }
}

struct BorrowedDeliverySource {
    accepted: Cell<u64>,
    connector: BorrowedDeliveryConnector,
}

impl Operation for BorrowedDeliverySource {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        assert!(input.is_none());
        let accepted = self.accepted.clone();
        let delivery = self.connector.poll();
        Ok(Turn::ready(move |access| {
            accepted.access(access)?.set(&7)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    delivery.ack();
                    Ok(())
                }),
            ))
        }))
    }
}

#[test]
fn a_borrowed_delivery_crosses_the_transaction_and_is_only_acked_after_commit() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let accepted = store.create_data::<Cell<u64>>("accepted").unwrap();
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let mut operation = BorrowedDeliverySource {
        accepted: accepted.clone(),
        connector: BorrowedDeliveryConnector {
            acknowledgements: Arc::clone(&acknowledgements),
        },
    };
    let mut transactions = store.into_transactions();

    {
        let Turn::Ready(prepared) = operation.turn(None).unwrap() else {
            panic!("delivery source did not prepare its polled delivery");
        };
        let transaction = transactions.begin().unwrap();
        let (Action::Commit(None), after_commit) = prepared.apply(transaction.access()).unwrap()
        else {
            panic!("delivery source did not stage its checkpoint");
        };
        drop(transaction);
        drop(after_commit);
    }
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            accepted
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        transaction.commit().unwrap();
    }

    let Turn::Ready(prepared) = operation.turn(None).unwrap() else {
        panic!("delivery source did not prepare the replayed delivery");
    };
    let transaction = transactions.begin().unwrap();
    let (Action::Commit(None), after_commit) = prepared.apply(transaction.access()).unwrap() else {
        panic!("delivery source did not stage its replayed checkpoint");
    };
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    transaction.commit().unwrap();
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 0);
    after_commit.run().unwrap();
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 1);

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        accepted
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(7)
    );
    transaction.commit().unwrap();
}

fn change(diffs: &[i64]) -> Change {
    change_with_field_name("input", diffs)
}

fn change_with_field_name(field_name: &str, diffs: &[i64]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
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

fn comparison(operator: Operator, left: Expr, right: Expr) -> Expr {
    match operator {
        Operator::Eq => left.eq(right),
        Operator::NotEq => left.not_eq(right),
        _ => panic!("comparison helper received unsupported operator {operator}"),
    }
}

fn stateless_operation(
    definition: &dyn OperationDefinition,
    input_schema: arrow_schema::SchemaRef,
) -> Box<dyn Operation> {
    let data = DataInstances::new();
    definition
        .bind(&[input_schema])
        .unwrap()
        .materialize(data)
        .unwrap()
}

fn roundtripped_output(definition: &dyn OperationDefinition, input: &Change) -> Change {
    let encoded = encode_definition(definition);
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
    let mut operation = stateless_operation(decoded.as_ref(), input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("round-tripped stateless Operation did not complete with output");
    };
    output
}

fn temporal_and_decimal_change() -> Change {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new(
            "occurred_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                None,
                Some(2_500),
                Some(4_000),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(100), None, Some(300), Some(400), Some(500)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2, 3])).unwrap()
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
    let mut source = SequenceSourceOperation::new(41, position);
    let mut transform = RunningEventCountOperation::new(count);
    let mut sink = DiscardOperation;
    let input = change(&[2, -1]);
    let mut transactions = store.into_transactions();

    for commit in [false, true] {
        let source_action = if commit {
            commit_ready(&mut source, None, &mut transactions)
        } else {
            rollback_ready(&mut source, None, &mut transactions)
        }
        .unwrap();
        assert_eq!(
            output_values(source_action, ExpectedAction::Commit, "value"),
            [41]
        );
        let transform_action = if commit {
            commit_ready(&mut transform, Some(turn_input(&input)), &mut transactions)
        } else {
            rollback_ready(&mut transform, Some(turn_input(&input)), &mut transactions)
        }
        .unwrap();
        assert_eq!(
            output_values(transform_action, ExpectedAction::Complete, "count"),
            [1, 2]
        );
        let sink_action = if commit {
            commit_ready(&mut sink, Some(turn_input(&input)), &mut transactions)
        } else {
            rollback_ready(&mut sink, Some(turn_input(&input)), &mut transactions)
        }
        .unwrap();
        assert!(matches!(sink_action, Action::Complete(None)));
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let mut source =
        SequenceSourceOperation::new(41, store.open_data::<Cell<u64>>("position").unwrap());
    let count_state = store.open_data::<Cell<u64>>("count").unwrap();
    let mut transform = RunningEventCountOperation::new(count_state.clone());
    let mut transactions = store.into_transactions();
    assert_eq!(
        output_values(
            commit_ready(&mut source, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [42]
    );
    assert_eq!(
        output_values(
            commit_ready(&mut transform, Some(turn_input(&input)), &mut transactions,).unwrap(),
            ExpectedAction::Complete,
            "count",
        ),
        [3, 4]
    );
    let transaction = transactions.begin().unwrap();
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
    let mut source = SequenceSourceOperation::new(
        u64::MAX - 1,
        store.create_data::<Cell<u64>>("position").unwrap(),
    );
    let mut count =
        RunningEventCountOperation::new(store.create_data::<Cell<u64>>("count").unwrap());
    let mut sink = DiscardOperation;
    let input = change(&[1]);
    let mut transactions = store.into_transactions();

    for expected in [u64::MAX - 1, u64::MAX] {
        assert_eq!(
            output_values(
                commit_ready(&mut source, None, &mut transactions).unwrap(),
                ExpectedAction::Commit,
                "value",
            ),
            [expected]
        );
    }
    for _ in 0..2 {
        assert!(matches!(
            rollback_ready(&mut source, None, &mut transactions).unwrap(),
            Action::Idle
        ));
    }

    let source_error =
        rollback_ready(&mut source, Some(turn_input(&input)), &mut transactions).unwrap_err();
    assert!(matches!(
        source_error.downcast_ref::<SequenceSourceError>(),
        Some(SequenceSourceError::UnexpectedInput)
    ));

    let count_error = rollback_ready(&mut count, None, &mut transactions).unwrap_err();
    assert!(matches!(
        count_error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::MissingInput)
    ));
    let count_error = rollback_ready(
        &mut count,
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        count_error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::InvalidInputPort { port: 1 })
    ));

    let sink_error = rollback_ready(&mut sink, None, &mut transactions).unwrap_err();
    assert!(matches!(
        sink_error.downcast_ref::<DiscardError>(),
        Some(DiscardError::MissingInput)
    ));
    let sink_error = rollback_ready(
        &mut sink,
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        sink_error.downcast_ref::<DiscardError>(),
        Some(DiscardError::InvalidInputPort { port: 1 })
    ));

    drop(transactions);
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = Store::create(foreign_root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign.into_transactions();
    let error = rollback_ready(&mut source, None, &mut foreign_transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
    let error = rollback_ready(
        &mut count,
        Some(turn_input(&input)),
        &mut foreign_transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
}

#[test]
fn project_input_protocol_errors_are_exact() {
    let input = change(&[1]);
    let mut project =
        ProjectOperation::new(ChangeProjection::try_new(input.schema(), [0]).unwrap());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let missing = rollback_ready(&mut project, None, &mut transactions).unwrap_err();
    assert!(matches!(
        missing.downcast_ref::<ProjectError>(),
        Some(ProjectError::MissingInput)
    ));
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
        invalid_port.downcast_ref::<ProjectError>(),
        Some(ProjectError::InvalidInputPort { port: 1 })
    ));

    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "expected",
        DataType::UInt64,
        false,
    )]));
    let mut mismatched =
        ProjectOperation::new(ChangeProjection::try_new(expected_schema, [0]).unwrap());
    let error =
        rollback_ready(&mut mismatched, Some(turn_input(&input)), &mut transactions).unwrap_err();
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
    let mut operation = ProjectOperation::new(ChangeProjection::try_new(schema, [1]).unwrap());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
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
    let mut operation = stateless_operation(
        &FilterDefinition::try_new(col("keep")).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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
    let mut operation = stateless_operation(
        &FilterDefinition::try_new(col("keep")).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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
    let mut all_true = stateless_operation(
        &FilterDefinition::try_new(lit(true)).unwrap(),
        input.schema(),
    );
    let Action::Complete(Some(output)) = commit_ready(
        all_true.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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

    for predicate in [lit(false), lit(ScalarValue::Boolean(None))] {
        let mut operation = stateless_operation(
            &FilterDefinition::try_new(predicate).unwrap(),
            input.schema(),
        );
        assert!(matches!(
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&input)),
                &mut transactions,
            )
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
    let expression = col("flag")
        .and(lit(ScalarValue::Boolean(None)))
        .or(col("label").is_null());
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("selected", expression).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
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

    let mut copy = stateless_operation(
        &ExtendDefinition::try_new("label_copy", col("label")).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(copied)) =
        commit_ready(copy.as_mut(), Some(turn_input(&input)), &mut transactions).unwrap()
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
    let mut filter = stateless_operation(
        &FilterDefinition::try_new(lit(true)).unwrap(),
        input.schema(),
    );
    let mut extend = stateless_operation(
        &ExtendDefinition::try_new("copy", col("input")).unwrap(),
        input.schema(),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(filter.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::MissingInput)
    ));
    let error = rollback_ready(
        filter.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::InvalidInputPort { port: 1 })
    ));
    let error = rollback_ready(extend.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ExtendError>(),
        Some(ExtendError::MissingInput)
    ));
    let error = rollback_ready(
        extend.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
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
    for operation in [&mut filter, &mut extend] {
        let error = rollback_ready(
            operation.as_mut(),
            Some(turn_input(&drifted)),
            &mut transactions,
        )
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
fn temporal_and_decimal_direct_columns_cross_project_select_and_extend_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let schema = input.schema();

    let projected = roundtripped_output(&ProjectDefinition::new([0, 1, 2]), &input);
    assert_eq!(projected.schema(), schema);
    for index in 0..3 {
        assert!(Arc::ptr_eq(
            projected.records().column(index),
            input.records().column(index)
        ));
    }
    assert_eq!(
        projected.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    let select_definition = SelectDefinition::try_new([
        ("selected_amount", col("amount")),
        ("selected_date", col("date")),
        ("selected_time", col("occurred_at")),
    ])
    .unwrap();
    let selected = roundtripped_output(&select_definition, &input);
    assert_eq!(
        selected
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("selected_amount", &DataType::Decimal128(10, 2), true),
            ("selected_date", &DataType::Date32, false),
            (
                "selected_time",
                &DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]
    );
    for (output, input_index) in [(0, 2), (1, 0), (2, 1)] {
        assert!(Arc::ptr_eq(
            selected.records().column(output),
            input.records().column(input_index)
        ));
    }
    assert_eq!(
        selected.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    for (source, copy, input_index) in [
        ("date", "date_copy", 0),
        ("occurred_at", "occurred_at_copy", 1),
        ("amount", "amount_copy", 2),
    ] {
        let definition = ExtendDefinition::try_new(copy, col(source)).unwrap();
        let extended = roundtripped_output(&definition, &input);
        assert_eq!(extended.schema().field(3).name(), copy);
        assert_eq!(
            extended.schema().field(3).data_type(),
            schema.field(input_index).data_type()
        );
        assert_eq!(
            extended.schema().field(3).is_nullable(),
            schema.field(input_index).is_nullable()
        );
        assert!(Arc::ptr_eq(
            extended.records().column(3),
            input.records().column(input_index)
        ));
        assert_eq!(
            extended.diffs().values().as_ptr(),
            input.diffs().values().as_ptr()
        );
    }
}

#[test]
fn temporal_and_decimal_schema_align_executes_explicit_casts_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let align_definition = SchemaAlignDefinition::try_new([
        SchemaAlignField::try_new("aligned_date", col("date"), true).unwrap(),
        SchemaAlignField::try_new("aligned_time", col("occurred_at"), true).unwrap(),
        SchemaAlignField::try_new("aligned_amount", col("amount"), true).unwrap(),
        SchemaAlignField::try_new("date_days", cast(col("date"), DataType::Int32), false).unwrap(),
        SchemaAlignField::try_new(
            "time_millis",
            cast(col("occurred_at"), DataType::Int64),
            true,
        )
        .unwrap(),
        SchemaAlignField::try_new(
            "amount_rescaled",
            cast(col("amount"), DataType::Decimal128(12, 3)),
            true,
        )
        .unwrap(),
    ])
    .unwrap();
    let aligned = roundtripped_output(&align_definition, &input);
    for index in 0..3 {
        assert!(Arc::ptr_eq(
            aligned.records().column(index),
            input.records().column(index)
        ));
        assert!(aligned.schema().field(index).is_nullable());
    }
    assert_eq!(aligned.schema().field(3).data_type(), &DataType::Int32);
    assert!(!aligned.schema().field(3).is_nullable());
    assert_eq!(aligned.schema().field(4).data_type(), &DataType::Int64);
    assert!(aligned.schema().field(4).is_nullable());
    assert_eq!(
        aligned.schema().field(5).data_type(),
        &DataType::Decimal128(12, 3)
    );
    assert!(aligned.schema().field(5).is_nullable());
    let date_days = aligned
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(date_days.values(), &[0, 1, 2, 3, 4]);
    let time_millis = aligned
        .records()
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        time_millis.iter().collect::<Vec<_>>(),
        [Some(1_000), Some(2_000), None, Some(2_500), Some(4_000)]
    );
    let amount_rescaled = aligned
        .records()
        .column(5)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        amount_rescaled.iter().collect::<Vec<_>>(),
        [Some(1_000), None, Some(3_000), Some(4_000), Some(5_000)]
    );
    assert_eq!(
        aligned.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
}

#[test]
fn temporal_and_decimal_filter_comparisons_preserve_selected_order_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let predicate = col("date")
        .gt_eq(lit(ScalarValue::Date32(Some(0))))
        .and(col("occurred_at").lt(lit(ScalarValue::TimestampMillisecond(Some(3_000), None))))
        .and(col("amount").not_eq(lit(ScalarValue::Decimal128(Some(300), 10, 2))));
    let filter_definition = FilterDefinition::try_new(predicate).unwrap();
    let filtered = roundtripped_output(&filter_definition, &input);
    assert_eq!(filtered.diffs().values(), &[1, -2]);
    let dates = filtered
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.values(), &[0, 3]);
    let times = filtered
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(times.values(), &[1_000, 2_500]);
    let amounts = filtered
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[100, 400]);
}

#[test]
fn select_evaluates_ordered_expressions_and_shares_direct_columns_and_diffs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let definition =
        SelectDefinition::try_new([("copied", col("label")), ("next", col("id") + lit(1_u64))])
            .unwrap();
    let mut operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Select did not complete with one output Change");
    };
    assert_eq!(output.schema().field(0).name(), "copied");
    assert_eq!(output.schema().field(1).name(), "next");
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(1)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let next = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(next.values(), &[11, 21, 31]);
}

#[test]
fn schema_align_applies_explicit_schema_and_shares_direct_columns_and_diffs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let definition = SchemaAlignDefinition::try_new_with_metadata(
        [
            SchemaAlignField::try_new_with_metadata(
                "renamed_label",
                col("label"),
                true,
                HashMap::from([("role".to_owned(), "label".to_owned())]),
            )
            .unwrap(),
            SchemaAlignField::try_new("signed_id", cast(col("id"), DataType::Int64), true).unwrap(),
            SchemaAlignField::try_new("original_id", col("id"), false).unwrap(),
        ],
        HashMap::from([("normalized".to_owned(), "v1".to_owned())]),
    )
    .unwrap();
    let mut operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("SchemaAlign did not complete with one output Change");
    };
    assert_eq!(output.schema().metadata().get("normalized").unwrap(), "v1");
    assert_eq!(output.schema().field(0).name(), "renamed_label");
    assert_eq!(
        output.schema().field(0).metadata().get("role").unwrap(),
        "label"
    );
    assert_eq!(output.schema().field(1).data_type(), &DataType::Int64);
    assert!(output.schema().field(1).is_nullable());
    assert!(!output.schema().field(2).is_nullable());
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(1)
    ));
    assert!(Arc::ptr_eq(
        output.records().column(2),
        input.records().column(0)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let signed = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(signed.values(), &[10, 20, 30]);
}

#[test]
fn empty_schema_align_preserves_row_count_and_diffs_and_rejects_schema_drift() {
    let input = change(&[1, -1, 2]);
    let definition = SchemaAlignDefinition::try_new([]).unwrap();
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("empty SchemaAlign did not complete with one output Change");
    };
    assert_eq!(output.num_rows(), input.num_rows());
    assert!(output.schema().fields().is_empty());
    assert!(output.records().columns().is_empty());
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    let drifted = change_with_field_name("other", &[1, -1, 2]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InputSchemaMismatch)
    ));
}

#[test]
fn schema_align_rejects_missing_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let definition =
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("renamed", col("input"), false).unwrap()
        ])
        .unwrap();
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(operation.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::MissingInput)
    ));
    let error = rollback_ready(
        operation.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("other", &[1]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InputSchemaMismatch)
    ));
}

#[test]
fn empty_select_preserves_input_row_count_and_diffs_and_rejects_schema_drift() {
    let input = change(&[1, -1, 2]);
    let definition = SelectDefinition::try_new(std::iter::empty::<(&str, Expr)>()).unwrap();
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("empty Select did not complete with one output Change");
    };
    assert_eq!(output.num_rows(), input.num_rows());
    assert!(output.schema().fields().is_empty());
    assert!(output.records().columns().is_empty());
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    let drifted = change_with_field_name("other", &[1, -1, 2]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SelectError>(),
        Some(SelectError::InputSchemaMismatch)
    ));
}

#[test]
fn union_all_forwards_every_legal_port_without_copying() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let input = Change::try_new(
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, -1, 2]),
    )
    .unwrap();
    let definition = UnionAllDefinition::new(std::num::NonZeroU32::new(3).unwrap());
    let data = DataInstances::new();
    let mut operation = (&definition as &dyn OperationDefinition)
        .bind(&[input.schema(), input.schema(), input.schema()])
        .unwrap()
        .materialize(data)
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    for port in 0..3 {
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(OperationInput {
                port,
                change: &input,
            }),
            &mut transactions,
        )
        .unwrap() else {
            panic!("UnionAll did not forward input port {port}");
        };
        assert_eq!(output.schema(), input.schema());
        assert_eq!(output.num_rows(), input.num_rows());
        assert!(
            output
                .records()
                .columns()
                .iter()
                .zip(input.records().columns())
                .all(|(output, input)| Arc::ptr_eq(output, input))
        );
        assert_eq!(
            output.diffs().values().as_ptr(),
            input.diffs().values().as_ptr()
        );
    }

    let drifted = change_with_field_name("other", &[1, -1, 2]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &drifted,
        }),
        &mut transactions,
    )
    .unwrap_err();
    let Some(UnionAllError::InputSchemaMismatch {
        port,
        expected,
        actual,
    }) = error.downcast_ref::<UnionAllError>()
    else {
        panic!("UnionAll accepted a runtime Schema that differs from its binding");
    };
    assert_eq!(*port, 1);
    assert_eq!(expected.as_ref(), input.schema().as_ref());
    assert_eq!(actual.as_ref(), drifted.schema().as_ref());
}

#[test]
fn select_and_union_all_reject_missing_and_invalid_ports() {
    let input = change(&[1]);
    let mut select = stateless_operation(
        &SelectDefinition::try_new([("input", col("input"))]).unwrap(),
        input.schema(),
    );
    let union_definition = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    let data = DataInstances::new();
    let mut union = (&union_definition as &dyn OperationDefinition)
        .bind(&[input.schema(), input.schema()])
        .unwrap()
        .materialize(data)
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(select.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SelectError>(),
        Some(SelectError::MissingInput)
    ));
    let error = rollback_ready(
        select.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SelectError>(),
        Some(SelectError::InvalidInputPort { port: 1 })
    ));

    let error = rollback_ready(union.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<UnionAllError>(),
        Some(UnionAllError::MissingInput)
    ));
    let error = rollback_ready(
        union.as_mut(),
        Some(OperationInput {
            port: 2,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<UnionAllError>(),
        Some(UnionAllError::InvalidInputPort {
            port: 2,
            input_count: 2,
        })
    ));
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
    for (name, expression, expected) in kleene_cases() {
        let mut operation = stateless_operation(
            &ExtendDefinition::try_new(name, expression).unwrap(),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap() else {
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

fn kleene_cases() -> Vec<(&'static str, Expr, Vec<Option<bool>>)> {
    vec![
        (
            "and",
            col("left").and(col("right")),
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
            col("left").or(col("right")),
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
            col("left").and(lit(ScalarValue::Boolean(None))),
            repeat_each([None, Some(false), None]),
        ),
        (
            "or_scalar_left",
            lit(false).or(col("right")),
            [Some(true), Some(false), None].repeat(3),
        ),
        (
            "not",
            !col("left"),
            repeat_each([Some(false), Some(true), None]),
        ),
        (
            "null_type_is_null",
            col("nothing").is_null(),
            vec![Some(true); 9],
        ),
        (
            "non_null_is_null",
            col("number").is_null(),
            vec![Some(false); 9],
        ),
    ]
}

#[test]
fn equality_operators_cover_representative_scalar_types_and_propagate_null() {
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
    let operands = [
        ("boolean", ScalarValue::Boolean(Some(true))),
        ("signed", ScalarValue::Int64(Some(-2))),
        ("unsigned", ScalarValue::UInt64(Some(7))),
        ("text", ScalarValue::Utf8(Some("x".to_owned()))),
    ];

    for (column, literal) in operands {
        for (operator, expected) in [
            (Operator::Eq, [Some(true), Some(false), None]),
            (Operator::NotEq, [Some(false), Some(true), None]),
        ] {
            for expression in [
                comparison(operator, col(column), lit(literal.clone())),
                comparison(operator, lit(literal.clone()), col(column)),
            ] {
                let mut operation = stateless_operation(
                    &ExtendDefinition::try_new("result", expression).unwrap(),
                    Arc::clone(&schema),
                );
                let Action::Complete(Some(output)) = commit_ready(
                    operation.as_mut(),
                    Some(turn_input(&input)),
                    &mut transactions,
                )
                .unwrap() else {
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
        (Operator::Eq, [Some(true), Some(true), None]),
        (Operator::NotEq, [Some(false), Some(false), None]),
    ] {
        let mut operation = stateless_operation(
            &ExtendDefinition::try_new(
                "array_result",
                comparison(operator, col("boolean"), col("boolean")),
            )
            .unwrap(),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap() else {
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

#[test]
fn datafusion_arithmetic_comparison_and_casts_execute_vectorized() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![Some("10"), Some("bad")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let predicate = (cast(col("value"), DataType::Int64) + lit(1_i64)).gt(lit(8_i64));
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("greater", predicate).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("arithmetic expression did not produce an output");
    };
    let greater = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        greater.iter().collect::<Vec<_>>(),
        [Some(false), Some(true)]
    );

    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("parsed", try_cast(col("text"), DataType::Int64)).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("try-cast expression did not produce an output");
    };
    let parsed = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(parsed.iter().collect::<Vec<_>>(), [Some(10), None]);
}

fn structural_trace(
    definition: &dyn OperationDefinition,
    port: usize,
    rows: &[(u64, u64, i64)],
    batches: &[usize],
) -> Vec<(Vec<u64>, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), rows.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::UInt64, false),
        Field::new("right", DataType::UInt64, false),
    ]));
    let input_schemas = (0..definition.kind().input_count())
        .map(|_| Arc::clone(&schema))
        .collect::<Vec<_>>();
    let data = DataInstances::new();
    let mut operation = definition
        .bind(&input_schemas)
        .unwrap()
        .materialize(data)
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut trace = Vec::new();
    let mut start = 0;

    for &batch_rows in batches {
        let batch = &rows[start..start + batch_rows];
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from_iter_values(batch.iter().map(|row| row.0))),
                Arc::new(UInt64Array::from_iter_values(batch.iter().map(|row| row.1))),
            ],
        )
        .unwrap();
        let input = Change::try_new(
            records,
            Int64Array::from_iter_values(batch.iter().map(|row| row.2)),
        )
        .unwrap();
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(OperationInput {
                port,
                change: &input,
            }),
            &mut transactions,
        )
        .unwrap() else {
            panic!("structural Operation returned the wrong action");
        };
        for row in 0..output.num_rows() {
            let values = output
                .records()
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(row)
                })
                .collect();
            trace.push((values, output.diffs().value(row)));
        }
        start += batch_rows;
    }
    trace
}

#[test]
fn project_select_and_schema_align_preserve_flattened_records_and_diffs_across_rebatching() {
    let rows = [(1, 10, 1), (2, 20, -1), (3, 30, 2), (4, 40, -2)];
    let cases: [(&str, Box<dyn OperationDefinition>); 3] = [
        ("Project", Box::new(ProjectDefinition::new([1]))),
        (
            "Select",
            Box::new(
                SelectDefinition::try_new([
                    ("right", col("right")),
                    ("next", col("left") + lit(1_u64)),
                ])
                .unwrap(),
            ),
        ),
        (
            "SchemaAlign",
            Box::new(
                SchemaAlignDefinition::try_new([
                    SchemaAlignField::try_new("right", col("right"), false).unwrap(),
                    SchemaAlignField::try_new("left", col("left"), true).unwrap(),
                ])
                .unwrap(),
            ),
        ),
    ];

    for (name, definition) in cases {
        let expected = structural_trace(definition.as_ref(), 0, &rows, &[rows.len()]);
        for batches in [&[1, 3][..], &[2, 1, 1], &[1, 1, 1, 1]] {
            assert_eq!(
                structural_trace(definition.as_ref(), 0, &rows, batches),
                expected,
                "{name} changed its flattened trace after rebatching"
            );
        }
    }
}

#[test]
fn union_all_preserves_each_port_subsequence_across_rebatching() {
    let definition = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    let ports = [
        (0, &[(1, 10, 1), (2, 20, -1), (3, 30, 2)][..]),
        (1, &[(101, 110, -2), (102, 120, 3), (103, 130, 1)][..]),
    ];

    for (port, rows) in ports {
        let expected = structural_trace(&definition, port, rows, &[rows.len()]);
        for batches in [&[1, 2][..], &[2, 1], &[1, 1, 1]] {
            assert_eq!(
                structural_trace(&definition, port, rows, batches),
                expected,
                "UnionAll changed port {port}'s flattened subsequence after rebatching"
            );
        }
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
    let mut operation =
        stateless_operation(&FilterDefinition::try_new(col("keep")).unwrap(), schema);
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
        match commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
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
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("seven", col("value").eq(lit(7_u64))).unwrap(),
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
        let Action::Complete(Some(change)) = commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap() else {
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

fn running_event_count_trace(diffs: &[i64], batches: &[usize]) -> Vec<u64> {
    assert_eq!(batches.iter().sum::<usize>(), diffs.len());
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let mut operation =
        RunningEventCountOperation::new(store.create_data::<Cell<u64>>("count").unwrap());
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let input = change(&diffs[start..start + rows]);
        output.extend(output_values(
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap(),
            ExpectedAction::Complete,
            "count",
        ));
        start += rows;
    }
    output
}

#[test]
fn running_event_count_trace_is_rebatch_invariant_and_overflow_is_atomic() {
    let diffs = [1, 1, 1, -1, 1];
    let expected = [1, 2, 3, 4, 5];
    for batches in [&[5][..], &[2, 3], &[1, 1, 1, 1, 1]] {
        assert_eq!(running_event_count_trace(&diffs, batches), expected);
    }

    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let state = store.create_data::<Cell<u64>>("count").unwrap();
    let mut operation = RunningEventCountOperation::new(state.clone());
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
    let error =
        rollback_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::Overflow)
    ));
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX - 1)
    );
    transaction.commit().unwrap();
}

#[test]
fn running_event_count_preserves_persisted_bytes_when_state_codec_is_wrong() {
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
    let mut operation =
        RunningEventCountOperation::new(store.open_data::<Cell<u64>>("count").unwrap());
    let mut transactions = store.into_transactions();
    let change = change(&[1]);
    let error =
        rollback_ready(&mut operation, Some(turn_input(&change)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Codec(_))
    ));
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
