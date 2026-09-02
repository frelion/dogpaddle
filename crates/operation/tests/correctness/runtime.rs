use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, ChangeProjection, ProjectionError};
use dogpaddle_operation::operation::{
    Action, Operation, OperationInput,
    sink::{DiscardError, DiscardOperation},
    source::{SequenceSourceError, SequenceSourceOperation},
    transform::{CountError, CountOperation, ProjectError, ProjectOperation},
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
