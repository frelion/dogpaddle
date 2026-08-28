use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::operation::{
    InputProgress, Operation, OperationInput, TurnCommit, TurnDecision,
    transform::{CountDefinition, CountError, CountOperation},
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::TestStore;

fn input_change(diffs: Vec<i64>) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ignored",
        DataType::UInt64,
        false,
    )]));
    let values = vec![0_u64; diffs.len()];
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

fn count_values(decision: TurnDecision) -> Vec<u64> {
    let TurnDecision::Commit(TurnCommit {
        input: Some(InputProgress::Complete),
        output: Some(change),
    }) = decision
    else {
        panic!("Count did not complete its input and commit one output Change");
    };
    let schema = change.schema();
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    assert_eq!(field.name(), "count");
    assert_eq!(field.data_type(), &DataType::UInt64);
    assert!(!field.is_nullable());
    assert_eq!(
        change.diffs().values().as_ref(),
        vec![1_i64; change.num_rows()]
    );
    let values = change.records().column(0);
    let values = values.as_any().downcast_ref::<UInt64Array>().unwrap();
    (0..change.num_rows())
        .map(|index| values.value(index))
        .collect()
}

fn run_count(diffs: &[i64], segment_rows: &[usize]) -> Vec<u64> {
    assert_eq!(segment_rows.iter().sum::<usize>(), diffs.len());
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let count_state = count.clone();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();
    let mut flattened = Vec::new();
    let mut start = 0;

    for &rows in segment_rows {
        let change = input_change(diffs[start..start + rows].to_vec());
        let transaction = transactions.begin().unwrap();
        flattened.extend(count_values(
            operation
                .turn(
                    Some(OperationInput {
                        port: 0,
                        change: &change,
                    }),
                    transaction.access(),
                )
                .unwrap(),
        ));
        transaction.commit().unwrap();
        start += rows;
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count_state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(u64::try_from(diffs.len()).unwrap())
    );
    flattened
}

#[test]
fn count_finishes_a_complete_change_and_continues_after_reopen() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![2, -1, 7]);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            count_values(
                operation
                    .turn(
                        Some(OperationInput {
                            port: 0,
                            change: &change,
                        }),
                        transaction.access(),
                    )
                    .unwrap()
            ),
            [1, 2, 3]
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let count = store.open_data::<Cell<u64>>("count").unwrap();
    let count_state = count.clone();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1, 1]);
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            count_values(
                operation
                    .turn(
                        Some(OperationInput {
                            port: 0,
                            change: &change,
                        }),
                        transaction.access(),
                    )
                    .unwrap()
            ),
            [4, 5]
        );
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count_state
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(5)
    );
}

#[test]
fn count_is_invariant_to_stable_rebatching() {
    let diffs = [1, 1, 1, -1, 1];
    let expected = vec![1, 2, 3, 4, 5];

    assert_eq!(run_count(&diffs, &[5]), expected);
    assert_eq!(run_count(&diffs, &[2, 3]), expected);
    assert_eq!(run_count(&diffs, &[1, 1, 1, 1, 1]), expected);
}

#[test]
fn dropping_a_turn_rolls_back_the_complete_count_change() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1, 1]);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            count_values(
                operation
                    .turn(
                        Some(OperationInput {
                            port: 0,
                            change: &change,
                        }),
                        transaction.access(),
                    )
                    .unwrap()
            ),
            [1, 2]
        );
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count_values(
            operation
                .turn(
                    Some(OperationInput {
                        port: 0,
                        change: &change,
                    }),
                    transaction.access(),
                )
                .unwrap()
        ),
        [1, 2]
    );
    transaction.commit().unwrap();
}

#[test]
fn count_rejects_a_change_that_would_overflow_without_partial_progress() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let count_state = count.clone();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1, 1]);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        count_state
            .access(transaction.access())
            .unwrap()
            .set(&(u64::MAX - 1))
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let error = operation
            .turn(
                Some(OperationInput {
                    port: 0,
                    change: &change,
                }),
                transaction.access(),
            )
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CountError>(),
            Some(CountError::Overflow)
        ));
        assert_eq!(
            count_state
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(u64::MAX - 1)
        );
        transaction.commit().unwrap();
    }
}

#[test]
fn count_rejects_missing_input_and_nonzero_ports() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1]);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let error = operation.turn(None, transaction.access()).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CountError>(),
            Some(CountError::MissingInput)
        ));
    }
    {
        let transaction = transactions.begin().unwrap();
        let error = operation
            .turn(
                Some(OperationInput {
                    port: 1,
                    change: &change,
                }),
                transaction.access(),
            )
            .unwrap_err();
        assert!(matches!(
            *error.downcast::<CountError>().unwrap(),
            CountError::InvalidInputPort { port: 1 }
        ));
    }
}

#[test]
fn count_transparently_reports_wrong_store_access() {
    let root = tempfile::tempdir().unwrap();
    let mut owning_store = Store::create(root.path().join("owning")).unwrap();
    let count = owning_store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1]);

    let foreign_store = Store::create(root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign_store.into_transactions();
    let transaction = foreign_transactions.begin().unwrap();
    let error = operation
        .turn(
            Some(OperationInput {
                port: 0,
                change: &change,
            }),
            transaction.access(),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
}

#[test]
fn count_reports_a_persisted_wrong_codec_without_mutating_the_bytes() {
    let fixture = TestStore::new();
    let persisted = "not-a-u64".to_owned();
    let mut store = Store::create(fixture.path()).unwrap();
    let raw_count = store.create_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        raw_count
            .access(transaction.access())
            .unwrap()
            .set(&persisted)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let count = store.open_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let change = input_change(vec![1]);
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let error = operation
            .turn(
                Some(OperationInput {
                    port: 0,
                    change: &change,
                }),
                transaction.access(),
            )
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<StoreError>(),
            Some(StoreError::Codec(_))
        ));
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let raw_count = store.open_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        raw_count
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(persisted)
    );
}
