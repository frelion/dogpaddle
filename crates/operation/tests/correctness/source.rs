use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::operation::{
    Operation, OperationInput,
    source::{SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation},
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::TestStore;

fn input_change() -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1])).unwrap()
}

fn source_values(output: Option<Change>) -> Vec<u64> {
    let change = output.expect("SequenceSource did not return one output Change");
    let schema = change.schema();
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    assert_eq!(field.name(), "value");
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

#[test]
fn sequence_source_emits_one_value_and_continues_after_reopen() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            source_values(operation.turn(None, transaction.access()).unwrap()),
            [41]
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let position = store.open_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        source_values(operation.turn(None, transaction.access()).unwrap()),
        [42]
    );
    transaction.commit().unwrap();
}

#[test]
fn dropping_the_transaction_rolls_back_sequence_source_progress_and_output() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            source_values(operation.turn(None, transaction.access()).unwrap()),
            [41]
        );
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        source_values(operation.turn(None, transaction.access()).unwrap()),
        [41]
    );
    transaction.commit().unwrap();
}

#[test]
fn sequence_source_emits_u64_max_once_then_reports_exhaustion() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation =
        SequenceSourceOperation::new(SequenceSourceDefinition::new(u64::MAX - 1), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            source_values(operation.turn(None, transaction.access()).unwrap()),
            [u64::MAX - 1]
        );
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            source_values(operation.turn(None, transaction.access()).unwrap()),
            [u64::MAX]
        );
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let error = operation.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SequenceSourceError>(),
        Some(SequenceSourceError::Exhausted)
    ));
}

#[test]
fn sequence_source_rejects_an_input_change_with_a_downcastable_error() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(0), position);
    let mut transactions = store.into_transactions();
    let change = input_change();
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
    assert!(error.is::<SequenceSourceError>());
    assert!(matches!(
        *error.downcast::<SequenceSourceError>().unwrap(),
        SequenceSourceError::UnexpectedInput
    ));
}

#[test]
fn sequence_source_transparently_reports_wrong_store_access() {
    let root = tempfile::tempdir().unwrap();
    let mut owning_store = Store::create(root.path().join("owning")).unwrap();
    let position = owning_store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(0), position);

    let foreign_store = Store::create(root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign_store.into_transactions();
    let transaction = foreign_transactions.begin().unwrap();
    let error = operation.turn(None, transaction.access()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SequenceSourceError>(),
        Some(SequenceSourceError::Store(StoreError::WrongStore))
    ));
}
