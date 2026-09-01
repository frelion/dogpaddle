use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::operation::{
    Action, Operation, OperationInput,
    sink::{DiscardError, DiscardOperation},
};
use dogpaddle_store::Store;

use super::support::TestStore;

fn input_change() -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ignored",
        DataType::UInt64,
        false,
    )]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![7, 8]))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap()
}

#[test]
fn discard_completes_one_input_without_output() {
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let operation = DiscardOperation;
    let change = input_change();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let decision = operation
        .turn(
            Some(OperationInput {
                port: 0,
                change: &change,
            }),
            transaction.access(),
        )
        .unwrap();

    assert!(matches!(decision, Action::Complete(None)));
    transaction.commit().unwrap();
}

#[test]
fn discard_rejects_missing_input_and_nonzero_ports() {
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let operation = DiscardOperation;
    let change = input_change();
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let error = operation.turn(None, transaction.access()).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DiscardError>(),
            Some(DiscardError::MissingInput)
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
            *error.downcast::<DiscardError>().unwrap(),
            DiscardError::InvalidInputPort { port: 1 }
        ));
    }
}
