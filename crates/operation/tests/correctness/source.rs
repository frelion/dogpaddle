use dogpaddle_operation::operation::source::{
    SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::TestStore;

#[test]
fn sequence_source_starts_at_the_definition_and_continues_after_reopen() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), 41);
        assert_eq!(operation.step(transaction.access()).unwrap(), 42);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let position = store.open_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(operation.step(transaction.access()).unwrap(), 43);
    transaction.commit().unwrap();
}

#[test]
fn dropping_the_transaction_rolls_back_sequence_source_progress() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), 41);
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(operation.step(transaction.access()).unwrap(), 41);
    transaction.commit().unwrap();
}

#[test]
fn sequence_source_exhaustion_can_commit_without_changing_persisted_position() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(u64::MAX), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), u64::MAX);
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        assert!(matches!(
            operation.step(transaction.access()),
            Err(SequenceSourceError::Exhausted)
        ));
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let position = store.open_data::<Cell<u64>>("position").unwrap();
    let position_state = position.clone();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(u64::MAX), position);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    assert_eq!(
        position_state.access(access).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert!(matches!(
        operation.step(access),
        Err(SequenceSourceError::Exhausted)
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
    assert!(matches!(
        operation.step(transaction.access()),
        Err(SequenceSourceError::Store(StoreError::WrongStore))
    ));
}
