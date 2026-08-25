use dogpaddle_operation::operation::source::{
    SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, Store};

#[test]
fn sequence_source_starts_at_the_definition_and_continues_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::create(&path).unwrap();
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

    let store = Store::open(&path).unwrap();
    let position = store.open_data::<Cell<u64>>("position").unwrap();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(41), position);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(operation.step(transaction.access()).unwrap(), 43);
    transaction.commit().unwrap();
}

#[test]
fn sequence_source_emits_u64_max_once_and_then_reports_exhaustion() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let position_state = position.clone();
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(u64::MAX), position);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), u64::MAX);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    assert!(matches!(
        operation.step(access),
        Err(SequenceSourceError::Exhausted)
    ));
    assert_eq!(
        position_state.access(access).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
}
