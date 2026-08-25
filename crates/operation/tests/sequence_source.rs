use dogpaddle_operation::operation::source::{
    SequenceSourceData, SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, DataPlacement, Store};

#[test]
fn sequence_source_starts_at_the_definition_and_continues_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let position = Cell::new(
        store
            .create_data("position", DataPlacement::Shared)
            .unwrap(),
    );
    let operation = SequenceSourceOperation::new(
        SequenceSourceDefinition::new(41),
        SequenceSourceData::new(position),
    );
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut position = operation.data().position().access(&transaction).unwrap();
        assert_eq!(operation.apply(&mut position).unwrap(), 41);
        assert_eq!(operation.apply(&mut position).unwrap(), 42);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let position = Cell::new(store.open_data("position").unwrap());
    let operation = SequenceSourceOperation::new(
        SequenceSourceDefinition::new(41),
        SequenceSourceData::new(position),
    );
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut position = operation.data().position().access(&transaction).unwrap();
    assert_eq!(operation.apply(&mut position).unwrap(), 43);
    transaction.commit().unwrap();
}

#[test]
fn sequence_source_emits_u64_max_once_and_then_reports_exhaustion() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let position = Cell::new(
        store
            .create_data("position", DataPlacement::Shared)
            .unwrap(),
    );
    let operation = SequenceSourceOperation::new(
        SequenceSourceDefinition::new(u64::MAX),
        SequenceSourceData::new(position),
    );
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut position = operation.data().position().access(&transaction).unwrap();
        assert_eq!(operation.apply(&mut position).unwrap(), u64::MAX);
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let mut position = operation.data().position().access(&transaction).unwrap();
    assert!(matches!(
        operation.apply(&mut position),
        Err(SequenceSourceError::Exhausted)
    ));
    assert_eq!(position.get().unwrap(), Some(u64::MAX));
}
