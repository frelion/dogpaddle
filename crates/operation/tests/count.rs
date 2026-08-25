use dogpaddle_operation::{
    OperationDefinition,
    operation::transform::{CountData, CountDefinition, CountError, CountOperation},
};
use dogpaddle_store::{Cell, DataPlacement, Store};

#[test]
fn count_definition_implements_the_shared_trait_with_one_input() {
    let definition = CountDefinition::new();

    assert_eq!(definition.input_count(), 1);
}

#[test]
fn count_starts_at_zero_and_continues_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let count = Cell::new(store.create_data("count", DataPlacement::Shared).unwrap());
    let operation = CountOperation::new(CountDefinition::new(), CountData::new(count));
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut count = operation.data().count().access(&transaction).unwrap();
        assert_eq!(operation.apply(&mut count).unwrap(), 1);
        assert_eq!(operation.apply(&mut count).unwrap(), 2);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let count = Cell::new(store.open_data("count").unwrap());
    let operation = CountOperation::new(CountDefinition::new(), CountData::new(count));
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut count = operation.data().count().access(&transaction).unwrap();
    assert_eq!(operation.apply(&mut count).unwrap(), 3);
    transaction.commit().unwrap();
}

#[test]
fn dropping_the_transaction_rolls_back_count_progress() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = Cell::new(store.create_data("count", DataPlacement::Shared).unwrap());
    let operation = CountOperation::new(CountDefinition::new(), CountData::new(count));
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        let mut count = operation.data().count().access(&transaction).unwrap();
        assert_eq!(operation.apply(&mut count).unwrap(), 1);
    }

    let transaction = transactions.begin().unwrap();
    let mut count = operation.data().count().access(&transaction).unwrap();
    assert_eq!(operation.apply(&mut count).unwrap(), 1);
    transaction.commit().unwrap();
}

#[test]
fn count_rejects_overflow_without_changing_the_cell() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = Cell::new(store.create_data("count", DataPlacement::Shared).unwrap());
    let operation = CountOperation::new(CountDefinition::new(), CountData::new(count));
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        operation
            .data()
            .count()
            .access(&transaction)
            .unwrap()
            .set(&u64::MAX)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let mut count = operation.data().count().access(&transaction).unwrap();
    assert!(matches!(
        operation.apply(&mut count),
        Err(CountError::Overflow)
    ));
    assert_eq!(count.get().unwrap(), Some(u64::MAX));
}
