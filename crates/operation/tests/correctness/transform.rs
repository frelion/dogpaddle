use std::panic::{AssertUnwindSafe, catch_unwind};

use dogpaddle_operation::operation::transform::{CountDefinition, CountError, CountOperation};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::TestStore;

#[test]
fn count_starts_at_zero_and_continues_after_reopen() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), 1);
        assert_eq!(operation.step(transaction.access()).unwrap(), 2);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let count = store.open_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(operation.step(transaction.access()).unwrap(), 3);
    transaction.commit().unwrap();
}

#[test]
fn dropping_the_transaction_rolls_back_count_progress() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(operation.step(transaction.access()).unwrap(), 1);
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(operation.step(transaction.access()).unwrap(), 1);
    transaction.commit().unwrap();
}

#[test]
fn count_overflow_can_commit_without_changing_persisted_state() {
    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let count_state = count.clone();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin().unwrap();
        count_state
            .access(transaction.access())
            .unwrap()
            .set(&u64::MAX)
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        assert!(matches!(
            operation.step(transaction.access()),
            Err(CountError::Overflow)
        ));
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let count = store.open_data::<Cell<u64>>("count").unwrap();
    let count_state = count.clone();
    let operation = CountOperation::new(CountDefinition::new(), count);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = transaction.access();
    assert_eq!(
        count_state.access(access).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
    assert!(matches!(operation.step(access), Err(CountError::Overflow)));
}

#[test]
fn count_transparently_reports_wrong_store_access() {
    let root = tempfile::tempdir().unwrap();
    let mut owning_store = Store::create(root.path().join("owning")).unwrap();
    let count = owning_store.create_data::<Cell<u64>>("count").unwrap();
    let operation = CountOperation::new(CountDefinition::new(), count);

    let foreign_store = Store::create(root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign_store.into_transactions();
    let transaction = foreign_transactions.begin().unwrap();
    assert!(matches!(
        operation.step(transaction.access()),
        Err(CountError::Store(StoreError::WrongStore))
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
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let result = catch_unwind(AssertUnwindSafe(|| operation.step(transaction.access())));
        assert!(
            matches!(result, Ok(Err(CountError::Store(StoreError::Codec(_))))),
            "wrong-codec step should return the wrapped Store codec error"
        );
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
