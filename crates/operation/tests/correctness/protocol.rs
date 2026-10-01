use std::{borrow::Cow, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use dogpaddle_change::Change;
use dogpaddle_operation::operation::{
    AtomicOperation, BudgetExceeded, Operation, OperationError, OperationInput, Progress, Resume,
    StepBudget,
};
use dogpaddle_store::{Cell, Store, StoreSetup, StoreValue, TransactionAccess};

use super::support::{TestStore, value_schema};

struct Counter {
    count: Cell<u64>,
}

impl AtomicOperation for Counter {
    fn apply(
        &self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Option<Change>, OperationError> {
        let mut count = self.count.access(access)?;
        let current = count.get()?.unwrap_or(0);
        count.set(&(current + u64::try_from(input.change.num_rows())?))?;
        budget.charge(input.change.num_rows() * 8)?;
        Ok(Some(input.change.clone()))
    }
}

fn input() -> Change {
    let records = RecordBatch::try_new(
        value_schema(),
        vec![Arc::new(UInt64Array::from((0..5).collect::<Vec<_>>()))],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1; 5])).unwrap()
}

#[test]
fn caller_owned_resume_and_state_rollback_together_and_reopen_between_pages() {
    let root = TestStore::new();
    let mut store = StoreSetup::new();
    let count = store.create_data::<Cell<u64>>("count").unwrap();
    let operation = Operation::Atomic(Box::new(Counter { count }));
    let mut transactions = store.commit(root.path(), |_| Ok(())).unwrap();
    let input = input();
    let offered = OperationInput {
        port: 0,
        change: &input,
    };
    let initial = operation.initial_resume();
    {
        let transaction = transactions.begin();
        let error = operation
            .step(
                offered,
                &initial,
                transaction.access(),
                &mut StepBudget::new(2, 1),
            )
            .unwrap_err();
        assert!(error.is::<BudgetExceeded>());
        drop(transaction);
    }
    let next = {
        let transaction = transactions.begin();
        let step = operation
            .step(
                offered,
                &initial,
                transaction.access(),
                &mut StepBudget::new(2, 16),
            )
            .unwrap();
        assert_eq!(step.output.unwrap().num_rows(), 2);
        let Progress::More(next) = step.progress else {
            panic!("two events must leave a next page")
        };
        assert_ne!(initial, next);
        transaction.commit().unwrap();
        next
    };
    drop(operation);
    drop(transactions);
    let encoded = next.encode_value().unwrap().as_ref().to_vec();
    let mut resume = Resume::decode_value(Cow::Borrowed(&encoded)).unwrap();
    assert_eq!(resume, next);
    let store = Store::open(root.path()).unwrap();
    let count = store.open_data::<Cell<u64>>("count").unwrap();
    let operation = Operation::Atomic(Box::new(Counter {
        count: count.clone(),
    }));
    let mut transactions = store.into_transactions();
    loop {
        let transaction = transactions.begin();
        let step = operation
            .step(
                offered,
                &resume,
                transaction.access(),
                &mut StepBudget::new(2, 16),
            )
            .unwrap();
        transaction.commit().unwrap();
        match step.progress {
            Progress::More(next) => resume = next,
            Progress::Done => break,
        }
    }
    let transaction = transactions.begin();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(5)
    );
    transaction.commit().unwrap();
}
