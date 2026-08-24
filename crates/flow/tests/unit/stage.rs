use dogpaddle_operation::{OperationDefinition, SequenceSourceDefinition, SequenceSourceOperation};
use dogpaddle_store::Store;

use super::{OperationInstance, Stage};

fn sequence_source(stage: &Stage) -> &SequenceSourceOperation {
    let OperationInstance::SequenceSource(operation) = &stage.operation else {
        panic!("sequence source was materialized as another operation");
    };
    operation
}

#[test]
fn open_rematerializes_each_definition_and_its_own_data_handles() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let left_definition = OperationDefinition::from(SequenceSourceDefinition::new(100));
    let right_definition = OperationDefinition::from(SequenceSourceDefinition::new(200));
    let left = Stage::create(&mut store, 0, &left_definition).unwrap();
    let right = Stage::create(&mut store, 1, &right_definition).unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        sequence_source(&left)
            .data()
            .position()
            .access(&transaction)
            .unwrap()
            .set(&10)
            .unwrap();
        sequence_source(&right)
            .data()
            .position()
            .access(&transaction)
            .unwrap()
            .set(&20)
            .unwrap();
        left.data
            .state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"left".to_vec())
            .unwrap();
        right
            .data
            .state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"right".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let left = Stage::open(&store, 0, &left_definition).unwrap();
    let right = Stage::open(&store, 1, &right_definition).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    for (stage, expected_start, expected_position, expected_state) in [
        (&left, 100, 10, b"left".to_vec()),
        (&right, 200, 20, b"right".to_vec()),
    ] {
        let operation = sequence_source(stage);
        assert_eq!(operation.definition().start(), expected_start);
        assert_eq!(
            operation
                .data()
                .position()
                .access(&transaction)
                .unwrap()
                .get()
                .unwrap(),
            Some(expected_position)
        );
        assert_eq!(
            stage
                .data
                .state
                .access(&transaction)
                .unwrap()
                .get(&b"key".to_vec())
                .unwrap(),
            Some(expected_state)
        );
    }
}
