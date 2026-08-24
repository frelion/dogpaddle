use dogpaddle_operation::{SequenceSourceDefinition, SequenceSourceOperation};
use dogpaddle_store::{Cell, DataPlacement, OrderedMap, Store};

use super::{OperationInstance, Stage};

fn sequence_source(stage: &Stage) -> &SequenceSourceOperation {
    let OperationInstance::SequenceSource(operation) = &stage.operation else {
        panic!("sequence source was materialized as another operation");
    };
    operation
}

fn create_source_stage(
    store: &mut Store,
    name: &str,
    definition: SequenceSourceDefinition,
) -> Stage {
    Stage::sequence_source(
        OrderedMap::new(
            store
                .create_data(&format!("{name}-state"), DataPlacement::Shared)
                .unwrap(),
        ),
        definition,
        Cell::new(
            store
                .create_data(&format!("{name}-position"), DataPlacement::Shared)
                .unwrap(),
        ),
    )
}

fn open_source_stage(store: &Store, name: &str, definition: SequenceSourceDefinition) -> Stage {
    Stage::sequence_source(
        OrderedMap::new(store.open_data(&format!("{name}-state")).unwrap()),
        definition,
        Cell::new(store.open_data(&format!("{name}-position")).unwrap()),
    )
}

#[test]
fn construction_uses_only_injected_definitions_and_data_handles() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let left_definition = SequenceSourceDefinition::new(100);
    let right_definition = SequenceSourceDefinition::new(200);
    let left = create_source_stage(&mut store, "left", left_definition);
    let right = create_source_stage(&mut store, "right", right_definition);
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
        left.state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"left".to_vec())
            .unwrap();
        right
            .state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"right".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let left = open_source_stage(&store, "left", left_definition);
    let right = open_source_stage(&store, "right", right_definition);
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
                .state
                .access(&transaction)
                .unwrap()
                .get(&b"key".to_vec())
                .unwrap(),
            Some(expected_state)
        );
    }
}
