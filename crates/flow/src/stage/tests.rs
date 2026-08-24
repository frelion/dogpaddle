use dogpaddle_operation::{
    CountData, CountDefinition, CountOperation, OperationDefinition, SequenceSourceData,
    SequenceSourceDefinition, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, DataPlacement, OrderedMap, Store};

use super::Stage;

#[test]
fn construction_boxes_heterogeneous_operations_and_keeps_stage_state_isolated() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();

    let source_definition = SequenceSourceDefinition::new(100);
    let source = Stage::new(
        OrderedMap::new(
            store
                .create_data("source-state", DataPlacement::Shared)
                .unwrap(),
        ),
        SequenceSourceOperation::new(
            source_definition,
            SequenceSourceData::new(Cell::new(
                store
                    .create_data("source-position", DataPlacement::Shared)
                    .unwrap(),
            )),
        ),
    );

    let count_definition = CountDefinition::new();
    let count = Stage::new(
        OrderedMap::new(
            store
                .create_data("count-state", DataPlacement::Shared)
                .unwrap(),
        ),
        CountOperation::new(
            count_definition,
            CountData::new(Cell::new(
                store
                    .create_data("count-value", DataPlacement::Shared)
                    .unwrap(),
            )),
        ),
    );

    assert_eq!(
        source.operation.definition(),
        OperationDefinition::from(source_definition)
    );
    assert_eq!(
        count.operation.definition(),
        OperationDefinition::from(count_definition)
    );

    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        source
            .state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"source".to_vec())
            .unwrap();
        count
            .state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"count".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        source
            .state
            .access(&transaction)
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"source".to_vec())
    );
    assert_eq!(
        count
            .state
            .access(&transaction)
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"count".to_vec())
    );
}
