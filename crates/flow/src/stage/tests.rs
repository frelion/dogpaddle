use dogpaddle_operation::{
    encode_definition,
    operation::{
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{Cell, OrderedMap, Small, Store};

use super::Stage;

#[test]
fn construction_boxes_heterogeneous_operations_and_keeps_stage_state_isolated() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();

    let source_definition = SequenceSourceDefinition::new(100);
    let source = Stage::new(
        store
            .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("source-state")
            .unwrap(),
        Box::new(SequenceSourceOperation::new(
            source_definition,
            store
                .create_data::<Cell<u64, Small>>("source-position")
                .unwrap(),
        )),
    );

    let count_definition = CountDefinition::new();
    let count = Stage::new(
        store
            .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("count-state")
            .unwrap(),
        Box::new(CountOperation::new(
            count_definition,
            store
                .create_data::<Cell<u64, Small>>("count-value")
                .unwrap(),
        )),
    );

    assert_eq!(
        encode_definition(source.operation.definition()),
        encode_definition(&source_definition)
    );
    assert_eq!(
        encode_definition(count.operation.definition()),
        encode_definition(&count_definition)
    );

    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        source
            .state
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"source".to_vec())
            .unwrap();
        count
            .state
            .access(transaction.access())
            .unwrap()
            .put(&b"key".to_vec(), &b"count".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        source
            .state
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"source".to_vec())
    );
    assert_eq!(
        count
            .state
            .access(transaction.access())
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"count".to_vec())
    );
}
