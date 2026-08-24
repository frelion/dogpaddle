use dogpaddle_operation::{
    CountData, CountDefinition, CountOperation, Operation, OperationDefinition, SequenceSourceData,
    SequenceSourceDefinition, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, DataPlacement, Store};

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[test]
fn boxed_operations_preserve_their_concrete_definitions() {
    assert_send_sync_static::<Box<dyn Operation>>();

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = CountOperation::new(
        CountDefinition::new(),
        CountData::new(Cell::new(
            store.create_data("count", DataPlacement::Shared).unwrap(),
        )),
    );
    let source_definition = SequenceSourceDefinition::new(42);
    let source = SequenceSourceOperation::new(
        source_definition,
        SequenceSourceData::new(Cell::new(
            store
                .create_data("position", DataPlacement::Shared)
                .unwrap(),
        )),
    );

    let operations: Vec<Box<dyn Operation>> = vec![Box::new(count), Box::new(source)];

    assert_eq!(
        operations[0].definition(),
        OperationDefinition::from(CountDefinition::new())
    );
    assert_eq!(
        operations[1].definition(),
        OperationDefinition::from(source_definition)
    );
}
