use dogpaddle_operation::{
    MaterializeError, OperationDefinition, encode_definition,
    operation::{
        Operation,
        source::{SequenceSourceData, SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountData, CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{Cell, DataPlacement, Store};

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[test]
fn boxed_operations_preserve_their_concrete_definitions() {
    assert_send_sync_static::<Box<dyn Operation>>();
    assert_send_sync_static::<Box<dyn OperationDefinition>>();

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
        encode_definition(operations[0].definition()),
        encode_definition(&CountDefinition::new())
    );
    assert_eq!(
        encode_definition(operations[1].definition()),
        encode_definition(&source_definition)
    );
}

#[test]
fn definitions_materialize_exact_declared_data_shapes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let source_definition = SequenceSourceDefinition::new(42);
    let source_position = store
        .create_data("source-position", DataPlacement::Shared)
        .unwrap();
    let count_definition = CountDefinition::new();
    let count = store.create_data("count", DataPlacement::Shared).unwrap();

    assert_eq!(source_definition.data_names(), ["sequence_source.position"]);
    assert_eq!(count_definition.data_names(), ["count"]);

    let source = source_definition
        .materialize(vec![source_position])
        .unwrap();
    let count = count_definition.materialize(vec![count]).unwrap();
    assert_eq!(
        encode_definition(source.definition()),
        encode_definition(&source_definition)
    );
    assert_eq!(
        encode_definition(count.definition()),
        encode_definition(&count_definition)
    );

    assert!(matches!(
        count_definition.materialize(Vec::new()),
        Err(MaterializeError::DataCount {
            expected: 1,
            actual: 0,
        })
    ));
}
