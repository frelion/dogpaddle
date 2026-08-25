use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationDefinition, encode_definition,
    operation::{
        Operation,
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{Cell, Small, Store};

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[test]
fn boxed_operations_preserve_their_concrete_definitions() {
    assert_send_sync_static::<Box<dyn Operation>>();
    assert_send_sync_static::<Box<dyn OperationDefinition>>();

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = CountOperation::new(
        CountDefinition::new(),
        store.create_data::<Cell<u64, Small>>("count").unwrap(),
    );
    let source_definition = SequenceSourceDefinition::new(42);
    let source = SequenceSourceOperation::new(
        source_definition,
        store.create_data::<Cell<u64, Small>>("position").unwrap(),
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
    let count_definition = CountDefinition::new();

    assert_eq!(
        source_definition
            .data()
            .iter()
            .map(|declaration| declaration.name())
            .collect::<Vec<_>>(),
        ["sequence_source.position"]
    );
    assert_eq!(
        count_definition
            .data()
            .iter()
            .map(|declaration| declaration.name())
            .collect::<Vec<_>>(),
        ["count"]
    );

    let mut source_data = DataInstances::new();
    let source_position = source_definition
        .data()
        .iter()
        .copied()
        .find(|declaration| declaration.name() == "sequence_source.position")
        .unwrap();
    source_data
        .insert(
            source_position
                .create(&mut store, "source-position")
                .unwrap(),
        )
        .unwrap();
    let source = source_definition.materialize(&mut source_data).unwrap();
    source_data.finish().unwrap();

    let mut count_data = DataInstances::new();
    let count_cell = count_definition
        .data()
        .iter()
        .copied()
        .find(|declaration| declaration.name() == "count")
        .unwrap();
    count_data
        .insert(count_cell.create(&mut store, "count").unwrap())
        .unwrap();
    let count = count_definition.materialize(&mut count_data).unwrap();
    count_data.finish().unwrap();
    assert_eq!(
        encode_definition(source.definition()),
        encode_definition(&source_definition)
    );
    assert_eq!(
        encode_definition(count.definition()),
        encode_definition(&count_definition)
    );

    let mut missing = DataInstances::new();
    let Err(error) = count_definition.materialize(&mut missing) else {
        panic!("count unexpectedly materialized without its named data binding");
    };
    assert_eq!(error, MaterializeError::MissingData { name: "count" });
}
