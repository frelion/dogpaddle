use dogpaddle_operation::{
    DataDeclaration, DataInstances, OperationDefinition, encode_definition,
    operation::{Operation, source::SequenceSourceDefinition, transform::CountDefinition},
};
use dogpaddle_store::Store;

use super::support::TestStore;

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

fn names(definition: &dyn OperationDefinition) -> Vec<&'static str> {
    definition
        .data()
        .iter()
        .map(DataDeclaration::name)
        .collect()
}

fn materialize(
    definition: &dyn OperationDefinition,
    store: &Store,
    physical_name: &str,
) -> Box<dyn Operation> {
    let declaration = definition.data().first().unwrap();
    let mut data = DataInstances::new();
    data.insert(declaration.open(store, physical_name).unwrap())
        .unwrap();
    let operation = definition.materialize(&mut data).unwrap();
    data.finish().unwrap();
    operation
}

#[test]
fn definitions_expose_their_stable_public_contracts() {
    let source = SequenceSourceDefinition::new(42);
    assert_eq!(source.start(), 42);
    assert_eq!(source.input_count(), 0);
    assert_eq!(names(&source), ["sequence_source.position"]);

    let count = CountDefinition::new();
    assert_eq!(count.input_count(), 1);
    assert_eq!(names(&count), ["count"]);
}

#[test]
fn declarations_create_reopen_and_materialize_their_exact_data_classes() {
    assert_send_sync_static::<Box<dyn Operation>>();
    assert_send_sync_static::<Box<dyn OperationDefinition>>();

    let fixture = TestStore::new();
    let source_definition = SequenceSourceDefinition::new(42);
    let count_definition = CountDefinition::new();

    let mut store = Store::create(fixture.path()).unwrap();
    source_definition.data()[0]
        .create(&mut store, "source-position")
        .unwrap();
    count_definition.data()[0]
        .create(&mut store, "count")
        .unwrap();
    drop(store);

    let store = Store::open(fixture.path()).unwrap();
    let source = materialize(&source_definition, &store, "source-position");
    let count = materialize(&count_definition, &store, "count");

    assert_eq!(
        encode_definition(source.definition()),
        encode_definition(&source_definition)
    );
    assert_eq!(
        encode_definition(count.definition()),
        encode_definition(&count_definition)
    );
}
