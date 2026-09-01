use dogpaddle_operation::{
    DataDeclaration, DataInstances, OperationDefinition, OperationKind,
    operation::{
        Operation, sink::DiscardDefinition, source::SequenceSourceDefinition,
        transform::CountDefinition,
    },
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
    physical_names: &[&str],
) -> Box<dyn Operation> {
    assert_eq!(definition.data().len(), physical_names.len());
    let mut data = DataInstances::new();
    for (declaration, physical_name) in definition.data().iter().zip(physical_names) {
        data.insert(declaration.open(store, physical_name).unwrap())
            .unwrap();
    }
    let operation = definition.materialize(&mut data).unwrap();
    data.finish().unwrap();
    operation
}

#[test]
fn definitions_expose_their_stable_public_contracts() {
    let source = SequenceSourceDefinition::new(42);
    assert_eq!(source.kind(), OperationKind::Source);
    assert_eq!(source.start(), 42);
    assert_eq!(names(&source), ["sequence_source.position"]);

    let count = CountDefinition::new();
    assert_eq!(
        count.kind(),
        OperationKind::Transform(std::num::NonZeroU32::MIN)
    );
    assert_eq!(names(&count), ["count"]);

    let discard = DiscardDefinition::new();
    assert_eq!(
        discard.kind(),
        OperationKind::Sink(std::num::NonZeroU32::MIN)
    );
    assert!(names(&discard).is_empty());
}

#[test]
fn declarations_create_reopen_and_materialize_their_exact_data_classes() {
    assert_send_sync_static::<Box<dyn Operation>>();
    assert_send_sync_static::<Box<dyn OperationDefinition>>();

    let fixture = TestStore::new();
    let source_definition = SequenceSourceDefinition::new(42);
    let count_definition = CountDefinition::new();
    let discard_definition = DiscardDefinition::new();

    let mut store = Store::create(fixture.path()).unwrap();
    source_definition.data()[0]
        .create(&mut store, "source-position")
        .unwrap();
    count_definition.data()[0]
        .create(&mut store, "count")
        .unwrap();
    drop(store);

    let store = Store::open(fixture.path()).unwrap();
    let source = materialize(&source_definition, &store, &["source-position"]);
    let count = materialize(&count_definition, &store, &["count"]);
    let discard = materialize(&discard_definition, &store, &[]);
    drop((source, count, discard));
}
