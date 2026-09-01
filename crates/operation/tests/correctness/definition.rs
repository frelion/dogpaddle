use arrow_array::UInt64Array;
use dogpaddle_operation::{
    DataDeclaration, DataInstances, OperationDefinition, OperationKind,
    operation::{
        Action, Operation, OperationInput, sink::DiscardDefinition,
        source::SequenceSourceDefinition, transform::CountDefinition,
    },
};
use dogpaddle_store::{Cell, Store};

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
    let source_position = store.open_data::<Cell<u64>>("source-position").unwrap();
    let source = materialize(&source_definition, &store, &["source-position"]);
    let count = materialize(&count_definition, &store, &["count"]);
    let discard = materialize(&discard_definition, &store, &[]);
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let Action::Commit(Some(output)) = source.turn(None, transaction.access()).unwrap() else {
        panic!("materialized SequenceSource did not commit one output Change");
    };
    assert_eq!(output.num_rows(), 1);
    let values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.value(0), 42);
    assert_eq!(
        source_position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(42)
    );
    let Action::Complete(Some(count_output)) = count
        .turn(
            Some(OperationInput {
                port: 0,
                change: &output,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("materialized Count did not complete one input with output");
    };
    assert_eq!(count_output.num_rows(), 1);
    let Action::Complete(None) = discard
        .turn(
            Some(OperationInput {
                port: 0,
                change: &output,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("materialized Discard did not complete one input without output");
    };
    transaction.commit().unwrap();
    drop((source, count, discard));
}
