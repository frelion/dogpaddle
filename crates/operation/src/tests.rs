use std::collections::HashSet;

use dogpaddle_store::{Cell, DataPlacement, OrderedMap, Store};

use crate::{
    DataBindings, MaterializeError, OperationDefinition,
    codec::DECODERS,
    operation::{source::SequenceSourceDefinition, transform::CountDefinition},
};

#[test]
fn decoder_tags_and_data_names_are_unique() {
    let mut tags = HashSet::new();
    for (tag, _) in DECODERS {
        assert!(tags.insert(*tag), "duplicate operation decoder tag {tag}");
    }

    for definition in [
        &SequenceSourceDefinition::new(0) as &dyn OperationDefinition,
        &CountDefinition::new(),
    ] {
        assert!(tags.contains(&definition.persistence_tag()));
        let mut names = HashSet::new();
        for name in definition.data_names() {
            assert!(!name.is_empty());
            assert!(!name.as_bytes().contains(&0));
            assert!(
                names.insert(*name),
                "duplicate operation data name {name:?}"
            );
        }
    }
}

#[test]
fn data_bindings_resolve_typed_resources_by_name_not_position() {
    const COUNT: &str = "count";
    const STATE: &str = "state";

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = store
        .create_data("physical-count", DataPlacement::Shared)
        .unwrap();
    let state = store
        .create_data("physical-state", DataPlacement::Shared)
        .unwrap();

    let mut bindings = DataBindings::new();
    bindings.insert(STATE, state).unwrap();
    bindings.insert(COUNT, count.clone()).unwrap();
    assert_eq!(
        bindings.insert(COUNT, count).unwrap_err(),
        MaterializeError::DuplicateData { name: "count" }
    );

    let _count: Cell<u64> = bindings.take(COUNT, Cell::<u64>::new).unwrap();
    let _state: OrderedMap<Vec<u8>, Vec<u8>> = bindings
        .take(STATE, OrderedMap::<Vec<u8>, Vec<u8>>::new)
        .unwrap();
    bindings.finish().unwrap();
}

#[test]
fn data_bindings_reject_missing_and_unconsumed_names() {
    const COUNT: &str = "count";

    let mut missing = DataBindings::new();
    let Err(error) = missing.take(COUNT, Cell::<u64>::new) else {
        panic!("missing binding unexpectedly resolved");
    };
    assert_eq!(error, MaterializeError::MissingData { name: "count" });

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = store
        .create_data("physical-count", DataPlacement::Shared)
        .unwrap();
    let mut unconsumed = DataBindings::new();
    unconsumed.insert(COUNT, count).unwrap();
    assert_eq!(
        unconsumed.finish().unwrap_err(),
        MaterializeError::UnexpectedData { name: "count" }
    );
}
