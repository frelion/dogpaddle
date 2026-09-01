use std::collections::HashSet;

use dogpaddle_store::{Cell, Large, OrderedMap, Small, Store};

use crate::{
    DataInstances, MaterializeError, OperationDefinition, OperationKind,
    codec::DECODERS,
    definition::DataName,
    operation::{
        sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
    },
};

const COUNT: DataName<Cell<u64>> = DataName::new("count");
const STRING_COUNT: DataName<Cell<String>> = DataName::new("count");
const MAP_COUNT: DataName<OrderedMap<Vec<u8>, Vec<u8>, Small>> = DataName::new("count");
const SMALL_STATE: DataName<OrderedMap<Vec<u8>, Vec<u8>, Small>> = DataName::new("state");
const STATE: DataName<OrderedMap<Vec<u8>, Vec<u8>, Large>> = DataName::new("state");

fn builtin_definitions() -> [Box<dyn OperationDefinition>; 3] {
    [
        Box::new(SequenceSourceDefinition::new(0)),
        Box::new(CountDefinition::new()),
        Box::new(DiscardDefinition::new()),
    ]
}

#[test]
fn decoder_registry_exactly_matches_builtins() {
    let definitions = builtin_definitions();
    let expected_tags = definitions
        .iter()
        .map(|definition| definition.persistence_tag())
        .collect::<HashSet<_>>();
    assert_eq!(
        expected_tags.len(),
        definitions.len(),
        "duplicate built-in definition tag"
    );
    let registered_tags = DECODERS.iter().map(|(tag, _)| *tag).collect::<HashSet<_>>();
    assert_eq!(
        registered_tags.len(),
        DECODERS.len(),
        "duplicate decoder tag"
    );
    assert_eq!(registered_tags, expected_tags);
}

#[test]
fn builtin_kinds_match_their_structural_contracts() {
    let kinds = builtin_definitions().map(|definition| definition.kind());
    assert_eq!(
        kinds,
        [
            OperationKind::Source,
            OperationKind::Transform(std::num::NonZeroU32::MIN),
            OperationKind::Sink(std::num::NonZeroU32::MIN),
        ]
    );
}

#[test]
fn builtin_data_names_are_valid() {
    for definition in builtin_definitions() {
        let mut names = HashSet::new();
        for declaration in definition.data() {
            let name = declaration.name();
            assert!(!name.is_empty());
            assert!(!name.as_bytes().contains(&0));
            assert!(names.insert(name), "duplicate operation data name {name:?}");
        }
    }
}

#[test]
fn data_instances_resolve_typed_objects_by_name_not_insertion_order() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = COUNT
        .declaration()
        .create(&mut store, "physical-count")
        .unwrap();
    let state = STATE
        .declaration()
        .create(&mut store, "physical-state")
        .unwrap();

    let mut instances = DataInstances::new();
    instances.insert(state).unwrap();
    instances.insert(count).unwrap();

    let _count: Cell<u64> = instances.take(&COUNT).unwrap();
    let _state: OrderedMap<Vec<u8>, Vec<u8>, Large> = instances.take(&STATE).unwrap();
    instances.finish().unwrap();
}

#[test]
fn data_instances_reject_duplicate_names() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let first = COUNT
        .declaration()
        .create(&mut store, "physical-count-a")
        .unwrap();
    let duplicate = COUNT
        .declaration()
        .create(&mut store, "physical-count-b")
        .unwrap();

    let mut instances = DataInstances::new();
    instances.insert(first).unwrap();
    assert_eq!(
        instances.insert(duplicate).unwrap_err(),
        MaterializeError::DuplicateData { name: "count" }
    );
}

#[test]
fn data_instances_reject_missing_and_unconsumed_names() {
    let mut missing = DataInstances::new();
    let Err(error) = missing.take(&COUNT) else {
        panic!("missing data instance unexpectedly resolved");
    };
    assert_eq!(error, MaterializeError::MissingData { name: "count" });

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = COUNT
        .declaration()
        .create(&mut store, "physical-count")
        .unwrap();
    let mut unconsumed = DataInstances::new();
    unconsumed.insert(count).unwrap();
    assert_eq!(
        unconsumed.finish().unwrap_err(),
        MaterializeError::UnexpectedData { name: "count" }
    );
}

#[test]
fn data_instances_reject_the_wrong_data_class() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = COUNT
        .declaration()
        .create(&mut store, "physical-count")
        .unwrap();
    let mut instances = DataInstances::new();
    instances.insert(count).unwrap();

    let Err(error) = instances.take(&STRING_COUNT) else {
        panic!("u64 cell unexpectedly materialized as a string cell");
    };
    assert_eq!(error, MaterializeError::WrongDataClass { name: "count" });
}

#[test]
fn data_instances_reject_a_different_collection_with_the_same_layout() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let count = COUNT
        .declaration()
        .create(&mut store, "physical-count")
        .unwrap();
    let mut instances = DataInstances::new();
    instances.insert(count).unwrap();

    let Err(error) = instances.take(&MAP_COUNT) else {
        panic!("cell unexpectedly materialized as an ordered map with the same layout");
    };
    assert_eq!(error, MaterializeError::WrongDataClass { name: "count" });
}

#[test]
fn data_instances_reject_a_different_size_of_the_same_collection() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let state = SMALL_STATE
        .declaration()
        .create(&mut store, "physical-state")
        .unwrap();
    let mut instances = DataInstances::new();
    instances.insert(state).unwrap();

    let Err(error) = instances.take(&STATE) else {
        panic!("small map unexpectedly materialized as the large data class");
    };
    assert_eq!(error, MaterializeError::WrongDataClass { name: "state" });
}
