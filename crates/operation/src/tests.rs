use std::{collections::HashSet, num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_store::{Cell, Large, OrderedMap, Small, Store};

use crate::{
    DataDeclaration, DataInstances, Expression, Literal, MaterializeError, OperationBindError,
    OperationBinding, OperationDefinition, OperationKind, OperationSchemaError,
    codec::DECODERS,
    definition::{DataName, Sealed},
    operation::{
        Operation,
        sink::DiscardDefinition,
        sink::DiscardOperation,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ExtendDefinition, FilterDefinition, ProjectDefinition},
    },
};

const COUNT: DataName<Cell<u64>> = DataName::new("count");
const STRING_COUNT: DataName<Cell<String>> = DataName::new("count");
const MAP_COUNT: DataName<OrderedMap<Vec<u8>, Vec<u8>, Small>> = DataName::new("count");
const SMALL_STATE: DataName<OrderedMap<Vec<u8>, Vec<u8>, Small>> = DataName::new("state");
const STATE: DataName<OrderedMap<Vec<u8>, Vec<u8>, Large>> = DataName::new("state");

#[derive(Clone, Copy, Debug)]
enum TestBinding {
    Rejected,
    MissingOutput,
    UnexpectedOutput,
    InvalidOutput,
}

#[derive(Clone, Copy, Debug)]
struct TestDefinition {
    kind: OperationKind,
    binding: TestBinding,
}

impl Sealed for TestDefinition {
    fn bind_schemas(
        &self,
        _input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let output = match self.binding {
            TestBinding::Rejected => {
                return Err(std::io::Error::other("rejected by test definition").into());
            }
            TestBinding::MissingOutput => None,
            TestBinding::UnexpectedOutput => Some(valid_schema()),
            TestBinding::InvalidOutput => Some(invalid_schema()),
        };
        Ok(OperationBinding::new(
            output,
            |_data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                Ok(Box::new(DiscardOperation))
            },
        ))
    }
}

impl OperationDefinition for TestDefinition {
    fn kind(&self) -> OperationKind {
        self.kind
    }

    fn data(&self) -> &'static [DataDeclaration] {
        &[]
    }

    fn persistence_tag(&self) -> u16 {
        u16::MAX
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

fn builtin_definitions() -> [Box<dyn OperationDefinition>; 6] {
    [
        Box::new(SequenceSourceDefinition::new(0)),
        Box::new(CountDefinition::new()),
        Box::new(ProjectDefinition::new([0])),
        Box::new(FilterDefinition::new(Expression::literal(
            Literal::Boolean(Some(true)),
        ))),
        Box::new(ExtendDefinition::new("copy", Expression::column(0))),
        Box::new(DiscardDefinition::new()),
    ]
}

fn valid_schema() -> SchemaRef {
    Arc::new(Schema::empty())
}

fn invalid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "$dogpaddle.invalid",
        DataType::UInt64,
        false,
    )]))
}

#[test]
fn final_binding_entrypoint_enforces_every_common_output_invariant() {
    let rejected = TestDefinition {
        kind: OperationKind::Source,
        binding: TestBinding::Rejected,
    };
    assert!(matches!(
        (&rejected as &dyn OperationDefinition).bind(&[]),
        Err(OperationBindError::Rejected { .. })
    ));

    let missing = TestDefinition {
        kind: OperationKind::Source,
        binding: TestBinding::MissingOutput,
    };
    assert!(matches!(
        (&missing as &dyn OperationDefinition).bind(&[]),
        Err(OperationBindError::MissingOutput)
    ));

    let unexpected = TestDefinition {
        kind: OperationKind::Sink(NonZeroU32::MIN),
        binding: TestBinding::UnexpectedOutput,
    };
    assert!(matches!(
        (&unexpected as &dyn OperationDefinition).bind(&[valid_schema()]),
        Err(OperationBindError::UnexpectedOutput)
    ));

    let invalid = TestDefinition {
        kind: OperationKind::Source,
        binding: TestBinding::InvalidOutput,
    };
    assert!(matches!(
        (&invalid as &dyn OperationDefinition).bind(&[]),
        Err(OperationBindError::InvalidOutputSchema { .. })
    ));
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
