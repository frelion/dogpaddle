use std::{
    collections::HashSet,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_store::{Cell, OrderedMap, Store};

use crate::{
    DataDeclaration, DataInstances, MaterializeError, OperationBindError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::DECODERS,
    col,
    definition::{DataName, Sealed},
    lit,
    operation::{
        scan::{
            MySqlCdcScanDefinition, MySqlCdcScanSpec, MySqlColumn, MySqlType,
            PostgresCdcScanDefinition, PostgresCdcScanSpec, PostgresColumn, PostgresType,
            SequenceScanDefinition,
        },
        sink::{
            DiscardDefinition, DiscardOperation, PostgresSinkDefinition, PostgresTargetSpec,
            SqliteSinkDefinition,
        },
        transform::{
            AggregateCall, AggregateDefinition, DistinctDefinition, ExtendDefinition,
            FilterDefinition, ProjectDefinition, RunningEventCountDefinition,
            SchemaAlignDefinition, SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
};

const COUNT: DataName<Cell<u64>> = DataName::new("count");
const STRING_COUNT: DataName<Cell<String>> = DataName::new("count");
const MAP_COUNT: DataName<OrderedMap<Vec<u8>, Vec<u8>>> = DataName::new("count");
const STATE: DataName<OrderedMap<Vec<u8>, Vec<u8>>> = DataName::new("state");

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
        Ok(OperationBinding::without_data_turn(
            output,
            DiscardOperation,
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

fn builtin_definitions() -> [(u16, Box<dyn OperationDefinition>); 15] {
    [
        (1, Box::new(SequenceScanDefinition::new(0))),
        (2, Box::new(RunningEventCountDefinition::new())),
        (3, Box::new(DiscardDefinition::new())),
        (4, Box::new(ProjectDefinition::new([0]))),
        (5, Box::new(FilterDefinition::try_new(lit(true)).unwrap())),
        (
            6,
            Box::new(ExtendDefinition::try_new("copy", col("value")).unwrap()),
        ),
        (
            7,
            Box::new(SelectDefinition::try_new([("copy", col("value"))]).unwrap()),
        ),
        (
            8,
            Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        ),
        (
            9,
            Box::new(
                SchemaAlignDefinition::try_new([SchemaAlignField::try_new(
                    "copy",
                    col("value"),
                    false,
                )
                .unwrap()])
                .unwrap(),
            ),
        ),
        (
            10,
            Box::new(SqliteSinkDefinition::try_new("/tmp/dogpaddle.sqlite", "events").unwrap()),
        ),
        (
            11,
            Box::new(
                PostgresCdcScanDefinition::try_new(
                    PostgresCdcScanSpec {
                        engine_name: "events".into(),
                        database: "shop".into(),
                        schema: "public".into(),
                        table: "events".into(),
                        slot: "events".into(),
                        publication: "events".into(),
                        system_identifier: "1".into(),
                        database_oid: 1,
                        table_oid: 1,
                        columns: vec![PostgresColumn::new("id", PostgresType::Int64, false)],
                    },
                    NonZeroU64::MIN,
                )
                .unwrap(),
            ),
        ),
        (
            12,
            Box::new(
                PostgresSinkDefinition::try_new(
                    PostgresTargetSpec::try_new("events", "shop", "public", "events", "1", 1)
                        .unwrap(),
                )
                .unwrap(),
            ),
        ),
        (13, Box::new(DistinctDefinition::new())),
        (
            14,
            Box::new(
                AggregateDefinition::try_new(
                    [("value", col("value"))],
                    [("count", AggregateCall::count_all())],
                )
                .unwrap(),
            ),
        ),
        (
            15,
            Box::new(
                MySqlCdcScanDefinition::try_new(
                    MySqlCdcScanSpec {
                        engine_name: "events".into(),
                        database: "shop".into(),
                        table: "events".into(),
                        server_uuid: "01234567-89ab-cdef-0123-456789abcdef".into(),
                        table_id: 1,
                        columns: vec![MySqlColumn::new("id", MySqlType::Int64, false)],
                    },
                    NonZeroU64::MIN,
                )
                .unwrap(),
            ),
        ),
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
        kind: OperationKind::Scan,
        binding: TestBinding::Rejected,
    };
    assert!(matches!(
        (&rejected as &dyn OperationDefinition).bind(&[]),
        Err(OperationBindError::Rejected { .. })
    ));

    let missing = TestDefinition {
        kind: OperationKind::Scan,
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
        kind: OperationKind::Scan,
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
        .map(|(expected_tag, definition)| {
            assert_eq!(definition.persistence_tag(), *expected_tag);
            *expected_tag
        })
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
    let _state: OrderedMap<Vec<u8>, Vec<u8>> = instances.take(&STATE).unwrap();
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
fn data_instances_reject_missing_names_and_materialization_rejects_unconsumed_names() {
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
    let binding = OperationBinding::without_data_turn(None, DiscardOperation);
    let Err(error) = binding.materialize(unconsumed, crate::RuntimeResource::none()) else {
        panic!("binding unexpectedly accepted an unconsumed data instance");
    };
    assert_eq!(error, MaterializeError::UnexpectedData { name: "count" });
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
