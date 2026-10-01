use dogpaddle_operation::operation::transform::SelectDefinition;
use std::path::Path;

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, OrderedMap, Queue, Store, StoreError, StoreSetup};

use super::support::{
    build_scan_sink_and_read_definition, fixture_bytes, read_published_definition,
};

const V1_SEQUENCE_RUNNING_EVENT_COUNT_DISCARD: &str =
    include_str!("../fixtures/v1/sequence_scan_running_event_count_discard.hex");
const V1_LOGICAL_OPERATIONS: &str = include_str!("../fixtures/v1/logical_operations.hex");
const OWNER_IDENTITY: [u8; 32] = [0xa5; 32];

#[derive(Clone, Copy)]
enum ResourceFault {
    MissingOutput,
    MissingPosition,
    WrongOutputKind,
}

#[test]
fn build_publishes_the_stable_v1_definition_bytes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_chain(&path);
    assert_eq!(
        read_published_definition(&path),
        fixture_bytes(V1_SEQUENCE_RUNNING_EVENT_COUNT_DISCARD)
    );
}

#[test]
fn build_publishes_owner_identity_and_multiple_operations_in_stable_order() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_logical_operations(&path);
    assert_eq!(
        read_published_definition(&path),
        fixture_bytes(V1_LOGICAL_OPERATIONS)
    );
}

#[test]
fn build_uses_one_stack_and_operation_owned_source_queue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_chain(&path);
    let store = Store::open(&path).unwrap();
    for name in ["flow/frames", "flow/outputs"] {
        let map: OrderedMap<u32, Vec<u8>> = store.open_data(name).unwrap();
        assert!(
            map.read(store.read_transaction().access())
                .unwrap()
                .get(&0)
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(
        store.open_data::<Cell<Vec<u8>>>("flow/input"),
        Err(StoreError::DataNotFound(_))
    ));
    assert!(matches!(
        store.open_data::<OrderedMap<u32, Vec<u8>>>("flow/inputs"),
        Err(StoreError::DataNotFound(_))
    ));
    let queue: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    assert!(
        queue
            .read(store.read_transaction().access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
    let _: Cell<u64> = store
        .open_data("operation/00000001/running_event_count.count")
        .unwrap();
    assert!(matches!(
        store.open_data::<Cell<u32>>("station/00000000/active-input"),
        Err(StoreError::DataNotFound(_))
    ));
}

#[test]
fn open_classifies_each_required_source_resource_fault() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_scan_sink_and_read_definition(&root.path().join("complete"));
    for (name, fault) in [
        ("missing-output", ResourceFault::MissingOutput),
        ("missing-position", ResourceFault::MissingPosition),
        ("wrong-output-kind", ResourceFault::WrongOutputKind),
    ] {
        let path = root.path().join(name);
        publish_faulty_resources(&path, &definition, fault);
        let Err(error) = FlowFactory::new(&path).open() else {
            panic!("case {name} unexpectedly opened");
        };
        match fault {
            ResourceFault::MissingOutput => assert!(matches!(
                error,
                FlowError::MissingResource { name }
                    if name == "operation/00000000/sequence_scan.published"
            )),
            ResourceFault::MissingPosition => assert!(matches!(
                error,
                FlowError::MissingResource { name }
                    if name == "operation/00000000/sequence_scan.position"
            )),
            ResourceFault::WrongOutputKind => assert!(matches!(
                error,
                FlowError::Store(StoreError::DataKindMismatch {
                    name,
                    expected: "queue",
                    actual: "cell",
                }) if name == "operation/00000000/sequence_scan.published"
            )),
        }
    }
}

fn publish_faulty_resources(path: &Path, definition: &[u8], fault: ResourceFault) {
    let mut store = StoreSetup::new();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    match fault {
        ResourceFault::MissingOutput => {}
        ResourceFault::WrongOutputKind => {
            store
                .create_data::<Cell<Vec<u8>>>("operation/00000000/sequence_scan.published")
                .unwrap();
        }
        ResourceFault::MissingPosition => {
            store
                .create_data::<Queue<Vec<u8>>>("operation/00000000/sequence_scan.published")
                .unwrap();
        }
    }
    if !matches!(fault, ResourceFault::MissingPosition) {
        store
            .create_data::<Cell<u64>>("operation/00000000/sequence_scan.position")
            .unwrap();
    }
    let mut transactions = store.commit(path, |_| Ok(())).unwrap();
    let transaction = transactions.begin();
    published
        .access(transaction.access())
        .unwrap()
        .set(&definition.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn open_rejects_an_unpublished_build() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = StoreSetup::new();
    store
        .create_data::<Cell<Vec<u8>>>("flow/definition")
        .unwrap();
    drop(store.commit(&path, |_| Ok(())).unwrap());
    assert!(matches!(
        FlowFactory::new(path).open(),
        Err(FlowError::IncompleteBuild)
    ));
}

fn build_chain(path: &Path) {
    let mut builder = FlowFactory::new(path);
    let scan = builder.operation("scan", SequenceScanDefinition::new(7), []);
    let count = builder.operation("count", RunningEventCountDefinition::new(), [scan]);
    builder.operation("sink", DiscardDefinition::new(), [count]);

    drop(builder.build().unwrap());
}

fn build_logical_operations(path: &Path) {
    let mut builder = FlowFactory::new(path);
    builder.owner_identity(OWNER_IDENTITY);
    let scan = builder.operation("scan", SequenceScanDefinition::new(7), []);
    let scan = builder.operation(
        "scan/tail-1",
        SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))]).unwrap(),
        [scan],
    );
    let scan = builder.operation("scan/tail-2", RunningEventCountDefinition::new(), [scan]);
    let scan = builder.operation(
        "scan/tail-3",
        SelectDefinition::try_new([("count", dogpaddle_operation::col("count"))]).unwrap(),
        [scan],
    );

    builder.operation("sink", DiscardDefinition::new(), [scan]);
    drop(builder.build().unwrap());
}

#[test]
fn deeply_nested_expression_remains_opaque_to_json_depth_and_reopens() {
    use arrow_schema::{DataType, Field};
    use dogpaddle_operation::{ScalarValue, cast, lit};
    use std::sync::Arc;
    let mut data_type = DataType::Int64;
    // DataFusion protobuf has its own recursion limit (several messages per List).
    // Keep that existing admission boundary; Flow adds no JSON nesting to this type.
    for _ in 0..30 {
        data_type = DataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([("nested", cast(lit(ScalarValue::Null), data_type))]).unwrap(),
        [source],
    );
    factory.operation("sink", DiscardDefinition::new(), [select]);
    drop(factory.build().unwrap());
    let encoded = read_published_definition(&path);
    // Recursive DataType lives in the canonical protobuf string, not JSON objects.
    assert!(!String::from_utf8_lossy(&encoded).contains("List"));
    drop(FlowFactory::new(&path).open().unwrap());
    assert_eq!(read_published_definition(&path), encoded);
}

#[test]
fn exact_eight_mib_json_plan_builds_and_reopens() {
    fn factory(path: &Path, text: String) -> FlowFactory {
        let mut factory = FlowFactory::new(path);
        let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
        let select = factory.operation(
            "select",
            SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))])
                .unwrap()
                .with_metadata([("size", text)]),
            [scan],
        );
        factory.operation("sink", DiscardDefinition::new(), [select]);
        factory
    }
    let root = tempfile::tempdir().unwrap();
    let empty = root.path().join("empty");
    drop(factory(&empty, String::new()).build().unwrap());
    let overhead = read_published_definition(&empty).len();
    let path = root.path().join("exact");
    drop(
        factory(&path, "x".repeat(8 * 1024 * 1024 - overhead))
            .build()
            .unwrap(),
    );
    assert_eq!(read_published_definition(&path).len(), 8 * 1024 * 1024);
    drop(FlowFactory::new(&path).open().unwrap());
    let too_large = root.path().join("too-large");
    assert!(matches!(
        factory(&too_large, "x".repeat(8 * 1024 * 1024 - overhead + 1)).build(),
        Err(FlowError::Definition(
            dogpaddle_flow::FlowDefinitionError::LengthOverflow("definition")
        ))
    ));
    assert!(!too_large.exists());
}
