use dogpaddle_operation::operation::transform::SelectDefinition;
use std::path::Path;

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, OrderedMap, Queue, Store, StoreError};

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
    let mut store = Store::create(path).unwrap();
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
    let mut transactions = store.into_transactions();
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
    let mut store = Store::create(&path).unwrap();
    store
        .create_data::<Cell<Vec<u8>>>("flow/definition")
        .unwrap();
    drop(store);
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
