use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition,
    transform::RunningEventCountDefinition,
};
use dogpaddle_store::{
    AppendLog, Cell, OrderedMap, ReadTransactionAccess, Small, Store, StoreError,
};

use super::support::{
    build_source_sink_and_read_definition, fixture_bytes, read_published_definition,
};

const V1_SEQUENCE_RUNNING_EVENT_COUNT_DISCARD: &str =
    include_str!("../fixtures/v1/sequence_source_running_event_count_discard.hex");

#[derive(Clone, Copy)]
enum ResourceFault {
    MissingOutput,
    MissingPosition,
    WrongOutputSize,
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
fn build_uses_the_stable_resource_layout_and_input_origins() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_chain(&path);
    let store = Store::open(&path).unwrap();
    let states: [OrderedMap<Vec<u8>, Vec<u8>, Small>; 3] = [
        store.open_data("station/00000000/state").unwrap(),
        store.open_data("station/00000001/state").unwrap(),
        store.open_data("station/00000002/state").unwrap(),
    ];
    let _running_event_count: Cell<u64> = store
        .open_data("station/00000001/operation/running_event_count.count")
        .unwrap();
    assert!(matches!(
        store.open_data::<AppendLog<Vec<u8>>>("station/00000002/output"),
        Err(StoreError::DataNotFound(name)) if name == "station/00000002/output"
    ));
    let (_, reads) = store.into_transactions().split();
    let transaction = reads.begin().unwrap();
    assert_eq!(input_origin(&states[0], transaction.access()), (None, None));
    assert_eq!(
        input_origin(&states[1], transaction.access()),
        (Some(vec![0; 4]), Some(vec![0; 8]))
    );
    assert_eq!(
        input_origin(&states[2], transaction.access()),
        (Some(vec![0; 4]), Some(vec![0; 8]))
    );
}

#[test]
fn open_classifies_each_required_station_resource_fault() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_source_sink_and_read_definition(&root.path().join("complete"));
    for (name, fault) in [
        ("missing-output", ResourceFault::MissingOutput),
        ("missing-position", ResourceFault::MissingPosition),
        ("wrong-output-size", ResourceFault::WrongOutputSize),
    ] {
        let path = root.path().join(name);
        publish_faulty_resources(&path, &definition, fault);
        let Err(error) = FlowFactory::open(&path) else {
            panic!("case {name} unexpectedly opened");
        };
        match fault {
            ResourceFault::MissingOutput => assert!(matches!(
                error,
                FlowError::MissingResource { name }
                    if name == "station/00000000/output"
            )),
            ResourceFault::MissingPosition => assert!(matches!(
                error,
                FlowError::MissingResource { name }
                    if name == "station/00000000/operation/sequence_source.position"
            )),
            ResourceFault::WrongOutputSize => assert!(matches!(
                error,
                FlowError::Store(StoreError::DataSizeMismatch {
                    name,
                    expected: "large",
                    actual: "small",
                }) if name == "station/00000000/output"
            )),
        }
    }
}

fn publish_faulty_resources(path: &Path, definition: &[u8], fault: ResourceFault) {
    let mut store = Store::create(path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("station/00000000/state")
        .unwrap();
    if !matches!(fault, ResourceFault::MissingOutput) {
        if matches!(fault, ResourceFault::WrongOutputSize) {
            store
                .create_data::<Cell<Vec<u8>>>("station/00000000/output")
                .unwrap();
        } else {
            store
                .create_data::<AppendLog<Vec<u8>>>("station/00000000/output")
                .unwrap();
        }
    }
    if !matches!(fault, ResourceFault::MissingPosition) {
        store
            .create_data::<Cell<u64>>("station/00000000/operation/sequence_source.position")
            .unwrap();
    }
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
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
        FlowFactory::open(path),
        Err(FlowError::IncompleteBuild)
    ));
}

fn build_chain(path: &Path) {
    let mut builder = FlowFactory::new(path);
    let source = builder.station("source", SequenceSourceDefinition::new(7));
    let count = builder.station("count", RunningEventCountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], count);
    builder.connect([count], sink);
    builder.output_capacity_bytes(source, NonZeroU64::new(1_024).unwrap());
    builder.output_capacity_bytes(count, NonZeroU64::new(2_048).unwrap());
    drop(builder.build().unwrap());
}

fn input_origin(
    state: &OrderedMap<Vec<u8>, Vec<u8>, Small>,
    access: ReadTransactionAccess<'_>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let state = state.read(access).unwrap();
    (
        state.get(&b"input/active".to_vec()).unwrap(),
        state.get(&b"input/00000000/cursor".to_vec()).unwrap(),
    )
}
