use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{
    Cell, Store, StoreError, SubscribedLog, SubscribedLogStatus, SubscriptionStatus,
};

use super::support::{
    build_scan_sink_and_read_definition, fixture_bytes, read_published_definition,
};

const V1_SEQUENCE_RUNNING_EVENT_COUNT_DISCARD: &str =
    include_str!("../fixtures/v1/sequence_scan_running_event_count_discard.hex");

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
fn build_uses_subscribed_outputs_and_only_materializes_multi_input_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    build_chain(&path);
    let store = Store::open(&path).unwrap();
    let outputs: [SubscribedLog<Vec<u8>>; 2] = [
        store.open_data("station/00000000/output").unwrap(),
        store.open_data("station/00000001/output").unwrap(),
    ];
    let _running_event_count: Cell<u64> = store
        .open_data("station/00000001/operation/running_event_count.count")
        .unwrap();
    assert!(matches!(
        store.open_data::<SubscribedLog<Vec<u8>>>("station/00000002/output"),
        Err(StoreError::DataNotFound(name)) if name == "station/00000002/output"
    ));
    for index in 0..3 {
        let name = format!("station/{index:08x}/active-input");
        assert!(matches!(
            store.open_data::<Cell<u32>>(&name),
            Err(StoreError::DataNotFound(actual)) if actual == name
        ));
    }
    let transaction = store.read_transaction();
    for output in outputs {
        output
            .validate(NonZeroU64::MIN, transaction.access())
            .unwrap();
        assert_eq!(
            output.writer().status(transaction.access()).unwrap(),
            SubscribedLogStatus {
                head: 0,
                tail: 0,
                retained_bytes: 0,
            }
        );
        assert_eq!(
            output.subscription(0).status(transaction.access()).unwrap(),
            SubscriptionStatus {
                position: 0,
                tail: 0,
            }
        );
    }
}

#[test]
fn open_classifies_each_required_station_resource_fault() {
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
                    if name == "station/00000000/output"
            )),
            ResourceFault::MissingPosition => assert!(matches!(
                error,
                FlowError::MissingResource { name }
                    if name == "station/00000000/operation/sequence_scan.position"
            )),
            ResourceFault::WrongOutputKind => assert!(matches!(
                error,
                FlowError::Store(StoreError::DataKindMismatch {
                    name,
                    expected: "subscribed log",
                    actual: "cell",
                }) if name == "station/00000000/output"
            )),
        }
    }
}

fn publish_faulty_resources(path: &Path, definition: &[u8], fault: ResourceFault) {
    let mut store = Store::create(path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let output = match fault {
        ResourceFault::MissingOutput => None,
        ResourceFault::WrongOutputKind => {
            store
                .create_data::<Cell<Vec<u8>>>("station/00000000/output")
                .unwrap();
            None
        }
        ResourceFault::MissingPosition => Some(
            store
                .create_data::<SubscribedLog<Vec<u8>>>("station/00000000/output")
                .unwrap(),
        ),
    };
    if !matches!(fault, ResourceFault::MissingPosition) {
        store
            .create_data::<Cell<u64>>("station/00000000/operation/sequence_scan.position")
            .unwrap();
    }
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    if let Some(output) = output {
        output
            .initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
    }
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
    let scan = builder.station("scan", SequenceScanDefinition::new(7));
    let count = builder.station("count", RunningEventCountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([scan], count);
    builder.connect([count], sink);
    builder.output_capacity_bytes(scan, NonZeroU64::new(1_024).unwrap());
    builder.output_capacity_bytes(count, NonZeroU64::new(2_048).unwrap());
    drop(builder.build().unwrap());
}
