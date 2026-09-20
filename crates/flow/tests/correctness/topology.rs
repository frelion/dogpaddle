use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{FlowError, FlowFactory, InvalidStationIdReason, OperationRef, TopologyError};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024).unwrap();

#[derive(Clone, Copy, Debug)]
enum InvalidCase {
    Empty,
    EmptyId,
    NulId,
    NonScanRoot,
    ScanTerminal,
    UnexpectedCapacity,
    DuplicateCapacity,
    ForeignCapacity,
    SinkFeedsStation,
    ForeignConnection,
}

#[test]
fn every_topology_rejection_is_precise_and_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    for case in [
        InvalidCase::Empty,
        InvalidCase::EmptyId,
        InvalidCase::NulId,
        InvalidCase::NonScanRoot,
        InvalidCase::ScanTerminal,
        InvalidCase::UnexpectedCapacity,
        InvalidCase::DuplicateCapacity,
        InvalidCase::ForeignCapacity,
        InvalidCase::SinkFeedsStation,
        InvalidCase::ForeignConnection,
    ] {
        let path = root.path().join(format!("{case:?}"));
        let (builder, expected) = invalid_topology(case, &path, root.path());
        let FlowError::Topology(actual) = build_error(builder) else {
            panic!("case {case:?} returned a non-topology error");
        };
        assert_eq!(actual, expected, "case {case:?}");
        assert!(!path.exists(), "case {case:?} created the Store path");
    }
}

fn invalid_topology(case: InvalidCase, path: &Path, root: &Path) -> (FlowFactory, TopologyError) {
    let mut builder = FlowFactory::new(path);
    let expected = match case {
        InvalidCase::Empty => TopologyError::EmptyTopology,
        InvalidCase::EmptyId => {
            builder.operation("", Box::new(SequenceScanDefinition::new(0)), []);
            TopologyError::InvalidStationId {
                id: String::new(),
                reason: InvalidStationIdReason::Empty,
            }
        }
        InvalidCase::NulId => {
            builder.operation(
                "contains\0nul",
                Box::new(SequenceScanDefinition::new(0)),
                [],
            );
            TopologyError::InvalidStationId {
                id: "contains\0nul".to_owned(),
                reason: InvalidStationIdReason::ContainsNul,
            }
        }
        InvalidCase::NonScanRoot => {
            let count =
                builder.operation("count", Box::new(RunningEventCountDefinition::new()), []);
            builder.operation("sink", Box::new(DiscardDefinition::new()), [count]);

            TopologyError::InputCount {
                station: "count".to_owned(),
                expected: 1,
                actual: 0,
            }
        }
        InvalidCase::ScanTerminal => {
            builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
            TopologyError::TerminalIsNotSink("scan".to_owned())
        }

        InvalidCase::UnexpectedCapacity => {
            let (scan, sink) = scan_sink(&mut builder);
            builder.materialize(scan, CAPACITY);
            builder.materialize(sink, CAPACITY);
            TopologyError::UnexpectedOutputCapacity("sink".to_owned())
        }
        InvalidCase::DuplicateCapacity => {
            let (scan, _) = scan_sink(&mut builder);
            builder.materialize(scan, CAPACITY);
            builder.materialize(scan, CAPACITY);
            TopologyError::OutputCapacityAlreadySet("scan".to_owned())
        }
        InvalidCase::ForeignCapacity => {
            let foreign = foreign_scan(root);
            let (scan, _) = scan_sink(&mut builder);
            builder.materialize(scan, CAPACITY);
            builder.materialize(foreign, CAPACITY);
            TopologyError::ForeignOperationRef(foreign)
        }
        InvalidCase::SinkFeedsStation => {
            let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
            let sink = builder.operation("sink", Box::new(DiscardDefinition::new()), [scan]);
            let count = builder.operation(
                "count",
                Box::new(RunningEventCountDefinition::new()),
                [sink],
            );
            builder.operation("terminal", Box::new(DiscardDefinition::new()), [count]);

            TopologyError::InputHasNoOutput {
                input_station: "sink".to_owned(),
                station: "count".to_owned(),
            }
        }
        InvalidCase::ForeignConnection => {
            let foreign = foreign_scan(root);
            builder.operation("own-scan", Box::new(SequenceScanDefinition::new(0)), []);
            builder.operation(
                "count",
                Box::new(RunningEventCountDefinition::new()),
                [foreign],
            );

            TopologyError::ForeignOperationRef(foreign)
        }
    };
    (builder, expected)
}

fn scan_sink(builder: &mut FlowFactory) -> (OperationRef, OperationRef) {
    let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    let sink = builder.operation("sink", Box::new(DiscardDefinition::new()), [scan]);

    (scan, sink)
}

fn foreign_scan(root: &Path) -> OperationRef {
    let mut foreign = FlowFactory::new(root.join("foreign"));
    foreign.operation("foreign", Box::new(SequenceScanDefinition::new(0)), [])
}

#[test]
fn build_rejects_an_occupied_path_without_mutating_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let sentinel: Cell<u64> = store.create_data("sentinel").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    sentinel
        .access(transaction.access())
        .unwrap()
        .set(&41)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let mut builder = FlowFactory::new(&path);
    let (scan, _) = scan_sink(&mut builder);
    builder.materialize(scan, CAPACITY);
    assert!(matches!(
        build_error(builder),
        FlowError::Store(StoreError::PathExists(actual)) if actual == path
    ));

    let store = Store::open(&path).unwrap();
    let sentinel: Cell<u64> = store.open_data("sentinel").unwrap();
    assert!(matches!(
        store.open_data::<Cell<Vec<u8>>>("flow/definition"),
        Err(StoreError::DataNotFound(name)) if name == "flow/definition"
    ));
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        sentinel
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(41)
    );
}

fn build_error(builder: FlowFactory) -> FlowError {
    let Err(error) = builder.build() else {
        panic!("invalid Flow unexpectedly built");
    };
    error
}
