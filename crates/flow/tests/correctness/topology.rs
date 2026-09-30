use std::path::Path;

use dogpaddle_flow::{
    FlowError, FlowFactory, InvalidOperationIdReason, OperationRef, TopologyError,
};
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

#[derive(Clone, Copy, Debug)]
enum InvalidCase {
    Empty,
    EmptyId,
    NulId,
    NonScanRoot,
    ScanTerminal,
    SinkFeedsOperation,
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
        InvalidCase::SinkFeedsOperation,
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
            builder.operation("", SequenceScanDefinition::new(0), []);
            TopologyError::InvalidOperationId {
                id: String::new(),
                reason: InvalidOperationIdReason::Empty,
            }
        }
        InvalidCase::NulId => {
            builder.operation("contains\0nul", SequenceScanDefinition::new(0), []);
            TopologyError::InvalidOperationId {
                id: "contains\0nul".to_owned(),
                reason: InvalidOperationIdReason::ContainsNul,
            }
        }
        InvalidCase::NonScanRoot => {
            let count = builder.operation("count", RunningEventCountDefinition::new(), []);
            builder.operation("sink", DiscardDefinition::new(), [count]);

            TopologyError::InputCount {
                operation: "count".to_owned(),
                expected: 1,
                actual: 0,
            }
        }
        InvalidCase::ScanTerminal => {
            builder.operation("scan", SequenceScanDefinition::new(0), []);
            TopologyError::TerminalIsNotSink("scan".to_owned())
        }

        InvalidCase::SinkFeedsOperation => {
            let scan = builder.operation("scan", SequenceScanDefinition::new(0), []);
            let sink = builder.operation("sink", DiscardDefinition::new(), [scan]);
            let count = builder.operation("count", RunningEventCountDefinition::new(), [sink]);
            builder.operation("terminal", DiscardDefinition::new(), [count]);

            TopologyError::InputHasNoOutput {
                input: "sink".to_owned(),
                operation: "count".to_owned(),
            }
        }
        InvalidCase::ForeignConnection => {
            let foreign = foreign_scan(root);
            builder.operation("own-scan", SequenceScanDefinition::new(0), []);
            builder.operation("count", RunningEventCountDefinition::new(), [foreign]);

            TopologyError::ForeignOperationRef(foreign)
        }
    };
    (builder, expected)
}

fn scan_sink(builder: &mut FlowFactory) -> (OperationRef, OperationRef) {
    let scan = builder.operation("scan", SequenceScanDefinition::new(0), []);
    let sink = builder.operation("sink", DiscardDefinition::new(), [scan]);

    (scan, sink)
}

fn foreign_scan(root: &Path) -> OperationRef {
    let mut foreign = FlowFactory::new(root.join("foreign"));
    foreign.operation("foreign", SequenceScanDefinition::new(0), [])
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
    scan_sink(&mut builder);
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

#[test]
fn depth_limit_counts_durable_calls_after_fusion_and_rejects_before_creation() {
    use dogpaddle_operation::operation::transform::UnionAllDefinition;
    let root = tempfile::tempdir().unwrap();
    for depth in [64, 65] {
        let path = root.path().join(format!("depth-{depth}"));
        let mut factory = FlowFactory::new(&path);
        let mut tail = factory.operation("source", SequenceScanDefinition::new(0), []);
        for index in 1..depth {
            tail = factory.operation(
                format!("union-{index}"),
                UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap()),
                [tail, tail],
            );
        }
        factory.operation("sink", DiscardDefinition::new(), [tail]);
        if depth == 64 {
            assert!(factory.build().is_ok());
        } else {
            assert!(matches!(
                factory.build(),
                Err(FlowError::Topology(TopologyError::Limit("64 call frames")))
            ));
            assert!(!path.exists());
        }
    }
    let mut factory = FlowFactory::new(root.path().join("fused"));
    let mut tail = factory.operation("source", SequenceScanDefinition::new(0), []);
    for index in 0..128 {
        tail = factory.operation(
            format!("count-{index}"),
            RunningEventCountDefinition::new(),
            [tail],
        );
    }
    factory.operation("sink", DiscardDefinition::new(), [tail]);
    assert_eq!(factory.build().unwrap().operation_count(), 130);
}
