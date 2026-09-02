use std::{num::NonZeroU64, path::Path};

use dogpaddle_flow::{FlowError, FlowFactory, InvalidStationIdReason, StationRef, TopologyError};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024).unwrap();

#[derive(Clone, Copy, Debug)]
enum InvalidCase {
    Empty,
    EmptyId,
    NulId,
    NonSourceRoot,
    SourceTerminal,
    MissingCapacity,
    UnexpectedCapacity,
    DuplicateCapacity,
    ForeignCapacity,
    SinkFeedsStation,
    ForeignConnection,
    EmptySources,
    SourcesTwice,
}

#[test]
fn every_topology_rejection_is_precise_and_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    for case in [
        InvalidCase::Empty,
        InvalidCase::EmptyId,
        InvalidCase::NulId,
        InvalidCase::NonSourceRoot,
        InvalidCase::SourceTerminal,
        InvalidCase::MissingCapacity,
        InvalidCase::UnexpectedCapacity,
        InvalidCase::DuplicateCapacity,
        InvalidCase::ForeignCapacity,
        InvalidCase::SinkFeedsStation,
        InvalidCase::ForeignConnection,
        InvalidCase::EmptySources,
        InvalidCase::SourcesTwice,
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
            builder.station("", SequenceSourceDefinition::new(0));
            TopologyError::InvalidStationId {
                id: String::new(),
                reason: InvalidStationIdReason::Empty,
            }
        }
        InvalidCase::NulId => {
            builder.station("contains\0nul", SequenceSourceDefinition::new(0));
            TopologyError::InvalidStationId {
                id: "contains\0nul".to_owned(),
                reason: InvalidStationIdReason::ContainsNul,
            }
        }
        InvalidCase::NonSourceRoot => {
            let count = builder.station("count", CountDefinition::new());
            let sink = builder.station("sink", DiscardDefinition::new());
            builder.connect([count], sink);
            TopologyError::RootIsNotSource("count".to_owned())
        }
        InvalidCase::SourceTerminal => {
            builder.station("source", SequenceSourceDefinition::new(0));
            TopologyError::TerminalIsNotSink("source".to_owned())
        }
        InvalidCase::MissingCapacity => {
            source_sink(&mut builder);
            TopologyError::MissingOutputCapacity("source".to_owned())
        }
        InvalidCase::UnexpectedCapacity => {
            let (source, sink) = source_sink(&mut builder);
            builder.output_capacity_bytes(source, CAPACITY);
            builder.output_capacity_bytes(sink, CAPACITY);
            TopologyError::UnexpectedOutputCapacity("sink".to_owned())
        }
        InvalidCase::DuplicateCapacity => {
            let (source, _) = source_sink(&mut builder);
            builder.output_capacity_bytes(source, CAPACITY);
            builder.output_capacity_bytes(source, CAPACITY);
            TopologyError::OutputCapacityAlreadySet("source".to_owned())
        }
        InvalidCase::ForeignCapacity => {
            let foreign = foreign_source(root);
            let (source, _) = source_sink(&mut builder);
            builder.output_capacity_bytes(source, CAPACITY);
            builder.output_capacity_bytes(foreign, CAPACITY);
            TopologyError::ForeignStationRef(foreign)
        }
        InvalidCase::SinkFeedsStation => {
            let source = builder.station("source", SequenceSourceDefinition::new(0));
            let sink = builder.station("sink", DiscardDefinition::new());
            let count = builder.station("count", CountDefinition::new());
            let terminal = builder.station("terminal", DiscardDefinition::new());
            builder.connect([source], sink);
            builder.connect([sink], count);
            builder.connect([count], terminal);
            TopologyError::UpstreamHasNoOutput {
                upstream_station: "sink".to_owned(),
                downstream_station: "count".to_owned(),
            }
        }
        InvalidCase::ForeignConnection => {
            let foreign = foreign_source(root);
            builder.station("own-source", SequenceSourceDefinition::new(0));
            let count = builder.station("count", CountDefinition::new());
            builder.connect([foreign], count);
            TopologyError::ForeignStationRef(foreign)
        }
        InvalidCase::EmptySources => {
            let source = builder.station("source", SequenceSourceDefinition::new(0));
            builder.connect([], source);
            TopologyError::EmptySources("source".to_owned())
        }
        InvalidCase::SourcesTwice => {
            let first = builder.station("first", SequenceSourceDefinition::new(0));
            let second = builder.station("second", SequenceSourceDefinition::new(1));
            let count = builder.station("count", CountDefinition::new());
            builder.connect([first], count);
            builder.connect([second], count);
            TopologyError::SourcesAlreadySet("count".to_owned())
        }
    };
    (builder, expected)
}

fn source_sink(builder: &mut FlowFactory) -> (StationRef, StationRef) {
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    (source, sink)
}

fn foreign_source(root: &Path) -> StationRef {
    let mut foreign = FlowFactory::new(root.join("foreign"));
    foreign.station("foreign", SequenceSourceDefinition::new(0))
}

#[test]
fn build_rejects_an_occupied_path_without_mutating_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let sentinel: Cell<u64> = store.create_data("sentinel").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    sentinel
        .access(transaction.access())
        .unwrap()
        .set(&41)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let mut builder = FlowFactory::new(&path);
    let (source, _) = source_sink(&mut builder);
    builder.output_capacity_bytes(source, CAPACITY);
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
    let transaction = transactions.begin().unwrap();
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
