use std::num::NonZeroU64;

use dogpaddle_flow::{FlowError, FlowFactory, InvalidStationIdReason, TopologyError};
use dogpaddle_operation::operation::{
    sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

fn capacity() -> NonZeroU64 {
    NonZeroU64::new(1_024).unwrap()
}

#[test]
fn empty_topology_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");

    let error = build_error(FlowFactory::new(&path));

    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::EmptyTopology)
    ));
    assert!(!path.exists());
}

#[test]
fn invalid_and_duplicate_station_ids_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let empty_path = root.path().join("empty-id");
    let mut builder = FlowFactory::new(&empty_path);
    builder.station("", SequenceSourceDefinition::new(0));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InvalidStationId {
            id,
            reason: InvalidStationIdReason::Empty,
        }) if id.is_empty()
    ));
    assert!(!empty_path.exists());

    let nul_path = root.path().join("nul-id");
    let mut builder = FlowFactory::new(&nul_path);
    builder.station("contains\0nul", SequenceSourceDefinition::new(0));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InvalidStationId {
            id,
            reason: InvalidStationIdReason::ContainsNul,
        }) if id == "contains\0nul"
    ));
    assert!(!nul_path.exists());

    let duplicate_path = root.path().join("duplicate-id");
    let mut builder = FlowFactory::new(&duplicate_path);
    builder.station("same", SequenceSourceDefinition::new(0));
    builder.station("same", SequenceSourceDefinition::new(1));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::DuplicateStationId(id)) if id == "same"
    ));
    assert!(!duplicate_path.exists());
}

#[test]
fn non_source_root_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let count = builder.station("count", CountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([count], sink);

    let Err(error) = builder.build() else {
        panic!("invalid topology unexpectedly built");
    };
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::RootIsNotSource(id)) if id == "count"
    ));
    assert!(!path.exists());
}

#[test]
fn non_sink_terminals_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let source_path = root.path().join("source-terminal");
    let mut builder = FlowFactory::new(&source_path);
    builder.station("source", SequenceSourceDefinition::new(0));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::TerminalIsNotSink(id)) if id == "source"
    ));
    assert!(!source_path.exists());

    let transform_path = root.path().join("transform-terminal");
    let mut builder = FlowFactory::new(&transform_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::TerminalIsNotSink(id)) if id == "count"
    ));
    assert!(!transform_path.exists());
}

#[test]
fn multiple_source_sink_components_build() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_source = builder.station("first-source", SequenceSourceDefinition::new(0));
    let first_sink = builder.station("first-sink", DiscardDefinition::new());
    let second_source = builder.station("second-source", SequenceSourceDefinition::new(10));
    let count = builder.station("count", CountDefinition::new());
    let second_sink = builder.station("second-sink", DiscardDefinition::new());
    builder.connect([first_source], first_sink);
    builder.connect([second_source], count);
    builder.connect([count], second_sink);
    builder.output_capacity_bytes(first_source, capacity());
    builder.output_capacity_bytes(second_source, capacity());
    builder.output_capacity_bytes(count, capacity());

    let flow = builder.build().unwrap();

    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        [
            "first-source",
            "first-sink",
            "second-source",
            "count",
            "second-sink"
        ]
    );
}

#[test]
fn invalid_output_capacities_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let missing_path = root.path().join("missing");
    let mut builder = FlowFactory::new(&missing_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::MissingOutputCapacity(id)) if id == "source"
    ));
    assert!(!missing_path.exists());

    let unexpected_path = root.path().join("unexpected");
    let mut builder = FlowFactory::new(&unexpected_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    builder.output_capacity_bytes(source, capacity());
    builder.output_capacity_bytes(sink, capacity());
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::UnexpectedOutputCapacity(id)) if id == "sink"
    ));
    assert!(!unexpected_path.exists());

    let duplicate_path = root.path().join("duplicate");
    let mut builder = FlowFactory::new(&duplicate_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    builder.output_capacity_bytes(source, capacity());
    builder.output_capacity_bytes(source, capacity());
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::OutputCapacityAlreadySet(id)) if id == "source"
    ));
    assert!(!duplicate_path.exists());

    let foreign_path = root.path().join("foreign-reference");
    let mut foreign_factory = FlowFactory::new(root.path().join("foreign-factory"));
    let foreign = foreign_factory.station("foreign", SequenceSourceDefinition::new(0));
    let mut builder = FlowFactory::new(&foreign_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    builder.output_capacity_bytes(source, capacity());
    builder.output_capacity_bytes(foreign, capacity());
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::ForeignStationRef(reference)) if reference == foreign
    ));
    assert!(!foreign_path.exists());
}

#[test]
fn sink_cannot_feed_another_station() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    let count = builder.station("count", CountDefinition::new());
    let terminal = builder.station("terminal", DiscardDefinition::new());
    builder.connect([source], sink);
    builder.connect([sink], count);
    builder.connect([count], terminal);

    let error = build_error(builder);

    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::UpstreamHasNoOutput {
            upstream_station,
            downstream_station,
        }) if upstream_station == "sink" && downstream_station == "count"
    ));
    assert!(!path.exists());
}

#[test]
fn foreign_reference_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut foreign_builder = FlowFactory::new(root.path().join("foreign"));
    let foreign_source = foreign_builder.station("source", SequenceSourceDefinition::new(0));

    let mut builder = FlowFactory::new(&path);
    builder.station("own-source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([foreign_source], count);
    let Err(error) = builder.build() else {
        panic!("foreign reference unexpectedly built");
    };
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::ForeignStationRef(_))
    ));
    assert!(!path.exists());
}

#[test]
fn invalid_connection_shapes_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let empty_path = root.path().join("empty-sources");
    let mut builder = FlowFactory::new(&empty_path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    builder.connect([], source);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::EmptySources(id)) if id == "source"
    ));
    assert!(!empty_path.exists());

    let duplicate_path = root.path().join("sources-twice");
    let mut builder = FlowFactory::new(&duplicate_path);
    let first = builder.station("first", SequenceSourceDefinition::new(0));
    let second = builder.station("second", SequenceSourceDefinition::new(1));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([first], count);
    builder.connect([second], count);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::SourcesAlreadySet(id)) if id == "count"
    ));
    assert!(!duplicate_path.exists());
}

#[test]
fn direct_and_indirect_cycles_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let self_loop_path = root.path().join("self-loop");
    let mut builder = FlowFactory::new(&self_loop_path);
    let count = builder.station("count", CountDefinition::new());
    builder.connect([count], count);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::SelfLoop(id)) if id == "count"
    ));
    assert!(!self_loop_path.exists());

    let cycle_path = root.path().join("cycle");
    let mut builder = FlowFactory::new(&cycle_path);
    let left = builder.station("left", CountDefinition::new());
    let right = builder.station("right", CountDefinition::new());
    builder.connect([left], right);
    builder.connect([right], left);
    let error = build_error(builder);
    assert!(matches!(error, FlowError::Topology(TopologyError::Cycle)));
    assert!(!cycle_path.exists());
}

#[test]
fn build_rejects_an_occupied_path_without_mutating_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let sentinel: Cell<u64> = store.create_data("sentinel").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        sentinel
            .access(transaction.access())
            .unwrap()
            .set(&41)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    builder.output_capacity_bytes(source, capacity());
    let error = build_error(builder);
    assert!(matches!(
        error,
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
