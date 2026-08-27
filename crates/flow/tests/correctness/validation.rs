use dogpaddle_flow::{FlowError, FlowFactory, InvalidStationIdReason, TopologyError};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

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
fn topology_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    builder.station("count", CountDefinition::new());

    let Err(error) = builder.build() else {
        panic!("invalid topology unexpectedly built");
    };
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InputCount {
            station,
            expected: 1,
            actual: 0,
        }) if station == "count"
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
    builder.station("source", SequenceSourceDefinition::new(0));
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
