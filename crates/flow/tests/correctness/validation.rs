use dogpaddle_flow::{Flow, FlowBuilder, FlowError, InvalidStageIdReason, TopologyError};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{Cell, Store, StoreError};

#[test]
fn empty_topology_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");

    let error = build_error(Flow::builder(&path));

    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::EmptyTopology)
    ));
    assert!(!path.exists());
}

#[test]
fn invalid_and_duplicate_stage_ids_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let empty_path = root.path().join("empty-id");
    let mut builder = Flow::builder(&empty_path);
    builder.stage("", SequenceSourceDefinition::new(0));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InvalidStageId {
            id,
            reason: InvalidStageIdReason::Empty,
        }) if id.is_empty()
    ));
    assert!(!empty_path.exists());

    let nul_path = root.path().join("nul-id");
    let mut builder = Flow::builder(&nul_path);
    builder.stage("contains\0nul", SequenceSourceDefinition::new(0));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InvalidStageId {
            id,
            reason: InvalidStageIdReason::ContainsNul,
        }) if id == "contains\0nul"
    ));
    assert!(!nul_path.exists());

    let duplicate_path = root.path().join("duplicate-id");
    let mut builder = Flow::builder(&duplicate_path);
    builder.stage("same", SequenceSourceDefinition::new(0));
    builder.stage("same", SequenceSourceDefinition::new(1));
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::DuplicateStageId(id)) if id == "same"
    ));
    assert!(!duplicate_path.exists());
}

#[test]
fn topology_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    builder.stage("count", CountDefinition::new());

    let Err(error) = builder.build() else {
        panic!("invalid topology unexpectedly built");
    };
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::InputCount {
            stage,
            expected: 1,
            actual: 0,
        }) if stage == "count"
    ));
    assert!(!path.exists());
}

#[test]
fn foreign_reference_failure_has_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut foreign_builder = Flow::builder(root.path().join("foreign"));
    let foreign_source = foreign_builder.stage("source", SequenceSourceDefinition::new(0));

    let mut builder = Flow::builder(&path);
    builder.stage("own-source", SequenceSourceDefinition::new(0));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([foreign_source], count);
    let Err(error) = builder.build() else {
        panic!("foreign reference unexpectedly built");
    };
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::ForeignStageRef(_))
    ));
    assert!(!path.exists());
}

#[test]
fn invalid_connection_shapes_have_no_store_side_effect() {
    let root = tempfile::tempdir().unwrap();

    let empty_path = root.path().join("empty-sources");
    let mut builder = Flow::builder(&empty_path);
    let source = builder.stage("source", SequenceSourceDefinition::new(0));
    builder.connect([], source);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::EmptySources(id)) if id == "source"
    ));
    assert!(!empty_path.exists());

    let duplicate_path = root.path().join("sources-twice");
    let mut builder = Flow::builder(&duplicate_path);
    let first = builder.stage("first", SequenceSourceDefinition::new(0));
    let second = builder.stage("second", SequenceSourceDefinition::new(1));
    let count = builder.stage("count", CountDefinition::new());
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
    let mut builder = Flow::builder(&self_loop_path);
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([count], count);
    let error = build_error(builder);
    assert!(matches!(
        error,
        FlowError::Topology(TopologyError::SelfLoop(id)) if id == "count"
    ));
    assert!(!self_loop_path.exists());

    let cycle_path = root.path().join("cycle");
    let mut builder = Flow::builder(&cycle_path);
    let left = builder.stage("left", CountDefinition::new());
    let right = builder.stage("right", CountDefinition::new());
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

    let mut builder = Flow::builder(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
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

fn build_error(builder: FlowBuilder) -> FlowError {
    let Err(error) = builder.build() else {
        panic!("invalid Flow unexpectedly built");
    };
    error
}
