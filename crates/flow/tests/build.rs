use dogpaddle_flow::{Flow, FlowError, TopologyError};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{Cell, DataPlacement, Store};

#[test]
fn build_freezes_and_open_rematerializes_a_real_flow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    let source = builder.stage("source", SequenceSourceDefinition::new(100));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([source], count);

    let flow = builder.build().unwrap();
    assert_eq!(flow.path(), path);
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let flow = Flow::open(&path).unwrap();
    assert_eq!(flow.stage_count(), 2);
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
}

#[test]
fn an_active_flow_exclusively_owns_its_store_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
    let flow = builder.build().unwrap();

    assert!(matches!(Flow::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    assert!(Flow::open(&path).is_ok());
}

#[test]
fn build_uses_the_stable_resource_layout() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    let source = builder.stage("source", SequenceSourceDefinition::new(0));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([source], count);
    drop(builder.build().unwrap());

    let store = Store::open(&path).unwrap();
    let definition: Cell<Vec<u8>> = Cell::new(store.open_data("flow/definition").unwrap());
    store.open_data("flow/state").unwrap();
    store.open_data("stage/00000000/state").unwrap();
    store.open_data("stage/00000001/state").unwrap();
    let _source_position: Cell<u64> = Cell::new(
        store
            .open_data("stage/00000000/operation/sequence_source.position")
            .unwrap(),
    );
    let _count: Cell<u64> = Cell::new(store.open_data("stage/00000001/operation/count").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert!(
        definition
            .access(&transaction)
            .unwrap()
            .get()
            .unwrap()
            .is_some()
    );
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
fn open_rejects_an_unpublished_build() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    store
        .create_data("flow/definition", DataPlacement::Shared)
        .unwrap();
    drop(store);

    assert!(matches!(Flow::open(&path), Err(FlowError::IncompleteBuild)));
}

#[test]
fn open_rejects_a_corrupt_published_definition() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let definition: Cell<Vec<u8>> = Cell::new(
        store
            .create_data("flow/definition", DataPlacement::Shared)
            .unwrap(),
    );
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut definition = definition.access(&transaction).unwrap();
        definition.set(&b"not a flow".to_vec()).unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(Flow::open(&path), Err(FlowError::Definition(_))));
}

#[test]
fn open_reports_a_missing_resource_after_publication() {
    let root = tempfile::tempdir().unwrap();
    let complete_path = root.path().join("complete");
    let mut builder = Flow::builder(&complete_path);
    builder.stage("source", SequenceSourceDefinition::new(0));
    drop(builder.build().unwrap());

    let store = Store::open(&complete_path).unwrap();
    let definition: Cell<Vec<u8>> = Cell::new(store.open_data("flow/definition").unwrap());
    let mut transactions = store.into_transactions();
    let definition = {
        let transaction = transactions.begin().unwrap();
        definition
            .access(&transaction)
            .unwrap()
            .get()
            .unwrap()
            .unwrap()
    };
    drop(transactions);

    let incomplete_path = root.path().join("missing-resource");
    let mut store = Store::create(&incomplete_path).unwrap();
    let published: Cell<Vec<u8>> = Cell::new(
        store
            .create_data("flow/definition", DataPlacement::Shared)
            .unwrap(),
    );
    store
        .create_data("flow/state", DataPlacement::Shared)
        .unwrap();
    store
        .create_data("stage/00000000/state", DataPlacement::Shared)
        .unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        published
            .access(&transaction)
            .unwrap()
            .set(&definition)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        Flow::open(&incomplete_path),
        Err(FlowError::MissingResource { name })
            if name == "stage/00000000/operation/sequence_source.position"
    ));
}
