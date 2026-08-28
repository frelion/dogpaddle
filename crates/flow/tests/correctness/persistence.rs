use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    source::SequenceSourceDefinition, transform::CountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, Large, OrderedMap, Small, Store, StoreError};

use super::support::{build_source_and_read_definition, fixture_bytes, read_published_definition};

const V1_SEQUENCE_COUNT: &str = include_str!("../fixtures/v1/sequence_source_count.hex");

#[test]
fn build_publishes_the_stable_v1_definition_bytes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(7));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);
    drop(builder.build().unwrap());

    assert_eq!(
        read_published_definition(&path),
        fixture_bytes(V1_SEQUENCE_COUNT)
    );
    let flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["source", "count"]);
}

#[test]
fn build_uses_the_stable_resource_layout() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);
    drop(builder.build().unwrap());

    let store = Store::open(&path).unwrap();
    let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let _flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.open_data("flow/state").unwrap();
    let _source_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000000/state").unwrap();
    let _count_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let _source_output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let _terminal_count_output: AppendLog<Vec<u8>> =
        store.open_data("station/00000001/output").unwrap();
    let _source_position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let _count: Cell<u64> = store.open_data("station/00000001/operation/count").unwrap();
    let (_, reads) = store.into_transactions().split();
    let transaction = reads.begin().unwrap();
    assert!(
        definition
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap()
            .is_some()
    );
}

#[test]
fn build_initializes_input_state_at_the_stable_origins() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    builder.connect([source], count);
    drop(builder.build().unwrap());

    let store = Store::open(&path).unwrap();
    let source_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000000/state").unwrap();
    let count_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let (_, reads) = store.into_transactions().split();
    let transaction = reads.begin().unwrap();
    let active_key = b"input/active".to_vec();
    let cursor_key = b"input/00000000/cursor".to_vec();

    assert_eq!(
        source_state
            .read(transaction.access())
            .unwrap()
            .get(&active_key)
            .unwrap(),
        None
    );
    assert_eq!(
        source_state
            .read(transaction.access())
            .unwrap()
            .get(&cursor_key)
            .unwrap(),
        None
    );
    assert_eq!(
        count_state
            .read(transaction.access())
            .unwrap()
            .get(&active_key)
            .unwrap(),
        Some(vec![0; 4])
    );
    assert_eq!(
        count_state
            .read(transaction.access())
            .unwrap()
            .get(&cursor_key)
            .unwrap(),
        Some(vec![0; 8])
    );
}

#[test]
fn open_reports_a_missing_station_output_after_publication() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_source_and_read_definition(&root.path().join("complete"));

    let incomplete_path = root.path().join("missing-output");
    let mut store = Store::create(&incomplete_path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let _flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.create_data("flow/state").unwrap();
    let _station_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.create_data("station/00000000/state").unwrap();
    let _position: Cell<u64> = store
        .create_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        published
            .access(transaction.access())
            .unwrap()
            .set(&definition)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        FlowFactory::open(&incomplete_path),
        Err(FlowError::MissingResource { name })
            if name == "station/00000000/output"
    ));
}

#[test]
fn open_reports_a_station_output_size_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_source_and_read_definition(&root.path().join("complete"));

    let mismatched_path = root.path().join("output-size-mismatch");
    let mut store = Store::create(&mismatched_path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let _flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.create_data("flow/state").unwrap();
    let _station_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.create_data("station/00000000/state").unwrap();
    let _position: Cell<u64> = store
        .create_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let _wrong_output: Cell<Vec<u8>> = store.create_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        published
            .access(transaction.access())
            .unwrap()
            .set(&definition)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        FlowFactory::open(&mismatched_path),
        Err(FlowError::Store(StoreError::DataSizeMismatch {
            name,
            expected: "large",
            actual: "small",
        })) if name == "station/00000000/output"
    ));
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
        FlowFactory::open(&path),
        Err(FlowError::IncompleteBuild)
    ));
}

#[test]
fn open_rejects_a_corrupt_published_definition() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let definition: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut definition = definition.access(transaction.access()).unwrap();
        definition.set(&b"not a flow".to_vec()).unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        FlowFactory::open(&path),
        Err(FlowError::Definition(_))
    ));
}

#[test]
fn open_reports_a_missing_resource_after_publication() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_source_and_read_definition(&root.path().join("complete"));

    let incomplete_path = root.path().join("missing-resource");
    let mut store = Store::create(&incomplete_path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let _flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.create_data("flow/state").unwrap();
    let _station_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.create_data("station/00000000/state").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        published
            .access(transaction.access())
            .unwrap()
            .set(&definition)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        FlowFactory::open(&incomplete_path),
        Err(FlowError::MissingResource { name })
            if name == "station/00000000/operation/sequence_source.position"
    ));
}

#[test]
fn open_reports_an_operation_data_size_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let definition = build_source_and_read_definition(&root.path().join("complete"));

    let mismatched_path = root.path().join("size-mismatch");
    let mut store = Store::create(&mismatched_path).unwrap();
    let published: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let _flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> = store.create_data("flow/state").unwrap();
    let _station_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.create_data("station/00000000/state").unwrap();
    let _position: OrderedMap<u64, u64, Large> = store
        .create_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        published
            .access(transaction.access())
            .unwrap()
            .set(&definition)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    assert!(matches!(
        FlowFactory::open(&mismatched_path),
        Err(FlowError::Store(StoreError::DataSizeMismatch {
            name,
            expected: "small",
            actual: "large",
        })) if name == "station/00000000/operation/sequence_source.position"
    ));
}
