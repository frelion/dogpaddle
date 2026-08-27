use dogpaddle_operation::{
    encode_definition,
    operation::{
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, ReadOnly, Small, Store, Transactions};

use super::Stage;

struct StageFixture {
    flow_transactions: Transactions,
    source: Stage,
    count: Stage,
    source_definition: SequenceSourceDefinition,
    count_definition: CountDefinition,
    _root: tempfile::TempDir,
}

#[test]
fn construction_boxes_heterogeneous_operations_and_keeps_stage_state_isolated() {
    let mut fixture = stage_fixture();

    assert_eq!(
        encode_definition(fixture.source.operation.definition()),
        encode_definition(&fixture.source_definition)
    );
    assert_eq!(
        encode_definition(fixture.count.operation.definition()),
        encode_definition(&fixture.count_definition)
    );

    put_state(&mut fixture.source, b"source");
    put_state(&mut fixture.count, b"count");

    let transaction = fixture.flow_transactions.begin().unwrap();
    assert_eq!(
        read_state(&fixture.source, transaction.access()),
        b"source".to_vec()
    );
    assert_eq!(
        read_state(&fixture.count, transaction.access()),
        b"count".to_vec()
    );
}

#[test]
fn stage_owns_transactions_and_output_while_the_downstream_input_is_read_only() {
    let mut fixture = stage_fixture();

    assert!(fixture.source.inputs.is_empty());
    assert_eq!(fixture.count.inputs.len(), 1);
    assert!(fixture.source.output.is_some());
    assert!(fixture.count.output.is_some());

    {
        let transaction = fixture.source.transactions.begin().unwrap();
        fixture
            .source
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append(&b"change".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = fixture.count.transactions.begin().unwrap();
        assert_eq!(
            fixture.count.inputs[0]
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
            0..1
        );
    }
}

fn stage_fixture() -> StageFixture {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();

    let source_definition = SequenceSourceDefinition::new(100);
    let source_state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("source-state")
        .unwrap();
    let source_operation = Box::new(SequenceSourceOperation::new(
        source_definition,
        store.create_data::<Cell<u64>>("source-position").unwrap(),
    ));
    let source_output = store
        .create_data::<AppendLog<Vec<u8>>>("source-output")
        .unwrap();

    let count_definition = CountDefinition::new();
    let count_state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("count-state")
        .unwrap();
    let count_operation = Box::new(CountOperation::new(
        count_definition,
        store.create_data::<Cell<u64>>("count-value").unwrap(),
    ));
    let count_output = store
        .create_data::<AppendLog<Vec<u8>>>("count-output")
        .unwrap();

    let flow_transactions = store.into_transactions();
    let source = Stage::new(
        flow_transactions.clone(),
        source_state,
        source_operation,
        Vec::new(),
        Some(source_output),
    );
    let count = Stage::new(
        flow_transactions.clone(),
        count_state,
        count_operation,
        vec![ReadOnly::new(
            source.output.as_ref().expect("source has output").clone(),
        )],
        Some(count_output),
    );
    StageFixture {
        flow_transactions,
        source,
        count,
        source_definition,
        count_definition,
        _root: root,
    }
}

fn put_state(stage: &mut Stage, value: &[u8]) {
    let transaction = stage.transactions.begin().unwrap();
    stage
        .state
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &value.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}

fn read_state(stage: &Stage, access: dogpaddle_store::TransactionAccess<'_>) -> Vec<u8> {
    stage
        .state
        .access(access)
        .unwrap()
        .get(&b"key".to_vec())
        .unwrap()
        .unwrap()
}
