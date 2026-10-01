use super::support::{TestStore, assert_literal_definition, construct_checked, value_schema};
use arrow_array::UInt64Array;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::{
    OperationDefinition, OperationKind, RuntimeResource,
    operation::{Operation, scan::SequenceScanDefinition},
};
use dogpaddle_store::{Store, StoreSetup};
const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_scan_start_42.hex");
#[test]
fn definition_has_stable_v1_literal_exact_schema_and_published_queue() {
    let definition = SequenceScanDefinition::new(42);
    let decoded = assert_literal_definition(&definition, SEQUENCE_V1, OperationKind::Scan);
    assert_eq!(definition.start(), 42);
    assert_eq!(
        construct_checked(&decoded, &[]).unwrap().as_ref(),
        Some(&value_schema())
    );
    let root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (operation, _) = decoded
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    source.restore(reads.begin().access()).unwrap();
    let mut delivery = source.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(source.record(txn.access(), &mut delivery).unwrap());
    }
    assert!(source.published(reads.begin().access()).unwrap().is_none());
    {
        let txn = writes.begin();
        assert!(source.record(txn.access(), &mut delivery).unwrap());
        txn.commit().unwrap();
    }
    source.ack(delivery).unwrap();
    {
        let txn = writes.begin();
        let encoded = source.published(reads.begin().access()).unwrap().unwrap();
        let change = SchemaBoundChangeCodec::try_new(value_schema())
            .unwrap()
            .decode_owned(encoded)
            .unwrap();
        source.consume_published(txn.access()).unwrap();
        assert_eq!(
            change
                .records()
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[42]
        );
    }
    {
        let txn = writes.begin();
        let encoded = source.published(reads.begin().access()).unwrap().unwrap();
        let change = SchemaBoundChangeCodec::try_new(value_schema())
            .unwrap()
            .decode_owned(encoded)
            .unwrap();
        source.consume_published(txn.access()).unwrap();
        assert_eq!(change.num_rows(), 1);
        txn.commit().unwrap();
    }
    drop((source, writes, reads));
    let store = Store::open(root.path()).unwrap();
    store
        .open_data::<dogpaddle_store::Queue<Vec<u8>>>("operation/sequence_scan.published")
        .unwrap();
}
#[test]
fn terminal_position_and_captured_data_survive_reopen_before_ack() {
    let root = TestStore::new();
    let definition: OperationDefinition = SequenceScanDefinition::new(u64::MAX).into();
    let mut setup = StoreSetup::new();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    source.restore(reads.begin().access()).unwrap();
    let mut delivery = source.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(source.record(txn.access(), &mut delivery).unwrap());
        txn.commit().unwrap();
    }
    drop((delivery, source, writes, reads));
    let store = Store::open(root.path()).unwrap();
    let (operation, _) = definition
        .construct(
            &[],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = store.into_transactions().split();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    source.restore(reads.begin().access()).unwrap();
    assert!(source.poll().unwrap().is_none());
    let txn = writes.begin();
    assert!(source.published(reads.begin().access()).unwrap().is_some());
    source.consume_published(txn.access()).unwrap();
    txn.commit().unwrap();
}

#[test]
fn restore_rejects_a_position_before_the_definition_without_rewriting_it() {
    let root = TestStore::new();
    let definition: OperationDefinition = SequenceScanDefinition::new(42).into();
    let mut setup = StoreSetup::new();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    drop(operation);
    drop(setup.commit(root.path(), |_| Ok(())).unwrap());
    let store = Store::open(root.path()).unwrap();
    let position: dogpaddle_store::Cell<u64> =
        store.open_data("operation/sequence_scan.position").unwrap();
    let (operation, _) = definition
        .construct(
            &[],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = store.into_transactions().split();
    {
        let txn = writes.begin();
        position.access(txn.access()).unwrap().set(&41).unwrap();
        txn.commit().unwrap();
    }
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    assert!(source.restore(reads.begin().access()).is_err());
    assert_eq!(
        position
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        Some(41)
    );
}

#[test]
fn capturing_a_successor_preserves_the_front_through_rollback_and_reopen() {
    let root = TestStore::new();
    let definition: OperationDefinition = SequenceScanDefinition::new(42).into();
    let mut setup = StoreSetup::new();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    source.restore(reads.begin().access()).unwrap();
    let mut first = source.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(source.record(txn.access(), &mut first).unwrap());
        txn.commit().unwrap();
    }
    source.ack(first).unwrap();
    let front = source.published(reads.begin().access()).unwrap().unwrap();
    let mut successor = source.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(source.record(txn.access(), &mut successor).unwrap());
        txn.commit().unwrap();
    }
    source.ack(successor).unwrap();
    assert_eq!(
        source.published(reads.begin().access()).unwrap(),
        Some(front.clone())
    );
    {
        let txn = writes.begin();
        source.consume_published(txn.access()).unwrap();
    }
    drop((source, writes, reads));
    let store = Store::open(root.path()).unwrap();
    let (operation, _) = definition
        .construct(
            &[],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = store.into_transactions().split();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    source.restore(reads.begin().access()).unwrap();
    assert_eq!(
        source.published(reads.begin().access()).unwrap(),
        Some(front)
    );
    {
        let txn = writes.begin();
        source.consume_published(txn.access()).unwrap();
        txn.commit().unwrap();
    }
    let encoded = source.published(reads.begin().access()).unwrap().unwrap();
    let change = SchemaBoundChangeCodec::try_new(value_schema())
        .unwrap()
        .decode_owned(encoded)
        .unwrap();
    let values = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.values(), &[43]);
    {
        let txn = writes.begin();
        source.consume_published(txn.access()).unwrap();
        txn.commit().unwrap();
    }
    assert!(source.published(reads.begin().access()).unwrap().is_none());
}
