use dogpaddle_operation::SequenceSourceDefinition;
use dogpaddle_store::{OrderedMap, Store};

use super::Flow;

#[test]
fn open_rematerializes_flow_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    builder.stage("source", SequenceSourceDefinition::new(0));
    drop(builder.build().unwrap());

    let store = Store::open(&path).unwrap();
    let state: OrderedMap<Vec<u8>, Vec<u8>> =
        OrderedMap::new(store.open_data("flow/state").unwrap());
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        state
            .access(&transaction)
            .unwrap()
            .put(&b"key".to_vec(), &b"flow".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let mut flow = Flow::open(&path).unwrap();
    assert_eq!(flow.stages.len(), 1);
    let transaction = flow.transactions.begin().unwrap();
    assert_eq!(
        flow.state
            .access(&transaction)
            .unwrap()
            .get(&b"key".to_vec())
            .unwrap(),
        Some(b"flow".to_vec())
    );
}
