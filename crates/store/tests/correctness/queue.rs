use std::{borrow::Cow, num::NonZeroU64};

use dogpaddle_store::{Cell, CodecError, Queue, Store, StoreError, StoreValue};

use crate::support::store_path;

#[test]
fn queue_is_fifo_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let queue = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    let mut queue = queue.access(transaction.access()).unwrap();
    assert!(queue.is_empty().unwrap());
    assert_eq!(queue.queued_bytes().unwrap(), 0);
    assert_eq!(queue.pop_front().unwrap(), None);
    assert!(
        queue
            .try_push(&b"first".to_vec(), NonZeroU64::new(100).unwrap())
            .unwrap()
    );
    assert!(
        queue
            .try_push(&b"second".to_vec(), NonZeroU64::new(100).unwrap())
            .unwrap()
    );
    assert_eq!(queue.queued_bytes().unwrap(), 8 + 5 + 8 + 6);
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let queue = store.open_data::<Queue<Vec<u8>>>("queue").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut queue = queue.access(transaction.access()).unwrap();
    assert_eq!(queue.pop_front().unwrap(), Some(b"first".to_vec()));
    assert_eq!(queue.queued_bytes().unwrap(), 8 + 6);
    assert_eq!(queue.pop_front().unwrap(), Some(b"second".to_vec()));
    assert!(queue.is_empty().unwrap());
    assert_eq!(queue.queued_bytes().unwrap(), 0);
    assert_eq!(queue.pop_front().unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn capacity_is_hard_even_for_an_empty_queue_and_counts_the_private_key() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let queue = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
    let mut transactions = store.into_transactions();
    let value = vec![0; 100];

    let transaction = transactions.begin();
    let mut queue = queue.access(transaction.access()).unwrap();
    assert!(
        !queue
            .try_push(&value, NonZeroU64::new(107).unwrap())
            .unwrap()
    );
    assert!(queue.is_empty().unwrap());
    assert_eq!(queue.queued_bytes().unwrap(), 0);
    assert!(
        queue
            .try_push(&value, NonZeroU64::new(108).unwrap())
            .unwrap()
    );
    assert_eq!(queue.queued_bytes().unwrap(), 108);
    assert!(
        !queue
            .try_push(&Vec::new(), NonZeroU64::new(115).unwrap())
            .unwrap()
    );
    assert_eq!(queue.pop_front().unwrap(), Some(value.clone()));
    assert!(queue.is_empty().unwrap());

    // Emptying the queue resets its private sequence and capacity accounting.
    assert!(
        queue
            .try_push(&value, NonZeroU64::new(108).unwrap())
            .unwrap()
    );
    transaction.commit().unwrap();
}

#[test]
fn pop_and_other_state_roll_back_together() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let queue = store.create_data::<Queue<u64>>("queue").unwrap();
    let cell = store.create_data::<Cell<u64>>("cell").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    let mut access = queue.access(transaction.access()).unwrap();
    assert!(access.try_push(&10, NonZeroU64::new(100).unwrap()).unwrap());
    assert!(access.try_push(&20, NonZeroU64::new(100).unwrap()).unwrap());
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    assert_eq!(
        queue
            .access(transaction.access())
            .unwrap()
            .pop_front()
            .unwrap(),
        Some(10)
    );
    cell.access(transaction.access()).unwrap().set(&1).unwrap();
    drop(transaction);

    let transaction = transactions.begin();
    assert_eq!(
        queue
            .access(transaction.access())
            .unwrap()
            .pop_front()
            .unwrap(),
        Some(10)
    );
    assert_eq!(
        cell.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    transaction.commit().unwrap();
}

struct BrokenValue;

impl StoreValue for BrokenValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional encode failure"))
    }

    fn decode_value(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional decode failure"))
    }
}

#[test]
fn codec_failures_poison_and_roll_back_the_whole_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let raw = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
    let broken = store.open_data::<Queue<BrokenValue>>("queue").unwrap();
    let broken_encode = store
        .create_data::<Queue<BrokenValue>>("broken_encode")
        .unwrap();
    let safe = store.create_data::<Cell<u64>>("safe").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    safe.access(transaction.access()).unwrap().set(&41).unwrap();
    assert!(matches!(
        broken_encode
            .access(transaction.access())
            .unwrap()
            .try_push(&BrokenValue, NonZeroU64::new(100).unwrap()),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert!(
        raw.access(transaction.access())
            .unwrap()
            .try_push(&vec![1], NonZeroU64::new(100).unwrap())
            .unwrap()
    );
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    safe.access(transaction.access()).unwrap().set(&42).unwrap();
    assert!(matches!(
        broken.access(transaction.access()).unwrap().pop_front(),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        raw.access(transaction.access())
            .unwrap()
            .pop_front()
            .unwrap(),
        Some(vec![1])
    );
}

#[test]
fn queue_has_its_own_persistent_collection_kind() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    store.create_data::<Queue<Vec<u8>>>("queue").unwrap();

    assert!(matches!(
        store.open_data::<Cell<Vec<u8>>>("queue"),
        Err(StoreError::DataKindMismatch {
            expected: "cell",
            actual: "queue",
            ..
        })
    ));
}
