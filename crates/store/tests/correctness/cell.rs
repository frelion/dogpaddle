use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, Small, Store, StoreError, StoreValue};

use crate::support::{TestValue, store_path};

fn create_cell<T: StoreValue>(store: &mut Store, name: &str) -> Result<Cell<T>, StoreError> {
    store.create_data(name)
}

fn open_cell<T: StoreValue>(store: &Store, name: &str) -> Result<Cell<T>, StoreError> {
    store.open_data(name)
}

#[test]
fn cell_state_transitions_are_exact() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = create_cell::<u64>(&mut store, "cell").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = cell.access(transaction.access()).unwrap();
    assert_eq!(access.get().unwrap(), None);
    access.set(&1).unwrap();
    assert_eq!(access.get().unwrap(), Some(1));
    access.set(&2).unwrap();
    assert_eq!(access.get().unwrap(), Some(2));
    assert!(access.clear().unwrap());
    assert!(!access.clear().unwrap());
    assert_eq!(access.get().unwrap(), None);
    transaction.commit().unwrap();
}

#[test]
fn cell_uses_custom_value_codecs_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let cell = create_cell::<TestValue>(&mut store, "cell").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    cell.access(transaction.access())
        .unwrap()
        .set(&TestValue(42))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let cell = open_cell::<TestValue>(&store, "cell").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        cell.access(transaction.access()).unwrap().get().unwrap(),
        Some(TestValue(42))
    );
}

struct BrokenValue;

struct OwnershipObservedValue {
    value: u64,
    input_was_owned: bool,
}

impl StoreValue for OwnershipObservedValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.value.to_be_bytes())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let input_was_owned = matches!(&bytes, Cow::Owned(_));
        let bytes = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid observed value"))?;
        Ok(Self {
            value: u64::from_be_bytes(bytes),
            input_was_owned,
        })
    }
}

#[test]
fn dirty_cell_values_are_owned_for_decoding() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = create_cell::<OwnershipObservedValue>(&mut store, "cell").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    let mut access = cell.access(transaction.access()).unwrap();
    access
        .set(&OwnershipObservedValue {
            value: 42,
            input_was_owned: false,
        })
        .unwrap();
    let decoded = access.get().unwrap().unwrap();
    assert_eq!(decoded.value, 42);
    assert!(decoded.input_was_owned);
}

impl StoreValue for BrokenValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional encode failure"))
    }

    fn decode_value(_bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional decode failure"))
    }
}

#[test]
fn encoding_failure_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_cell::<u64>(&mut store, "safe").unwrap();
    let broken = create_cell::<BrokenValue>(&mut store, "broken").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(transaction.access()).unwrap().set(&99).unwrap();
    assert!(matches!(
        broken
            .access(transaction.access())
            .unwrap()
            .set(&BrokenValue),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
}

#[test]
fn decoding_failure_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_cell::<u64>(&mut store, "safe").unwrap();
    let broken_data = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("broken")
        .unwrap();
    let broken = open_cell::<BrokenValue>(&store, "broken").unwrap();
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(transaction.access())
        .unwrap()
        .set(&100)
        .unwrap();
    broken_data
        .access(transaction.access())
        .unwrap()
        .put(&Vec::new(), &b"invalid".to_vec())
        .unwrap();
    assert!(matches!(
        broken.access(transaction.access()).unwrap().get(),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(
        safe.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        broken_data
            .access(transaction.access())
            .unwrap()
            .get(&Vec::new())
            .unwrap(),
        None
    );
}
