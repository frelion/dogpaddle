use dogpaddle_store::{
    Cell, CodecError, Large, OrderedMap, Small, Store, StoreData, StoreError, StoreValue,
};

use crate::support::{TestValue, store_path};

fn create_cell<T: StoreValue, SIZE>(
    store: &mut Store,
    name: &str,
) -> Result<Cell<T, SIZE>, StoreError>
where
    Cell<T, SIZE>: StoreData,
{
    store.create_data(name)
}

fn open_cell<T: StoreValue, SIZE>(store: &Store, name: &str) -> Result<Cell<T, SIZE>, StoreError>
where
    Cell<T, SIZE>: StoreData,
{
    store.open_data(name)
}

#[test]
fn cell_state_transitions_are_exact() {
    assert_cell_state_transitions::<Small>();
    assert_cell_state_transitions::<Large>();
}

fn assert_cell_state_transitions<SIZE>()
where
    Cell<u64, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let cell = create_cell::<u64, SIZE>(&mut store, "cell").unwrap();
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
    let cell = create_cell::<TestValue, Large>(&mut store, "cell").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    cell.access(transaction.access())
        .unwrap()
        .set(&TestValue(42))
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let cell = open_cell::<TestValue, Large>(&store, "cell").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        cell.access(transaction.access()).unwrap().get().unwrap(),
        Some(TestValue(42))
    );
}

struct BrokenValue;

impl StoreValue for BrokenValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Err::<[u8; 0], _>(CodecError::new("intentional encode failure"))
    }

    fn decode_value(_bytes: Vec<u8>) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional decode failure"))
    }
}

#[test]
fn encoding_failure_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_cell::<u64, Small>(&mut store, "safe").unwrap();
    let broken = create_cell::<BrokenValue, Small>(&mut store, "broken").unwrap();
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
    let safe = create_cell::<u64, Small>(&mut store, "safe").unwrap();
    let broken_data = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("broken")
        .unwrap();
    let broken = open_cell::<BrokenValue, Small>(&store, "broken").unwrap();
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
