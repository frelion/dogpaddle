use dogpaddle_store::{Cell, CodecError, DataPlacement, Store, StoreError, StoreValue};

use crate::support::{PLACEMENTS, TestValue, store_path};

fn create_cell<T: StoreValue>(
    store: &mut Store,
    name: &str,
    placement: DataPlacement,
) -> Result<Cell<T>, StoreError> {
    Ok(Cell::new(store.create_data(name, placement)?))
}

fn open_cell<T: StoreValue>(store: &Store, name: &str) -> Result<Cell<T>, StoreError> {
    Ok(Cell::new(store.open_data(name)?))
}

#[test]
fn cell_state_transitions_are_exact() {
    for placement in PLACEMENTS {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(store_path(&root)).unwrap();
        let cell = create_cell::<u64>(&mut store, "cell", placement).unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin().unwrap();
        let mut access = cell.access(&transaction).unwrap();
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
}

#[test]
fn cell_uses_custom_value_codecs_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let cell = create_cell::<TestValue>(&mut store, "cell", DataPlacement::Dedicated).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    cell.access(&transaction)
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
        cell.access(&transaction).unwrap().get().unwrap(),
        Some(TestValue(42))
    );
}

struct BrokenValue;

impl StoreValue for BrokenValue {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::new("intentional encode failure"))
    }

    fn decode_value(_bytes: &[u8]) -> Result<Self, CodecError> {
        Err(CodecError::new("intentional decode failure"))
    }
}

#[test]
fn encoding_failure_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_cell::<u64>(&mut store, "safe", DataPlacement::Shared).unwrap();
    let broken =
        Cell::<BrokenValue>::new(store.create_data("broken", DataPlacement::Shared).unwrap());
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(&transaction).unwrap().set(&99).unwrap();
    assert!(matches!(
        broken.access(&transaction).unwrap().set(&BrokenValue),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(safe.access(&transaction).unwrap().get().unwrap(), None);
}

#[test]
fn decoding_failure_poison_rolls_back_prior_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let safe = create_cell::<u64>(&mut store, "safe", DataPlacement::Shared).unwrap();
    let broken_data = store.create_data("broken", DataPlacement::Shared).unwrap();
    let broken = Cell::<BrokenValue>::new(broken_data.clone());
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin().unwrap();
    safe.access(&transaction).unwrap().set(&100).unwrap();
    broken_data
        .access(&transaction)
        .unwrap()
        .put(b"", b"invalid")
        .unwrap();
    assert!(matches!(
        broken.access(&transaction).unwrap().get(),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let transaction = transactions.begin().unwrap();
    assert_eq!(safe.access(&transaction).unwrap().get().unwrap(), None);
    assert_eq!(
        broken_data.access(&transaction).unwrap().get(b"").unwrap(),
        None
    );
}
