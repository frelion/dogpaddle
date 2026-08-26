use dogpaddle_store::{Cell, OrderedMap, Small, Store, StoreData, StoreValue, Transactions};
use tempfile::TempDir;

use crate::{STAGE_KEYS, VALUE_BYTES, support::sample_dir};

pub(super) type ByteMap<SIZE> = OrderedMap<Vec<u8>, Vec<u8>, SIZE>;
pub(super) type TypedMap<SIZE> = OrderedMap<u64, Vec<u8>, SIZE>;

pub(super) struct Fixture<SIZE> {
    pub(super) transactions: Transactions,
    pub(super) bytes: ByteMap<SIZE>,
    pub(super) map: TypedMap<SIZE>,
    _root: TempDir,
}

pub(super) struct StageFixture<SIZE> {
    pub(super) transactions: Transactions,
    pub(super) cursor: Cell<u64>,
    pub(super) map: TypedMap<SIZE>,
    _root: TempDir,
}

pub(super) struct ScanFixture<V, SIZE> {
    pub(super) transactions: Transactions,
    pub(super) map: OrderedMap<u64, V, SIZE>,
    _root: TempDir,
}

impl<SIZE> Fixture<SIZE>
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    pub(super) fn empty() -> Self {
        let root = sample_dir("ordered-map");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = store
            .create_data::<TypedMap<SIZE>>("map")
            .expect("create benchmark map");
        let bytes = store
            .create_data::<ByteMap<SIZE>>("bytes")
            .expect("create benchmark byte map");
        Self {
            transactions: store.into_transactions(),
            bytes,
            map,
            _root: root,
        }
    }

    pub(super) fn populated_typed(entries: usize) -> Self {
        let mut fixture = Self::empty();
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin seed transaction");
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access seed map");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..entries {
            map.put(&(key as u64), &value).expect("seed benchmark map");
        }
        transaction.commit().expect("commit benchmark seed");
        fixture
    }

    pub(super) fn populated_bytes(entries: usize) -> Self {
        let mut fixture = Self::empty();
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin byte map seed transaction");
        let mut bytes = fixture
            .bytes
            .access(transaction.access())
            .expect("access seed byte map");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..entries {
            bytes
                .put(&(key as u64).to_be_bytes().to_vec(), &value)
                .expect("seed benchmark byte map");
        }
        transaction
            .commit()
            .expect("commit benchmark byte map seed");
        fixture
    }

    pub(super) fn populated_with_small_background(
        entries: usize,
        background_namespaces: usize,
    ) -> Self {
        let root = sample_dir("ordered-map-background");
        let mut store = Store::create(root.path().join("store")).expect("create benchmark store");
        let map = store
            .create_data::<TypedMap<SIZE>>("target")
            .expect("create target map");
        let bytes = store
            .create_data::<ByteMap<SIZE>>("target-bytes")
            .expect("create target byte map");
        let backgrounds = (0..background_namespaces)
            .map(|index| {
                store
                    .create_data::<OrderedMap<u64, Vec<u8>, Small>>(&format!("background-{index}"))
                    .expect("create background map")
            })
            .collect::<Vec<_>>();
        let mut fixture = Self {
            transactions: store.into_transactions(),
            bytes,
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin mixed seed transaction");
        let value = vec![0x5a; VALUE_BYTES];
        {
            let mut target = fixture
                .map
                .access(transaction.access())
                .expect("access target map");
            for key in 0..entries {
                target.put(&(key as u64), &value).expect("seed target map");
            }
        }
        let entries_per_background = entries / background_namespaces;
        let extra_entries = entries % background_namespaces;
        for (index, background) in backgrounds.iter().enumerate() {
            let mut background = background
                .access(transaction.access())
                .expect("access background map");
            let background_entries = entries_per_background + usize::from(index < extra_entries);
            for key in 0..background_entries {
                background
                    .put(&(key as u64), &value)
                    .expect("seed background map");
            }
        }
        transaction.commit().expect("commit mixed benchmark seed");
        fixture
    }
}

impl<SIZE> StageFixture<SIZE>
where
    TypedMap<SIZE>: StoreData,
{
    pub(super) fn populated() -> Self {
        let root = sample_dir("ordered-map-multi-collection");
        let mut store = Store::create(root.path().join("store")).expect("create stage store");
        let cursor = store
            .create_data::<Cell<u64>>("cursor")
            .expect("create stage cursor");
        let map = store
            .create_data::<TypedMap<SIZE>>("map")
            .expect("create stage map");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            cursor,
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin stage seed transaction");
        fixture
            .cursor
            .access(transaction.access())
            .expect("access stage cursor")
            .set(&0)
            .expect("seed stage cursor");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access stage map");
            let value = vec![0x5a; VALUE_BYTES];
            for key in 0..STAGE_KEYS {
                map.put(&(key as u64), &value).expect("seed stage map");
            }
        }
        transaction.commit().expect("commit stage seed");
        fixture
    }
}

impl<V: StoreValue, SIZE> ScanFixture<V, SIZE>
where
    OrderedMap<u64, V, SIZE>: StoreData,
{
    pub(super) fn populated(entries: usize, value: &V) -> Self {
        let root = sample_dir("ordered-map-scan");
        let mut store =
            Store::create(root.path().join("store")).expect("create scan benchmark store");
        let map = store
            .create_data::<OrderedMap<u64, V, SIZE>>("map")
            .expect("create scan benchmark map");
        let mut fixture = Self {
            transactions: store.into_transactions(),
            map,
            _root: root,
        };
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin scan benchmark seed transaction");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access scan benchmark seed map");
            for key in 0..entries {
                map.put(&(key as u64), value)
                    .expect("seed scan benchmark map");
            }
        }
        transaction
            .commit()
            .expect("commit scan benchmark seed transaction");
        fixture
    }
}
