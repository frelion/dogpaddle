use dogpaddle_perf_context::RunRoot;
use dogpaddle_store::{Cell, OrderedMap, ReadTransactions, Store, Transactions};
use tempfile::TempDir;

use crate::{STATION_KEYS, VALUE_BYTES};

pub(super) type StateMap = OrderedMap<u64, Vec<u8>>;

pub(super) struct MapFixture {
    pub(super) writes: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) map: StateMap,
    _root: TempDir,
}

pub(super) struct StationFixture {
    pub(super) writes: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) step: Cell<u64>,
    pub(super) map: StateMap,
    _root: TempDir,
}

impl MapFixture {
    pub(super) fn empty(root: &RunRoot, scenario: &str) -> Self {
        let sample = root.sample(scenario);
        let mut store = Store::create(sample.path().join("store")).expect("create benchmark store");
        let map = store
            .create_data::<StateMap>("map")
            .expect("create benchmark map");
        let (writes, reads) = store.into_transactions().split();
        Self {
            writes,
            reads,
            map,
            _root: sample,
        }
    }

    pub(super) fn populated(
        root: &RunRoot,
        scenario: &str,
        entries: usize,
        value_bytes: usize,
    ) -> Self {
        let mut fixture = Self::empty(root, scenario);
        let value = vec![0x5a; value_bytes];
        let transaction = fixture.writes.begin();
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access benchmark seed map");
            for key in 0..u64::try_from(entries).expect("entry count fits u64") {
                map.put(&key, &value).expect("seed benchmark map");
            }
        }
        transaction.commit().expect("commit benchmark seed");
        fixture
    }
}

impl StationFixture {
    pub(super) fn populated(root: &RunRoot) -> Self {
        let sample = root.sample("station");
        let mut store = Store::create(sample.path().join("store")).expect("create station store");
        let step = store
            .create_data::<Cell<u64>>("step")
            .expect("create station step");
        let map = store
            .create_data::<StateMap>("map")
            .expect("create station map");
        let (mut writes, reads) = store.into_transactions().split();
        let transaction = writes.begin();
        step.access(transaction.access())
            .expect("access station step")
            .set(&0)
            .expect("seed station step");
        {
            let mut map = map
                .access(transaction.access())
                .expect("access station seed map");
            let value = vec![0x5a; VALUE_BYTES];
            for key in 0..u64::try_from(STATION_KEYS).expect("station key count fits u64") {
                map.put(&key, &value).expect("seed station map");
            }
        }
        transaction.commit().expect("commit station seed");
        Self {
            writes,
            reads,
            step,
            map,
            _root: sample,
        }
    }
}
