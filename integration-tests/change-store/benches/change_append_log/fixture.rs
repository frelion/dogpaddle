use dogpaddle_store::{AppendLog, Cell, Store, Transactions};

use crate::{
    case::BenchmarkCase,
    support::{BenchStoreRoot, SampleStore},
};

pub(crate) struct AppendFixture {
    pub(crate) sample: SampleStore,
    pub(crate) log: AppendLog<Vec<u8>>,
    pub(crate) transactions: Transactions,
}

pub(crate) struct ReplayFixture {
    pub(crate) sample: SampleStore,
    pub(crate) input: AppendLog<Vec<u8>>,
    pub(crate) output: Option<AppendLog<Vec<u8>>>,
    pub(crate) cursor: Option<Cell<u64>>,
    pub(crate) transactions: Transactions,
}

pub(crate) fn empty_append(root: &BenchStoreRoot, label: &str) -> AppendFixture {
    let sample = root.sample(label);
    let mut store = Store::create(sample.path()).expect("create append benchmark Store");
    let log = store
        .create_data::<AppendLog<Vec<u8>>>("changes")
        .expect("create append benchmark log");
    let transactions = store.into_transactions();
    AppendFixture {
        sample,
        log,
        transactions,
    }
}

pub(crate) fn seeded_replay(
    root: &BenchStoreRoot,
    label: &str,
    case: &BenchmarkCase,
    with_pipeline_output: bool,
) -> ReplayFixture {
    let sample = root.sample(label);
    let mut store = Store::create(sample.path()).expect("create replay benchmark Store");
    let input = store
        .create_data::<AppendLog<Vec<u8>>>("input")
        .expect("create replay input log");
    let output = with_pipeline_output.then(|| {
        store
            .create_data::<AppendLog<Vec<u8>>>("output")
            .expect("create replay output log")
    });
    let cursor = with_pipeline_output.then(|| {
        store
            .create_data::<Cell<u64>>("cursor")
            .expect("create replay cursor")
    });
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().expect("begin replay seed transaction");
        input
            .access(transaction.access())
            .expect("access replay seed log")
            .append_batch(&case.workload.encoded)
            .expect("append replay seed Changes");
        transaction.commit().expect("commit replay seed Changes");
    }
    ReplayFixture {
        sample,
        input,
        output,
        cursor,
        transactions,
    }
}
