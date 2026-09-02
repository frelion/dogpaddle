#[path = "append_log_errors.rs"]
mod errors;
#[path = "append_log_projection.rs"]
mod projection;

use std::{
    collections::VecDeque,
    num::{NonZeroU64, NonZeroUsize},
};

use dogpaddle_store::{AppendLog, AppendLogAccess, ScanLimit, Store, StoreError, StoreValue};

use crate::support::store_path;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LogModel {
    head: u64,
    entries: VecDeque<Vec<u8>>,
}

type ModelPage = Result<(Vec<(u64, Vec<u8>)>, u64, bool), usize>;

impl LogModel {
    fn tail(&self) -> u64 {
        self.head + u64::try_from(self.entries.len()).unwrap()
    }

    fn retained_bytes(&self) -> u64 {
        self.entries
            .iter()
            .map(|value| u64::try_from(size_of::<u64>() + value.len()).unwrap())
            .sum()
    }

    fn append(&mut self, value: &[u8]) -> u64 {
        let offset = self.tail();
        self.entries.push_back(value.to_vec());
        offset
    }

    fn append_batch(&mut self, values: &[Vec<u8>]) -> std::ops::Range<u64> {
        let start = self.tail();
        self.entries.extend(values.iter().cloned());
        start..self.tail()
    }

    fn try_append(&mut self, value: &[u8], capacity: NonZeroU64) -> Option<u64> {
        let item_bytes = u64::try_from(size_of::<u64>() + value.len()).unwrap();
        if !self.entries.is_empty()
            && self.retained_bytes().checked_add(item_bytes).unwrap() > capacity.get()
        {
            return None;
        }
        Some(self.append(value))
    }

    fn truncate_before(&mut self, target: u64, max_items: NonZeroUsize) -> u64 {
        assert!(target <= self.tail());
        let removable = usize::try_from(target - self.head)
            .unwrap()
            .min(max_items.get());
        self.entries.drain(..removable);
        self.head += u64::try_from(removable).unwrap();
        self.head
    }

    fn page(&self, offset: u64, max_items: usize, max_bytes: usize) -> ModelPage {
        assert!((self.head..=self.tail()).contains(&offset));
        let mut bytes = 0;
        let mut values = Vec::new();
        for (index, value) in self
            .entries
            .iter()
            .skip(usize::try_from(offset - self.head).unwrap())
            .enumerate()
        {
            let item_bytes = size_of::<u64>() + value.len();
            if values.is_empty() && item_bytes > max_bytes {
                return Err(item_bytes);
            }
            if values.len() == max_items || bytes + item_bytes > max_bytes {
                break;
            }
            values.push((offset + u64::try_from(index).unwrap(), value.clone()));
            bytes += item_bytes;
        }
        let next = offset + u64::try_from(values.len()).unwrap();
        Ok((values, next, next == self.tail()))
    }
}

fn create_log<T: StoreValue>(store: &mut Store, name: &str) -> AppendLog<T> {
    store.create_data(name).unwrap()
}

fn scan_values<T: StoreValue>(
    access: &AppendLogAccess<'_, T>,
    offset: u64,
    limit: ScanLimit,
) -> (Vec<(u64, T)>, dogpaddle_store::AppendLogScan) {
    let mut values = Vec::new();
    let scan = access
        .scan(offset, limit, |entry| -> Result<(), StoreError> {
            values.push((entry.offset(), entry.decode_owned()?));
            Ok(())
        })
        .unwrap();
    (values, scan)
}

fn assert_state(access: &AppendLogAccess<'_, Vec<u8>>, model: &LogModel) {
    assert_eq!(access.bounds().unwrap(), model.head..model.tail());
    assert_eq!(access.retained_bytes().unwrap(), model.retained_bytes());
}

fn assert_page(
    access: &AppendLogAccess<'_, Vec<u8>>,
    model: &LogModel,
    offset: u64,
    max_items: usize,
    max_bytes: usize,
) {
    let expected = model.page(offset, max_items, max_bytes);
    let mut actual = Vec::new();
    let result = access.scan(
        offset,
        ScanLimit::new(max_items, max_bytes).unwrap(),
        |entry| {
            actual.push((entry.offset(), entry.decode_owned()?));
            Ok::<(), StoreError>(())
        },
    );
    match expected {
        Ok((values, next_offset, caught_up)) => {
            let scan = result.unwrap();
            assert_eq!(actual, values);
            assert_eq!(scan.next_offset, next_offset);
            assert_eq!(scan.caught_up, caught_up);
        }
        Err(size) => assert!(matches!(
            result,
            Err(StoreError::ItemTooLarge { size: actual, limit })
                if actual == size && limit == max_bytes
        )),
    }
}

#[test]
fn append_log_matches_an_independent_trace_across_commit_drop_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    let mut store = Store::create(&path).unwrap();
    let log = create_log(&mut store, "log");
    let mut transactions = store.into_transactions();
    let mut model = LogModel::default();

    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        assert_state(&access, &model);
        assert_page(&access, &model, 0, 4, 64);

        assert_eq!(access.append_batch(&[]).unwrap(), model.append_batch(&[]));
        assert_eq!(access.append(&b"a".to_vec()).unwrap(), model.append(b"a"));
        let batch = [b"bb".to_vec(), b"ccc".to_vec()];
        assert_eq!(
            access.append_batch(&batch).unwrap(),
            model.append_batch(&batch)
        );
        let accepting_capacity = NonZeroU64::new(model.retained_bytes() + 12).unwrap();
        assert_eq!(
            access
                .try_append(&b"dddd".to_vec(), accepting_capacity)
                .unwrap(),
            model.try_append(b"dddd", accepting_capacity)
        );
        let capacity = NonZeroU64::new(model.retained_bytes()).unwrap();
        assert_eq!(
            access.try_append(&Vec::new(), capacity).unwrap(),
            model.try_append(&[], capacity)
        );
        assert_state(&access, &model);
        assert_page(&access, &model, 0, 2, 64);
        assert_page(&access, &model, 2, 8, 11);
        transaction.commit().unwrap();
    }

    // Append and truncate may share a transaction, but dropping it leaves the
    // committed model unchanged.
    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        access.append(&b"dropped".to_vec()).unwrap();
        access
            .truncate_before(2, NonZeroUsize::new(2).unwrap())
            .unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        assert_state(&access, &model);
        assert_page(&access, &model, model.head, 8, 64);
    }

    {
        let transaction = transactions.begin().unwrap();
        let mut access = log.access(transaction.access()).unwrap();
        let one = NonZeroUsize::new(1).unwrap();
        assert_eq!(
            access.truncate_before(2, one).unwrap(),
            model.truncate_before(2, one)
        );
        assert_eq!(
            access.truncate_before(2, one).unwrap(),
            model.truncate_before(2, one)
        );
        let all = NonZeroUsize::new(8).unwrap();
        let tail = model.tail();
        assert_eq!(
            access.truncate_before(tail, all).unwrap(),
            model.truncate_before(tail, all)
        );

        let capacity = NonZeroU64::new(17).unwrap();
        let oversize = vec![7; 20];
        assert_eq!(
            access.try_append(&oversize, capacity).unwrap(),
            model.try_append(&oversize, capacity)
        );
        assert_eq!(
            access.try_append(&Vec::new(), capacity).unwrap(),
            model.try_append(&[], capacity)
        );
        assert_state(&access, &model);

        // Admission failure is soft and retryable at the exact encoded
        // key-plus-value size.
        assert_page(&access, &model, model.head, 1, 27);
        assert_page(&access, &model, model.head, 1, 28);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log = store.open_data::<AppendLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_state(&access, &model);
    assert_page(&access, &model, model.head, 8, 1_024);
}
