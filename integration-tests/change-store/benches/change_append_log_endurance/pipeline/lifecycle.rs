use std::{collections::VecDeque, ops::Range, path::Path, time::Duration};

use dogpaddle_store::{
    AppendLog, Cell, CodecError as StoreCodecError, ScanLimit, Store, StoreError, Transactions,
};

use crate::support::decode_entry;

use super::super::{
    config::{Config, WorkloadMode},
    workload::{EntryOracle, WorkloadStream},
};

pub(super) const LOG_NAME: &str = "changes";
const FULL_CURSOR_NAME: &str = "full_consumer_cursor";
const PROJECTED_CURSOR_NAME: &str = "projected_consumer_cursor";

pub(super) struct Session {
    pub(super) log: AppendLog<Vec<u8>>,
    pub(super) full_cursor: Cell<u64>,
    pub(super) projected_cursor: Cell<u64>,
    pub(super) transactions: Transactions,
}

pub(super) struct Preflight {
    pub(super) max_entry_bytes: usize,
}

pub(super) fn preflight(config: &Config, mode: WorkloadMode) -> Preflight {
    let mut stream = WorkloadStream::new(config, mode);
    let representative = stream.prepare(if mode == WorkloadMode::HeterogeneousPipeline {
        8
    } else {
        1
    });
    let max_entry_bytes = representative
        .entries
        .iter()
        .map(|entry| entry.expected_encoded.len())
        .max()
        .expect("representative workload is non-empty");
    assert!(
        max_entry_bytes <= config.retained_encoded_bytes,
        "retained byte target must hold the largest representative entry"
    );
    assert!(
        max_entry_bytes
            .checked_add(size_of::<u64>())
            .expect("scan charge fits usize")
            <= config.consumer_page_bytes,
        "consumer byte page must hold the largest complete entry plus its offset"
    );
    let maximum_cycle_bytes = max_entry_bytes
        .checked_mul(config.changes_per_cycle)
        .expect("maximum cycle bytes fit usize");
    assert!(
        maximum_cycle_bytes
            .checked_mul(3)
            .expect("working-set estimate fits usize")
            <= config.max_working_set_bytes,
        "representative producer working set exceeds configured budget"
    );
    let maximum_page_bytes = max_entry_bytes
        .checked_add(size_of::<u64>())
        .and_then(|bytes| bytes.checked_mul(config.consumer_page_items))
        .expect("maximum consumer page bytes fit usize")
        .min(config.consumer_page_bytes);
    assert!(
        maximum_page_bytes
            .checked_mul(3)
            .expect("consumer working-set estimate fits usize")
            <= config.max_working_set_bytes,
        "representative expected, decoded, and scan page exceeds configured budget"
    );
    let maximum_measured = maximum_cycle_bytes
        .checked_mul(config.cycles)
        .expect("maximum measured writes fit usize");
    let maximum_total = config
        .retained_encoded_bytes
        .checked_add(maximum_measured)
        .expect("maximum total writes fit usize");
    assert!(
        maximum_total <= config.max_total_written_bytes,
        "conservative endurance write estimate exceeds configured total-write budget"
    );
    Preflight { max_entry_bytes }
}

pub(super) fn assert_byte_window(retained: usize, target: usize, max_entry: usize) {
    assert!(retained <= target, "retained bytes do not exceed target");
    assert!(
        retained > target.saturating_sub(max_entry),
        "retained byte window stays within one maximum entry of its target"
    );
}

pub(super) fn verify_final_reopen(
    store_path: &Path,
    retained: &VecDeque<EntryOracle>,
    retained_bytes: usize,
    mode: WorkloadMode,
    config: &Config,
    tail: u64,
    max_entry_bytes: usize,
) -> u64 {
    let mut session = Session::open(store_path);
    let head = retained.front().expect("retained final entry").offset;
    assert_eq!(session.bounds(), head..tail);
    assert_eq!(session.read_cursors(), (tail, tail));
    assert_byte_window(
        retained_bytes,
        config.retained_encoded_bytes,
        max_entry_bytes,
    );

    let mut cursor = head;
    let mut retained_index = 0_usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    while cursor < tail {
        let transaction = session
            .transactions
            .begin()
            .expect("begin final reopened scan");
        let mut page = Vec::<(u64, Vec<u8>)>::new();
        let scan = session
            .log
            .access(transaction.access())
            .expect("access final reopened log")
            .scan(
                cursor,
                ScanLimit::new(config.consumer_page_items, config.consumer_page_bytes)
                    .expect("valid final scan limit"),
                |entry| {
                    let raw =
                        entry.project(|encoded| Ok::<_, StoreCodecError>(encoded.to_vec()))?;
                    page.push((entry.offset(), raw));
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan final reopened page");
        transaction.commit().expect("commit final reopened scan");
        assert!(!page.is_empty(), "final reopened scan makes progress");
        for (offset, raw) in page {
            let expected = retained
                .get(retained_index)
                .expect("final scan does not exceed retained oracle");
            assert_eq!(offset, expected.offset);
            let (expected_generated, expected_raw) =
                expected.regenerate_with_encoded(mode.persona());
            assert_eq!(raw, expected_raw, "raw persisted IPC bytes remain exact");
            let decoded = decode_entry(&raw).expect("fully decode final retained Change");
            dogpaddle_change_store_integration::assert_change_eq(
                &decoded,
                &expected_generated.change,
            );
            checksum = fold_validation(checksum, offset);
            checksum = fold_validation(
                checksum,
                u64::try_from(raw.len()).expect("entry length fits u64"),
            );
            for byte in raw {
                checksum = (checksum ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
            retained_index += 1;
        }
        cursor = scan.next_offset;
        if scan.caught_up {
            assert_eq!(cursor, tail);
        }
    }
    assert_eq!(retained_index, retained.len());
    checksum
}

pub(super) fn total_duration(samples: &[Duration]) -> Duration {
    samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |total, sample| {
            total
                .checked_add(sample)
                .expect("duration samples fit Duration")
        })
}

fn fold_validation(mut state: u64, value: u64) -> u64 {
    for byte in value.to_le_bytes() {
        state = (state ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    state
}

impl Session {
    pub(super) fn create(path: &Path) -> Self {
        let mut store = Store::create(path).expect("create endurance Store");
        let log = store
            .create_data::<AppendLog<Vec<u8>>>(LOG_NAME)
            .expect("create endurance log");
        let full_cursor = store
            .create_data::<Cell<u64>>(FULL_CURSOR_NAME)
            .expect("create full consumer cursor");
        let projected_cursor = store
            .create_data::<Cell<u64>>(PROJECTED_CURSOR_NAME)
            .expect("create projected consumer cursor");
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin().expect("begin cursor initialization");
        full_cursor
            .access(transaction.access())
            .expect("access full cursor initialization")
            .set(&0)
            .expect("initialize full cursor");
        projected_cursor
            .access(transaction.access())
            .expect("access projected cursor initialization")
            .set(&0)
            .expect("initialize projected cursor");
        transaction
            .commit()
            .expect("durably initialize consumer cursors");
        Self {
            log,
            full_cursor,
            projected_cursor,
            transactions,
        }
    }

    pub(super) fn open(path: &Path) -> Self {
        let store = Store::open(path).expect("reopen endurance Store");
        let log = store
            .open_data::<AppendLog<Vec<u8>>>(LOG_NAME)
            .expect("reopen endurance log");
        let full_cursor = store
            .open_data::<Cell<u64>>(FULL_CURSOR_NAME)
            .expect("reopen full consumer cursor");
        let projected_cursor = store
            .open_data::<Cell<u64>>(PROJECTED_CURSOR_NAME)
            .expect("reopen projected consumer cursor");
        let transactions = store.into_transactions();
        Self {
            log,
            full_cursor,
            projected_cursor,
            transactions,
        }
    }

    pub(super) fn bounds(&mut self) -> Range<u64> {
        let transaction = self
            .transactions
            .begin()
            .expect("begin endurance bounds read");
        let bounds = self
            .log
            .access(transaction.access())
            .expect("access endurance bounds")
            .bounds()
            .expect("read endurance bounds");
        transaction.commit().expect("commit endurance bounds read");
        bounds
    }

    pub(super) fn read_cursors(&mut self) -> (u64, u64) {
        let transaction = self
            .transactions
            .begin()
            .expect("begin endurance cursors read");
        let full = self
            .full_cursor
            .access(transaction.access())
            .expect("access full endurance cursor")
            .get()
            .expect("read full endurance cursor")
            .expect("full endurance cursor is initialized");
        let projected = self
            .projected_cursor
            .access(transaction.access())
            .expect("access projected endurance cursor")
            .get()
            .expect("read projected endurance cursor")
            .expect("projected endurance cursor is initialized");
        transaction.commit().expect("commit endurance cursors read");
        (full, projected)
    }

    pub(super) fn read_cursor(&mut self, full: bool) -> u64 {
        let transaction = self
            .transactions
            .begin()
            .expect("begin endurance cursor read");
        let cursor = if full {
            &self.full_cursor
        } else {
            &self.projected_cursor
        };
        let value = cursor
            .access(transaction.access())
            .expect("access endurance cursor")
            .get()
            .expect("read endurance cursor")
            .expect("endurance cursor is initialized");
        transaction.commit().expect("commit endurance cursor read");
        value
    }
}
