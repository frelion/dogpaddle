use std::{collections::VecDeque, time::Duration};

use dogpaddle_change::{Change, decode_change_projected};
use dogpaddle_change_store_integration::WorkloadPersona;
use dogpaddle_store::{CodecError as StoreCodecError, ScanLimit, StoreError};

use crate::support::decode_entry;

use super::{
    super::{
        config::Config,
        oracle::{StreamOracle, verify_full_page, verify_projected_page},
        workload::{EntryOracle, ExpectedPageEntry},
    },
    lifecycle::{Session, total_duration},
};

#[derive(Clone, Copy)]
pub(super) enum ConsumerKind {
    Full,
    Projected,
}

pub(super) struct ConsumerRun {
    pub(super) cursor: u64,
    pub(super) pages: usize,
    pub(super) durations: Vec<Duration>,
}

impl ConsumerRun {
    pub(super) fn elapsed(&self) -> Duration {
        total_duration(&self.durations)
    }
}

pub(super) fn consume_to_tail(
    session: &mut Session,
    retained: &VecDeque<EntryOracle>,
    tail: u64,
    persona: WorkloadPersona,
    config: &Config,
    kind: ConsumerKind,
    full_oracle: &mut StreamOracle,
) -> ConsumerRun {
    let mut cursor = session.read_cursor(matches!(kind, ConsumerKind::Full));
    let mut pages = 0_usize;
    let mut durations = Vec::new();
    while cursor < tail {
        let expected = expected_page(retained, cursor, config.consumer_page_items, persona);
        let mut actual = Vec::<(u64, Change)>::with_capacity(expected.len());
        let started = std::time::Instant::now();
        let transaction = session
            .transactions
            .begin()
            .expect("begin endurance consumer page");
        let scan = session
            .log
            .access(transaction.access())
            .expect("access endurance consumer log")
            .scan(
                cursor,
                ScanLimit::new(config.consumer_page_items, config.consumer_page_bytes)
                    .expect("valid endurance consumer page limit"),
                |entry| {
                    let index = actual.len();
                    let decoded = match kind {
                        ConsumerKind::Full => entry.project(decode_entry)?,
                        ConsumerKind::Projected => {
                            let projection = &expected
                                .get(index)
                                .expect("scan respects configured page item limit")
                                .projection;
                            entry.project(|encoded| {
                                decode_change_projected(encoded, projection)
                                    .map_err(|error| StoreCodecError::new(error.to_string()))
                            })?
                        }
                    };
                    actual.push((entry.offset(), decoded));
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan endurance consumer page");
        match kind {
            ConsumerKind::Full => session
                .full_cursor
                .access(transaction.access())
                .expect("access full consumer cursor")
                .set(&scan.next_offset)
                .expect("advance full consumer cursor"),
            ConsumerKind::Projected => session
                .projected_cursor
                .access(transaction.access())
                .expect("access projected consumer cursor")
                .set(&scan.next_offset)
                .expect("advance projected consumer cursor"),
        }
        transaction
            .commit()
            .expect("durably commit endurance consumer cursor");
        let elapsed = started.elapsed();

        assert!(!actual.is_empty(), "consumer page must make progress");
        assert_eq!(
            scan.next_offset,
            cursor
                .checked_add(u64::try_from(actual.len()).expect("page length fits u64"))
                .expect("consumer cursor fits u64")
        );
        let expected = &expected[..actual.len()];
        match kind {
            ConsumerKind::Full => verify_full_page(expected, &actual, full_oracle),
            ConsumerKind::Projected => verify_projected_page(expected, &actual),
        }
        cursor = scan.next_offset;
        pages = pages
            .checked_add(1)
            .expect("consumer page count fits usize");
        durations.push(elapsed);
        if scan.caught_up {
            assert_eq!(cursor, tail);
        }
    }
    ConsumerRun {
        cursor,
        pages,
        durations,
    }
}

fn expected_page(
    retained: &VecDeque<EntryOracle>,
    cursor: u64,
    max_items: usize,
    requested_persona: WorkloadPersona,
) -> Vec<ExpectedPageEntry> {
    let head = retained.front().expect("retained entries").offset;
    let index = usize::try_from(cursor.checked_sub(head).expect("cursor is retained"))
        .expect("retained cursor index fits usize");
    retained
        .iter()
        .skip(index)
        .take(max_items)
        .copied()
        .map(|entry| entry.expected_page_entry(requested_persona))
        .collect()
}
