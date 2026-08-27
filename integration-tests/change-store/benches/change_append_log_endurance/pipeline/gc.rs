use std::{collections::VecDeque, time::Duration};

use super::{
    super::{config::Config, workload::EntryOracle},
    lifecycle::{Session, assert_byte_window},
};

pub(super) struct GcOutcome {
    pub(super) target: u64,
    pub(super) removed_entries: usize,
    pub(super) removed_bytes: usize,
    pub(super) durable_full_cursor: u64,
    pub(super) durable_projected_cursor: u64,
    pub(super) duration: Duration,
}

pub(super) fn truncate_to_window(
    session: &mut Session,
    retained: &mut VecDeque<EntryOracle>,
    retained_bytes: &mut usize,
    head_before: u64,
    tail: u64,
    max_entry_bytes: usize,
    config: &Config,
) -> GcOutcome {
    let mut removed_entries = 0_usize;
    let mut removed_bytes = 0_usize;
    while *retained_bytes > config.retained_encoded_bytes {
        let removed = retained.pop_front().expect("non-empty retained queue");
        *retained_bytes -= removed.encoded_len;
        removed_entries = removed_entries
            .checked_add(1)
            .expect("removed entry count fits usize");
        removed_bytes = removed_bytes
            .checked_add(removed.encoded_len)
            .expect("removed encoded bytes fit usize");
    }
    assert_byte_window(
        *retained_bytes,
        config.retained_encoded_bytes,
        max_entry_bytes,
    );
    let target = retained.front().expect("retained byte window").offset;
    let (durable_full_cursor, durable_projected_cursor) = session.read_cursors();
    assert!(
        target <= durable_full_cursor.min(durable_projected_cursor),
        "GC never passes either durably committed consumer cursor"
    );

    let started = std::time::Instant::now();
    let transaction = session
        .transactions
        .begin()
        .expect("begin endurance truncate transaction");
    let mut access = session
        .log
        .access(transaction.access())
        .expect("access endurance truncate log");
    let mut head = head_before;
    while head < target {
        head = access
            .truncate_before(target, config.truncate_items)
            .expect("truncate committed endurance prefix");
    }
    transaction
        .commit()
        .expect("durably commit endurance truncation");
    let duration = started.elapsed();
    assert_eq!(head, target);
    assert_eq!(session.bounds(), target..tail);

    GcOutcome {
        target,
        removed_entries,
        removed_bytes,
        durable_full_cursor,
        durable_projected_cursor,
        duration,
    }
}
