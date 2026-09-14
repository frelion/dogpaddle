use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use dogpaddle_change::{Change, decode_change_owned};
use dogpaddle_store::{OrderedMap, TransactionAccess};

use super::{invalid, state::{BufferState, Position}};
use crate::operation::OperationError;

const MAP_KEY_BYTES: u64 = size_of::<u64>() as u64;

pub(super) struct LoadedBatch {
    pub(super) change: Change,
    pub(super) after: BufferState,
}

pub(super) fn encoded_item_bytes(encoded: &[u8]) -> Result<u64, OperationError> {
    u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(MAP_KEY_BYTES))
        .ok_or_else(|| invalid("encoded Change size exceeds u64"))
}

pub(super) fn event_count(change: &Change) -> Result<u64, OperationError> {
    change.diffs().values().iter().try_fold(0_u64, |total, diff| {
        total
            .checked_add(diff.unsigned_abs())
            .ok_or_else(|| invalid("Change event count exceeds u64"))
    })
}

pub(super) fn load(
    buffer: &OrderedMap<u64, Vec<u8>>,
    before: BufferState,
    max_events: u64,
    schema: &SchemaRef,
    access: TransactionAccess<'_>,
) -> Result<LoadedBatch, OperationError> {
    let start = before
        .head
        .ok_or_else(|| invalid("cannot load a batch from an empty buffer"))?;
    if max_events == 0 {
        return Err(invalid("batch event limit must be nonzero"));
    }

    let map = buffer.access(access)?;
    let mut sequence = start.sequence;
    let mut row_index = usize::try_from(start.row_index)
        .map_err(|_| invalid("buffer row index exceeds usize"))?;
    let mut remaining = start.remaining;
    let mut budget = max_events;
    let mut delivered = 0_u64;
    let mut retained_bytes = before.retained_bytes;
    let mut batches = Vec::new();
    let mut diffs = Vec::new();
    let after_head;

    loop {
        if sequence >= before.tail {
            return Err(invalid("buffer head reaches or exceeds its tail"));
        }
        let encoded = map
            .get(&sequence)?
            .ok_or_else(|| invalid(format!("buffer entry {sequence} is missing")))?;
        let item_bytes = encoded_item_bytes(&encoded)?;
        let change = decode_change_owned(encoded)?;
        if change.records().schema() != *schema {
            return Err(invalid("buffered Change Schema differs from the bound Schema"));
        }
        if row_index >= change.num_rows()
            || remaining == 0
            || remaining > change.diffs().value(row_index).unsigned_abs()
        {
            return Err(invalid("buffer position does not match its Change"));
        }

        let first_row = row_index;
        let mut local_diffs = Vec::new();
        loop {
            let original = change.diffs().value(row_index);
            let take = remaining.min(budget);
            local_diffs.push(signed_count(original, take)?);
            delivered = delivered
                .checked_add(take)
                .ok_or_else(|| invalid("delivered event count exceeds u64"))?;
            budget -= take;
            remaining -= take;

            if remaining != 0 {
                after_head = Some(Position {
                    sequence,
                    row_index: u64::try_from(row_index).expect("an addressable row fits u64"),
                    remaining,
                });
                break;
            }

            row_index += 1;
            if row_index == change.num_rows() {
                retained_bytes = retained_bytes
                    .checked_sub(item_bytes)
                    .ok_or_else(|| invalid("buffer retained-byte count underflow"))?;
                sequence += 1;
                if sequence == before.tail {
                    after_head = None;
                    break;
                }
                if budget == 0 {
                    let next = map
                        .get(&sequence)?
                        .ok_or_else(|| invalid(format!("buffer entry {sequence} is missing")))?;
                    let next = decode_change_owned(next)?;
                    if next.records().schema() != *schema {
                        return Err(invalid(
                            "buffered Change Schema differs from the bound Schema",
                        ));
                    }
                    after_head = Some(first_position(sequence, &next));
                    break;
                }
                row_index = 0;
                remaining = 0;
                break;
            }
            remaining = change.diffs().value(row_index).unsigned_abs();
            if budget == 0 {
                after_head = Some(Position {
                    sequence,
                    row_index: u64::try_from(row_index).expect("an addressable row fits u64"),
                    remaining,
                });
                break;
            }
        }

        let row_count = local_diffs.len();
        batches.push(change.records().slice(first_row, row_count));
        diffs.extend(local_diffs);

        if budget == 0 || after_head.is_none() || remaining != 0 || row_index != 0 {
            break;
        }
        let next = map
            .get(&sequence)?
            .ok_or_else(|| invalid(format!("buffer entry {sequence} is missing")))?;
        let next = decode_change_owned(next)?;
        if next.records().schema() != *schema {
            return Err(invalid("buffered Change Schema differs from the bound Schema"));
        }
        row_index = 0;
        remaining = next.diffs().value(0).unsigned_abs();
        // Read it again in the next iteration. The extra point read keeps the
        // state transition small and validates the persisted next position.
    }

    let records = if batches.len() == 1 {
        batches.pop().expect("one batch exists")
    } else {
        concat_batches(schema, &batches)?
    };
    let change = Change::try_new(records, Int64Array::from(diffs))?;
    let pending_events = before
        .pending_events
        .checked_sub(delivered)
        .ok_or_else(|| invalid("buffer pending-event count underflow"))?;
    let after = BufferState {
        head: after_head,
        tail: before.tail,
        pending_events,
        retained_bytes,
    };
    after.validate()?;
    Ok(LoadedBatch { change, after })
}

pub(super) fn first_position(sequence: u64, change: &Change) -> Position {
    Position {
        sequence,
        row_index: 0,
        remaining: change.diffs().value(0).unsigned_abs(),
    }
}

fn signed_count(original: i64, count: u64) -> Result<i64, OperationError> {
    if original > 0 {
        i64::try_from(count).map_err(|_| invalid("positive event count exceeds i64"))
    } else if count == i64::MIN.unsigned_abs() {
        Ok(i64::MIN)
    } else {
        i64::try_from(count)
            .map(|count| -count)
            .map_err(|_| invalid("negative event count exceeds i64"))
    }
}

pub(super) fn concatenate(changes: &[Change], schema: &SchemaRef) -> Result<Change, OperationError> {
    let records = changes
        .iter()
        .map(|change| change.records().clone())
        .collect::<Vec<RecordBatch>>();
    let records = concat_batches(schema, &records)?;
    let diffs = changes
        .iter()
        .flat_map(|change| change.diffs().values().iter().copied())
        .collect::<Vec<_>>();
    Change::try_new(records, Int64Array::from(diffs)).map_err(Into::into)
}

#[allow(dead_code)]
fn _schema_clone(schema: &SchemaRef) -> SchemaRef {
    Arc::clone(schema)
}
