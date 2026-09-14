use arrow_array::Int64Array;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use dogpaddle_change::{Change, decode_change_owned};
use dogpaddle_store::{OrderedMap, TransactionAccess};

use super::{
    invalid,
    state::{BufferState, Position},
};
use crate::operation::OperationError;

const MAP_KEY_BYTES: u64 = size_of::<u64>() as u64;

/// One bounded delivery plus the admission obligation for each sliced row.
///
/// A negative diff can span delivery batches. Its first visible slice retains
/// the whole still-unadmitted magnitude, while later slices only need to prove
/// the events in that slice. Target planners use this metadata to reject an
/// invalid negative prefix before applying any part of it.
#[derive(Clone)]
pub(crate) struct DeliveryBatch {
    change: Change,
    admissions: Vec<u64>,
}

impl DeliveryBatch {
    fn new(change: Change, admissions: Vec<u64>) -> Result<Self, OperationError> {
        if change.num_rows() != admissions.len()
            || change
                .diffs()
                .values()
                .iter()
                .zip(&admissions)
                .any(|(diff, admission)| *admission < diff.unsigned_abs())
        {
            return Err(invalid("delivery admissions do not match the Change"));
        }
        Ok(Self { change, admissions })
    }

    pub(crate) const fn change(&self) -> &Change {
        &self.change
    }

    pub(crate) fn admission(&self, row_index: usize) -> u64 {
        self.admissions[row_index]
    }

    #[cfg(test)]
    pub(crate) fn for_test(change: Change, admissions: Vec<u64>) -> Result<Self, OperationError> {
        Self::new(change, admissions)
    }
}

#[derive(Clone)]
pub(super) struct LoadedBatch {
    pub(super) delivery: DeliveryBatch,
    pub(super) after: BufferState,
}

pub(super) struct EntryCache {
    pub(super) sequence: u64,
    pub(super) item_bytes: u64,
    pub(super) change: Change,
    event_bytes: Option<(usize, u64)>,
}

pub(super) fn encoded_item_bytes(encoded: &[u8]) -> Result<u64, OperationError> {
    u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(MAP_KEY_BYTES))
        .ok_or_else(|| invalid("encoded Change size exceeds u64"))
}

pub(super) fn event_count(change: &Change) -> Result<u64, OperationError> {
    change
        .diffs()
        .values()
        .iter()
        .try_fold(0_u64, |total, diff| {
            total
                .checked_add(diff.unsigned_abs())
                .ok_or_else(|| invalid("Change event count exceeds u64"))
        })
}

pub(super) fn positive_event_count(change: &Change) -> Result<u64, OperationError> {
    change
        .diffs()
        .values()
        .iter()
        .filter(|diff| **diff > 0)
        .try_fold(0_u64, |total, diff| {
            total
                .checked_add(diff.unsigned_abs())
                .ok_or_else(|| invalid("Change positive-event count exceeds u64"))
        })
}

pub(super) fn remaining_event_counts(
    change: &Change,
    position: Position,
) -> Result<(u64, u64), OperationError> {
    let row_index = usize::try_from(position.row_index)
        .map_err(|_| invalid("buffer row index exceeds usize"))?;
    let first_diff = *change
        .diffs()
        .values()
        .get(row_index)
        .ok_or_else(|| invalid("buffer row index exceeds its Change"))?;
    let original = first_diff.unsigned_abs();
    let current = if position.remaining == 0 {
        if row_index != 0 {
            return Err(invalid(
                "an entry-boundary position has a nonzero row index",
            ));
        }
        original
    } else if position.remaining <= original {
        position.remaining
    } else {
        return Err(invalid("buffer position does not match its Change"));
    };
    let mut total = current;
    let mut positive = if first_diff > 0 { current } else { 0 };
    for diff in &change.diffs().values()[row_index + 1..] {
        total = total
            .checked_add(diff.unsigned_abs())
            .ok_or_else(|| invalid("buffer remaining-event count exceeds u64"))?;
        if *diff > 0 {
            positive = positive
                .checked_add(diff.unsigned_abs())
                .ok_or_else(|| invalid("buffer positive-event count exceeds u64"))?;
        }
    }
    Ok((total, positive))
}

pub(super) fn decode_entry(
    sequence: u64,
    encoded: Vec<u8>,
    max_bytes: u64,
    schema: &SchemaRef,
) -> Result<Change, OperationError> {
    if encoded_item_bytes(&encoded)? > max_bytes {
        return Err(invalid(format!(
            "buffer entry {sequence} exceeds the delivery byte limit"
        )));
    }
    let change = decode_change_owned(encoded)?;
    if change.records().schema() != *schema {
        return Err(invalid(
            "buffered Change Schema differs from the bound Schema",
        ));
    }
    Ok(change)
}

pub(super) fn load(
    buffer: &OrderedMap<u64, Vec<u8>>,
    before: BufferState,
    max_events: u64,
    max_encoded_bytes: u64,
    max_item_bytes: u64,
    max_target_bytes: u64,
    cache: &mut Option<EntryCache>,
    schema: &SchemaRef,
    access: TransactionAccess<'_>,
    mut size_event: impl FnMut(&Change, usize) -> Result<u64, OperationError>,
) -> Result<LoadedBatch, OperationError> {
    before.validate()?;
    let start = before
        .head
        .ok_or_else(|| invalid("cannot load a batch from an empty buffer"))?;
    if max_events == 0 {
        return Err(invalid("batch event limit must be nonzero"));
    }
    if max_encoded_bytes == 0 {
        return Err(invalid("batch encoded-byte limit must be nonzero"));
    }
    if max_item_bytes == 0 {
        return Err(invalid("buffer item byte limit must be nonzero"));
    }
    if max_target_bytes == 0 {
        return Err(invalid("target batch byte limit must be nonzero"));
    }

    let map = buffer.access(access)?;
    let mut sequence = start.sequence;
    let mut row_index =
        usize::try_from(start.row_index).map_err(|_| invalid("buffer row index exceeds usize"))?;
    let mut remaining = start.remaining;
    let mut event_budget = max_events;
    let mut target_byte_budget = max_target_bytes;
    let mut encoded_bytes = 0_u64;
    let mut delivered_events = 0_u64;
    let mut retained_bytes = before.retained_bytes;
    let mut record_batches = Vec::new();
    let mut diffs = Vec::new();
    let mut admissions = Vec::new();

    let after_head = loop {
        if sequence >= before.tail {
            return Err(invalid("buffer head reaches or exceeds its tail"));
        }
        let (item_bytes, change) = match cache.as_ref() {
            Some(cached) if cached.sequence == sequence => {
                (cached.item_bytes, cached.change.clone())
            }
            Some(_) | None => {
                let encoded = map
                    .get(&sequence)?
                    .ok_or_else(|| invalid(format!("buffer entry {sequence} is missing")))?;
                let item_bytes = encoded_item_bytes(&encoded)?;
                let change = decode_entry(sequence, encoded, max_item_bytes, schema)?;
                *cache = Some(EntryCache {
                    sequence,
                    item_bytes,
                    event_bytes: None,
                    change: change.clone(),
                });
                (item_bytes, change)
            }
        };
        if item_bytes > max_encoded_bytes {
            return Err(invalid(format!(
                "buffer entry {sequence} exceeds the delivery byte limit"
            )));
        }
        let next_encoded_bytes = encoded_bytes
            .checked_add(item_bytes)
            .ok_or_else(|| invalid("delivery encoded-byte count exceeds u64"))?;
        if encoded_bytes != 0 && next_encoded_bytes > max_encoded_bytes {
            break Some(Position::entry_start(sequence));
        }
        encoded_bytes = next_encoded_bytes;

        if remaining == 0 {
            if row_index != 0 {
                return Err(invalid(
                    "an entry-boundary position has a nonzero row index",
                ));
            }
            remaining = change.diffs().value(0).unsigned_abs();
        }
        if row_index >= change.num_rows()
            || remaining > change.diffs().value(row_index).unsigned_abs()
        {
            return Err(invalid("buffer position does not match its Change"));
        }

        let first_row = row_index;
        let mut local_diffs = Vec::new();
        let mut stop = None;
        loop {
            let original = change.diffs().value(row_index);
            let first_visible_slice = remaining == original.unsigned_abs();
            let bytes_per_event = {
                let cached = cache
                    .as_mut()
                    .expect("the current entry was cached before slicing");
                match cached.event_bytes {
                    Some((cached_row, bytes)) if cached_row == row_index => bytes,
                    Some(_) | None => {
                        let bytes = size_event(&change, row_index)?;
                        if bytes == 0 {
                            return Err(invalid("target event byte charge must be nonzero"));
                        }
                        cached.event_bytes = Some((row_index, bytes));
                        bytes
                    }
                }
            };
            let target_events = target_byte_budget / bytes_per_event;
            if target_events == 0 {
                if delivered_events == 0 {
                    return Err(invalid(
                        "one target mutation exceeds the target batch byte limit",
                    ));
                }
                stop = Some(Some(
                    if row_index == 0 && remaining == original.unsigned_abs() {
                        Position::entry_start(sequence)
                    } else {
                        Position {
                            sequence,
                            row_index: u64::try_from(row_index)
                                .expect("an addressable row fits u64"),
                            remaining,
                        }
                    },
                ));
                break;
            }
            let take = remaining.min(event_budget).min(target_events);
            local_diffs.push(signed_count(original, take)?);
            admissions.push(if first_visible_slice { remaining } else { take });
            delivered_events = delivered_events
                .checked_add(take)
                .ok_or_else(|| invalid("delivered event count exceeds u64"))?;
            event_budget -= take;
            target_byte_budget -= take
                .checked_mul(bytes_per_event)
                .ok_or_else(|| invalid("target delivery byte count exceeds u64"))?;
            remaining -= take;

            if remaining != 0 {
                stop = Some(Some(Position {
                    sequence,
                    row_index: u64::try_from(row_index).expect("an addressable row fits u64"),
                    remaining,
                }));
                break;
            }

            row_index += 1;
            if row_index == change.num_rows() {
                retained_bytes = retained_bytes
                    .checked_sub(item_bytes)
                    .ok_or_else(|| invalid("buffer retained-byte count underflow"))?;
                sequence += 1;
                if sequence == before.tail {
                    stop = Some(None);
                } else if event_budget == 0 {
                    stop = Some(Some(Position::entry_start(sequence)));
                } else {
                    row_index = 0;
                    remaining = 0;
                }
                break;
            }

            remaining = change.diffs().value(row_index).unsigned_abs();
            if event_budget == 0 {
                stop = Some(Some(Position {
                    sequence,
                    row_index: u64::try_from(row_index).expect("an addressable row fits u64"),
                    remaining,
                }));
                break;
            }
        }

        if !local_diffs.is_empty() {
            record_batches.push(change.records().slice(first_row, local_diffs.len()));
            diffs.extend(local_diffs);
        }
        if let Some(after_head) = stop {
            break after_head;
        }
    };

    let records = if record_batches.len() == 1 {
        record_batches.pop().expect("one record batch exists")
    } else {
        concat_batches(schema, &record_batches)?
    };
    let delivery = DeliveryBatch::new(
        Change::try_new(records, Int64Array::from(diffs))?,
        admissions,
    )?;
    let pending_events = before
        .pending_events
        .checked_sub(delivered_events)
        .ok_or_else(|| invalid("buffer pending-event count underflow"))?;
    let after = match after_head {
        None => BufferState::EMPTY,
        Some(head) => BufferState {
            head: Some(head),
            tail: before.tail,
            pending_events,
            retained_bytes,
        },
    };
    after.validate()?;
    Ok(LoadedBatch { delivery, after })
}

#[cfg(test)]
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
