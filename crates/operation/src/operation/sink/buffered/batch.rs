use arrow_array::Int64Array;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use dogpaddle_change::{Change, decode_change_owned};
use dogpaddle_store::{OrderedMap, OrderedMapAccess, TransactionAccess};

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

#[derive(Clone, Copy)]
pub(super) struct LoadLimits {
    events: u64,
    encoded_bytes: u64,
    item_bytes: u64,
    target_bytes: u64,
}

impl LoadLimits {
    pub(super) const fn new(
        max_events: u64,
        max_encoded_bytes: u64,
        max_item_bytes: u64,
        max_target_bytes: u64,
    ) -> Self {
        Self {
            events: max_events,
            encoded_bytes: max_encoded_bytes,
            item_bytes: max_item_bytes,
            target_bytes: max_target_bytes,
        }
    }

    fn validate(self) -> Result<(), OperationError> {
        if self.events == 0 {
            return Err(invalid("batch event limit must be nonzero"));
        }
        if self.encoded_bytes == 0 {
            return Err(invalid("batch encoded-byte limit must be nonzero"));
        }
        if self.item_bytes == 0 {
            return Err(invalid("buffer item byte limit must be nonzero"));
        }
        if self.target_bytes == 0 {
            return Err(invalid("target batch byte limit must be nonzero"));
        }
        Ok(())
    }
}

enum LoadProgress {
    Continue,
    Stop(Option<Position>),
}

struct Loader<'resources, 'transaction, SizeEvent> {
    map: OrderedMapAccess<'transaction, u64, Vec<u8>>,
    before: BufferState,
    limits: LoadLimits,
    cache: &'resources mut Option<EntryCache>,
    schema: &'resources SchemaRef,
    size_event: SizeEvent,
    sequence: u64,
    row_index: usize,
    remaining: u64,
    event_budget: u64,
    target_byte_budget: u64,
    encoded_bytes: u64,
    delivered_events: u64,
    retained_bytes: u64,
    record_batches: Vec<arrow_array::RecordBatch>,
    diffs: Vec<i64>,
    admissions: Vec<u64>,
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
    limits: LoadLimits,
    cache: &mut Option<EntryCache>,
    schema: &SchemaRef,
    access: TransactionAccess<'_>,
    size_event: impl FnMut(&Change, usize) -> Result<u64, OperationError>,
) -> Result<LoadedBatch, OperationError> {
    before.validate()?;
    limits.validate()?;
    let start = before
        .head
        .ok_or_else(|| invalid("cannot load a batch from an empty buffer"))?;
    let row_index =
        usize::try_from(start.row_index).map_err(|_| invalid("buffer row index exceeds usize"))?;
    Loader {
        map: buffer.access(access)?,
        before,
        limits,
        cache,
        schema,
        size_event,
        sequence: start.sequence,
        row_index,
        remaining: start.remaining,
        event_budget: limits.events,
        target_byte_budget: limits.target_bytes,
        encoded_bytes: 0,
        delivered_events: 0,
        retained_bytes: before.retained_bytes,
        record_batches: Vec::new(),
        diffs: Vec::new(),
        admissions: Vec::new(),
    }
    .run()
}

impl<SizeEvent> Loader<'_, '_, SizeEvent>
where
    SizeEvent: FnMut(&Change, usize) -> Result<u64, OperationError>,
{
    fn run(mut self) -> Result<LoadedBatch, OperationError> {
        let after_head = loop {
            if self.sequence >= self.before.tail {
                return Err(invalid("buffer head reaches or exceeds its tail"));
            }
            let (item_bytes, change) = self.load_entry()?;
            if let LoadProgress::Stop(after_head) = self.consume_entry(item_bytes, &change)? {
                break after_head;
            }
        };
        self.finish(after_head)
    }

    fn load_entry(&mut self) -> Result<(u64, Change), OperationError> {
        if let Some(cached) = self
            .cache
            .as_ref()
            .filter(|cached| cached.sequence == self.sequence)
        {
            return Ok((cached.item_bytes, cached.change.clone()));
        }
        let encoded = self
            .map
            .get(&self.sequence)?
            .ok_or_else(|| invalid(format!("buffer entry {} is missing", self.sequence)))?;
        let item_bytes = encoded_item_bytes(&encoded)?;
        let change = decode_entry(self.sequence, encoded, self.limits.item_bytes, self.schema)?;
        *self.cache = Some(EntryCache {
            sequence: self.sequence,
            item_bytes,
            event_bytes: None,
            change: change.clone(),
        });
        Ok((item_bytes, change))
    }

    fn consume_entry(
        &mut self,
        item_bytes: u64,
        change: &Change,
    ) -> Result<LoadProgress, OperationError> {
        if item_bytes > self.limits.encoded_bytes {
            return Err(invalid(format!(
                "buffer entry {} exceeds the delivery byte limit",
                self.sequence
            )));
        }
        let next_encoded_bytes = self
            .encoded_bytes
            .checked_add(item_bytes)
            .ok_or_else(|| invalid("delivery encoded-byte count exceeds u64"))?;
        if self.encoded_bytes != 0 && next_encoded_bytes > self.limits.encoded_bytes {
            return Ok(LoadProgress::Stop(Some(Position::entry_start(
                self.sequence,
            ))));
        }
        self.encoded_bytes = next_encoded_bytes;
        self.validate_position(change)?;

        let first_row = self.row_index;
        let (local_diffs, progress) = self.consume_rows(item_bytes, change)?;
        if !local_diffs.is_empty() {
            self.record_batches
                .push(change.records().slice(first_row, local_diffs.len()));
            self.diffs.extend(local_diffs);
        }
        Ok(progress)
    }

    fn validate_position(&mut self, change: &Change) -> Result<(), OperationError> {
        if self.remaining == 0 {
            if self.row_index != 0 {
                return Err(invalid(
                    "an entry-boundary position has a nonzero row index",
                ));
            }
            self.remaining = change.diffs().value(0).unsigned_abs();
        }
        if self.row_index >= change.num_rows()
            || self.remaining > change.diffs().value(self.row_index).unsigned_abs()
        {
            return Err(invalid("buffer position does not match its Change"));
        }
        Ok(())
    }

    fn consume_rows(
        &mut self,
        item_bytes: u64,
        change: &Change,
    ) -> Result<(Vec<i64>, LoadProgress), OperationError> {
        let mut local_diffs = Vec::new();
        loop {
            let original = change.diffs().value(self.row_index);
            let first_visible_slice = self.remaining == original.unsigned_abs();
            let bytes_per_event = self.event_bytes(change)?;
            let target_events = self.target_byte_budget / bytes_per_event;
            if target_events == 0 {
                if self.delivered_events == 0 {
                    return Err(invalid(
                        "one target mutation exceeds the target batch byte limit",
                    ));
                }
                return Ok((
                    local_diffs,
                    LoadProgress::Stop(Some(self.current_position(original))),
                ));
            }

            let take = self.remaining.min(self.event_budget).min(target_events);
            local_diffs.push(signed_count(original, take)?);
            self.admissions.push(if first_visible_slice {
                self.remaining
            } else {
                take
            });
            self.charge(take, bytes_per_event)?;

            if self.remaining != 0 {
                return Ok((
                    local_diffs,
                    LoadProgress::Stop(Some(self.current_position(original))),
                ));
            }
            if let Some(progress) = self.advance_row(item_bytes, change)? {
                return Ok((local_diffs, progress));
            }
        }
    }

    fn event_bytes(&mut self, change: &Change) -> Result<u64, OperationError> {
        let cached = self
            .cache
            .as_mut()
            .expect("the current entry was cached before slicing");
        if let Some((cached_row, bytes)) = cached.event_bytes
            && cached_row == self.row_index
        {
            return Ok(bytes);
        }
        let bytes = (self.size_event)(change, self.row_index)?;
        if bytes == 0 {
            return Err(invalid("target event byte charge must be nonzero"));
        }
        cached.event_bytes = Some((self.row_index, bytes));
        Ok(bytes)
    }

    fn charge(&mut self, take: u64, bytes_per_event: u64) -> Result<(), OperationError> {
        self.delivered_events = self
            .delivered_events
            .checked_add(take)
            .ok_or_else(|| invalid("delivered event count exceeds u64"))?;
        self.event_budget -= take;
        self.target_byte_budget -= take
            .checked_mul(bytes_per_event)
            .ok_or_else(|| invalid("target delivery byte count exceeds u64"))?;
        self.remaining -= take;
        Ok(())
    }

    fn advance_row(
        &mut self,
        item_bytes: u64,
        change: &Change,
    ) -> Result<Option<LoadProgress>, OperationError> {
        self.row_index += 1;
        if self.row_index == change.num_rows() {
            self.retained_bytes = self
                .retained_bytes
                .checked_sub(item_bytes)
                .ok_or_else(|| invalid("buffer retained-byte count underflow"))?;
            self.sequence += 1;
            if self.sequence == self.before.tail {
                return Ok(Some(LoadProgress::Stop(None)));
            }
            if self.event_budget == 0 {
                return Ok(Some(LoadProgress::Stop(Some(Position::entry_start(
                    self.sequence,
                )))));
            }
            self.row_index = 0;
            self.remaining = 0;
            return Ok(Some(LoadProgress::Continue));
        }

        self.remaining = change.diffs().value(self.row_index).unsigned_abs();
        if self.event_budget == 0 {
            Ok(Some(LoadProgress::Stop(Some(
                self.current_position(change.diffs().value(self.row_index)),
            ))))
        } else {
            Ok(None)
        }
    }

    fn current_position(&self, original: i64) -> Position {
        if self.row_index == 0 && self.remaining == original.unsigned_abs() {
            Position::entry_start(self.sequence)
        } else {
            Position {
                sequence: self.sequence,
                row_index: u64::try_from(self.row_index).expect("an addressable row fits u64"),
                remaining: self.remaining,
            }
        }
    }

    fn finish(mut self, after_head: Option<Position>) -> Result<LoadedBatch, OperationError> {
        let records = if self.record_batches.len() == 1 {
            self.record_batches.pop().expect("one record batch exists")
        } else {
            concat_batches(self.schema, &self.record_batches)?
        };
        let delivery = DeliveryBatch::new(
            Change::try_new(records, Int64Array::from(self.diffs))?,
            self.admissions,
        )?;
        let pending_events = self
            .before
            .pending_events
            .checked_sub(self.delivered_events)
            .ok_or_else(|| invalid("buffer pending-event count underflow"))?;
        let after = after_head.map_or(BufferState::EMPTY, |head| BufferState {
            head: Some(head),
            tail: self.before.tail,
            pending_events,
            retained_bytes: self.retained_bytes,
        });
        after.validate()?;
        Ok(LoadedBatch { delivery, after })
    }
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
