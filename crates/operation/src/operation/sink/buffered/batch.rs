use arrow_array::Int64Array;
use arrow_select::concat::concat_batches;
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_store::{OrderedMap, OrderedMapReadAccess, ReadTransactionAccess, StoreError};

use super::{
    invalid,
    state::{BufferState, Position},
};
use crate::operation::OperationError;

const MAP_KEY_BYTES: u64 = size_of::<u64>() as u64;

/// One bounded contiguous absolute-event interval with weighted rows.
#[derive(Clone)]
pub(crate) struct DeliveryBatch {
    change: Change,
    first_event_offset: u64,
}

impl DeliveryBatch {
    fn new(change: Change, first_event_offset: u64) -> Result<Self, OperationError> {
        let events = event_count(&change)?;
        if first_event_offset == 0
            || events == 0
            || first_event_offset.checked_add(events).is_none()
        {
            return Err(invalid("delivery interval exceeds the event domain"));
        }
        Ok(Self {
            change,
            first_event_offset,
        })
    }

    pub(crate) const fn change(&self) -> &Change {
        &self.change
    }
    pub(crate) const fn first_event_offset(&self) -> u64 {
        self.first_event_offset
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        change: Change,
        first_event_offset: u64,
    ) -> Result<Self, OperationError> {
        Self::new(change, first_event_offset)
    }
}

#[derive(Clone)]
pub(super) struct LoadedBatch {
    pub(super) delivery: DeliveryBatch,
    pub(super) after: BufferState,
    // Preserve the original head across cache replacement when delivery crosses entries.
    // Its bytes are included in the delivery's encoded-entry budget.
    pub(super) first_entry: Change,
    pub(super) consumed_keys: Vec<u64>,
}

#[derive(Clone, Copy)]
struct PositionHint {
    event_offset: u64,
    row_index: usize,
    remaining: u64,
}

pub(super) struct EntryCache {
    pub(super) entry_start: u64,
    pub(super) item_bytes: u64,
    pub(super) change: Change,
    entry_end: u64,
    position_hint: Option<PositionHint>,
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
    Stop(Position),
}

struct Loader<'resources, 'transaction, SizeEvent> {
    map: OrderedMapReadAccess<'transaction, u64, Vec<u8>>,
    before: BufferState,
    limits: LoadLimits,
    cache: &'resources mut Option<EntryCache>,
    codec: &'resources SchemaBoundChangeCodec,
    size_event: SizeEvent,
    entry_start: u64,
    row_index: usize,
    remaining: u64,
    event_offset: u64,
    entry_end: u64,
    first_entry: Option<Change>,
    consumed_keys: Vec<u64>,
    event_budget: u64,
    target_byte_budget: u64,
    encoded_bytes: u64,
    delivered_events: u64,
    retained_bytes: u64,
    record_batches: Vec<arrow_array::RecordBatch>,
    diffs: Vec<i64>,
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

pub(super) fn entry_end(change: &Change, entry_start: u64) -> Result<u64, OperationError> {
    let events = event_count(change)?;
    if events == 0 {
        return Err(invalid("buffer entry contains no events"));
    }
    entry_start
        .checked_add(events)
        .ok_or_else(|| invalid("buffer event offset is exhausted"))
}

pub(super) fn decode_entry(
    entry_start: u64,
    encoded: Vec<u8>,
    max_bytes: u64,
    codec: &SchemaBoundChangeCodec,
) -> Result<Change, OperationError> {
    if encoded_item_bytes(&encoded)? > max_bytes {
        return Err(invalid(format!(
            "buffer entry {entry_start} exceeds the delivery byte limit"
        )));
    }
    Ok(codec.decode_owned(encoded)?)
}

pub(super) fn load(
    buffer: &OrderedMap<u64, Vec<u8>>,
    before: BufferState,
    limits: LoadLimits,
    cache: &mut Option<EntryCache>,
    codec: &SchemaBoundChangeCodec,
    access: ReadTransactionAccess<'_>,
    size_event: impl FnMut(&Change, usize) -> Result<u64, OperationError>,
) -> Result<LoadedBatch, OperationError> {
    before.validate()?;
    limits.validate()?;
    if before.is_empty() {
        return Err(invalid("cannot load a batch from an empty buffer"));
    }
    let start = before.head;
    Loader {
        map: buffer.read(access)?,
        before,
        limits,
        cache,
        codec,
        size_event,
        entry_start: start.entry_start,
        row_index: 0,
        remaining: 0,
        event_offset: start.event_offset,
        entry_end: 0,
        first_entry: None,
        consumed_keys: Vec::new(),
        event_budget: limits.events,
        target_byte_budget: limits.target_bytes,
        encoded_bytes: 0,
        delivered_events: 0,
        retained_bytes: before.retained_bytes,
        record_batches: Vec::new(),
        diffs: Vec::new(),
    }
    .run()
}

impl<SizeEvent> Loader<'_, '_, SizeEvent>
where
    SizeEvent: FnMut(&Change, usize) -> Result<u64, OperationError>,
{
    fn run(mut self) -> Result<LoadedBatch, OperationError> {
        let after_head = loop {
            if self.entry_start >= self.before.tail {
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
            .filter(|cached| cached.entry_start == self.entry_start)
        {
            self.entry_end = cached.entry_end;
            if self.entry_end > self.before.tail {
                return Err(invalid("buffer entry exceeds its tail"));
            }
            return Ok((cached.item_bytes, cached.change.clone()));
        }
        let encoded = match self.map.get_bounded(
            &self.entry_start,
            usize::try_from(self.limits.item_bytes).expect("the item byte limit fits usize"),
        ) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => {
                return Err(invalid(format!(
                    "buffer entry {} is missing",
                    self.entry_start
                )));
            }
            Err(StoreError::ItemTooLarge { .. }) => {
                return Err(invalid("buffer entry exceeds the delivery byte limit"));
            }
            Err(source) => return Err(source.into()),
        };
        let item_bytes = encoded_item_bytes(&encoded)?;
        let change = decode_entry(
            self.entry_start,
            encoded,
            self.limits.item_bytes,
            self.codec,
        )?;
        self.entry_end = entry_end(&change, self.entry_start)?;
        if self.entry_end > self.before.tail {
            return Err(invalid("buffer entry exceeds its tail"));
        }
        *self.cache = Some(EntryCache {
            entry_start: self.entry_start,
            entry_end: self.entry_end,
            position_hint: None,
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
                self.entry_start
            )));
        }
        let next_encoded_bytes = self
            .encoded_bytes
            .checked_add(item_bytes)
            .ok_or_else(|| invalid("delivery encoded-byte count exceeds u64"))?;
        if self.encoded_bytes != 0 && next_encoded_bytes > self.limits.encoded_bytes {
            return Ok(LoadProgress::Stop(Position::entry_start(self.entry_start)));
        }
        self.encoded_bytes = next_encoded_bytes;
        self.locate_position(change)?;
        if self.first_entry.is_none() {
            self.first_entry = Some(change.clone());
        }

        let first_row = self.row_index;
        let (local_diffs, progress) = self.consume_rows(item_bytes, change)?;
        if !local_diffs.is_empty() {
            self.record_batches
                .push(change.records().slice(first_row, local_diffs.len()));
            self.diffs.extend(local_diffs);
        }
        Ok(progress)
    }

    fn locate_position(&mut self, change: &Change) -> Result<(), OperationError> {
        if self.event_offset < self.entry_start || self.event_offset >= self.entry_end {
            return Err(invalid("buffer position does not match its Change"));
        }
        let cached = self.cache.as_ref().expect("current entry is cached");
        if let Some(hint) = cached
            .position_hint
            .filter(|hint| hint.event_offset == self.event_offset)
        {
            self.row_index = hint.row_index;
            self.remaining = hint.remaining;
            return Ok(());
        }
        // Store's head is authoritative. A hint from a rolled-back later load is ignored.
        let mut offset = self.entry_start;
        for (row_index, diff) in change.diffs().values().iter().enumerate() {
            let end = offset
                .checked_add(diff.unsigned_abs())
                .ok_or_else(|| invalid("buffer event offset is exhausted"))?;
            if self.event_offset < end {
                self.row_index = row_index;
                self.remaining = end - self.event_offset;
                return Ok(());
            }
            offset = end;
        }
        Err(invalid("buffer position does not match its Change"))
    }

    fn current_position(&mut self) -> Position {
        if let Some(cached) = self
            .cache
            .as_mut()
            .filter(|cached| cached.entry_start == self.entry_start)
        {
            cached.position_hint = Some(PositionHint {
                event_offset: self.event_offset,
                row_index: self.row_index,
                remaining: self.remaining,
            });
        }
        Position {
            entry_start: self.entry_start,
            event_offset: self.event_offset,
        }
    }

    fn consume_rows(
        &mut self,
        item_bytes: u64,
        change: &Change,
    ) -> Result<(Vec<i64>, LoadProgress), OperationError> {
        let mut local_diffs = Vec::new();
        loop {
            let original = change.diffs().value(self.row_index);
            let bytes_per_event = self.event_bytes(change)?;
            let target_events = self.target_byte_budget / bytes_per_event;
            if target_events == 0 {
                if self.delivered_events == 0 {
                    return Err(invalid(
                        "one target mutation exceeds the target batch byte limit",
                    ));
                }
                return Ok((local_diffs, LoadProgress::Stop(self.current_position())));
            }

            let take = self.remaining.min(self.event_budget).min(target_events);
            local_diffs.push(signed_count(original, take)?);
            self.charge(take, bytes_per_event)?;

            if self.remaining != 0 {
                return Ok((local_diffs, LoadProgress::Stop(self.current_position())));
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
        self.event_offset = self
            .event_offset
            .checked_add(take)
            .ok_or_else(|| invalid("delivery event offset is exhausted"))?;
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
            if self.event_offset != self.entry_end {
                return Err(invalid("buffer entry span does not match its rows"));
            }
            self.consumed_keys.push(self.entry_start);
            self.entry_start = self.entry_end;
            if self.entry_start == self.before.tail {
                return Ok(Some(LoadProgress::Stop(Position::entry_start(
                    self.entry_start,
                ))));
            }
            if self.event_budget == 0 {
                return Ok(Some(LoadProgress::Stop(Position::entry_start(
                    self.entry_start,
                ))));
            }
            self.row_index = 0;
            self.remaining = 0;
            return Ok(Some(LoadProgress::Continue));
        }

        self.remaining = change.diffs().value(self.row_index).unsigned_abs();
        if self.event_budget == 0 {
            Ok(Some(LoadProgress::Stop(self.current_position())))
        } else {
            Ok(None)
        }
    }

    fn finish(mut self, after_head: Position) -> Result<LoadedBatch, OperationError> {
        let records = if self.record_batches.len() == 1 {
            self.record_batches.pop().expect("one record batch exists")
        } else {
            concat_batches(&self.codec.schema(), &self.record_batches)?
        };
        let delivery = DeliveryBatch::new(
            Change::try_new(records, Int64Array::from(self.diffs))?,
            self.before.head.event_offset,
        )?;
        let after = BufferState {
            head: after_head,
            tail: self.before.tail,
            retained_bytes: self.retained_bytes,
        };
        after.validate()?;
        Ok(LoadedBatch {
            delivery,
            after,
            first_entry: self
                .first_entry
                .expect("a delivery has an original head entry"),
            consumed_keys: self.consumed_keys,
        })
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
