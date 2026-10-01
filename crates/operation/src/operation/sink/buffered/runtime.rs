use super::{
    MAX_TARGET_BATCH_BYTES,
    batch::{self, LoadedBatch},
    invalid,
    state::{self, BufferState, MAX_CONTROL_BYTES, State},
};
use crate::operation::sink::relation::{self, RelationTarget};
use crate::operation::{OperationError, SinkOperation};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_store::{
    Cell, OrderedMap, ReadTransactionAccess, ScanDirection, ScanLimit, StoreError,
    TransactionAccess,
};
const MAX_BUFFERED_EVENTS: u64 = 1_048_576;
const MAX_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DELIVERY_BYTES: u64 = 8 * 1024 * 1024;
const BUFFER_VALIDATION_ITEMS: usize = 256;
struct Admission {
    encoded_change: Vec<u8>,
    item_bytes: u64,
    events: u64,
}
pub(crate) struct BufferedSink<T: RelationTarget> {
    codec: SchemaBoundChangeCodec,
    target: T,
    control: Cell<Vec<u8>>,
    buffer: OrderedMap<u64, Vec<u8>>,
    head_cache: Option<batch::EntryCache>,
    recovered: bool,
    #[cfg(test)]
    max_batch_events: u64,
}
/// A bounded immutable input prefix or initialization intent.
pub struct SinkPending {
    kind: PendingKind,
}
// One transient prefix stays inline to avoid an allocation per delivery.
#[expect(clippy::large_enum_variant)]
enum PendingKind {
    Initialize {
        fresh: bool,
    },
    Loaded {
        before: BufferState,
        batch: LoadedBatch,
    },
}
impl<T: RelationTarget> BufferedSink<T> {
    pub(crate) const fn new(
        codec: SchemaBoundChangeCodec,
        target: T,
        control: Cell<Vec<u8>>,
        buffer: OrderedMap<u64, Vec<u8>>,
    ) -> Self {
        Self {
            codec,
            target,
            control,
            buffer,
            head_cache: None,
            recovered: false,
            #[cfg(test)]
            max_batch_events: relation::MAX_MUTATIONS_PER_BATCH as u64,
        }
    }
    fn decode_restored(
        &mut self,
        encoded: Option<&[u8]>,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<State>, OperationError> {
        let Some(encoded) = encoded else {
            self.require_empty_buffer(access)?;
            return Ok(None);
        };
        let state = state::decode(encoded)?;
        match state {
            State::Initialize => {
                self.require_empty_buffer(access)?;
            }
            State::Ready(ready) => {
                validate_capacity(ready)?;
                if !self.recovered {
                    self.validate_buffer(ready, access)?;
                }
            }
        }
        Ok(Some(state))
    }

    fn require_empty_buffer(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        self.require_empty_range(.., access)
    }

    fn validate_buffer(
        &self,
        state: BufferState,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        if state.is_empty() {
            self.require_empty_buffer(access)?;
            return Ok(());
        }
        let head = state.head;
        self.require_empty_range(..head.entry_start, access)?;
        self.require_empty_range(state.tail.., access)?;

        let limit = ScanLimit::new(
            BUFFER_VALIDATION_ITEMS,
            usize::try_from(MAX_DELIVERY_BYTES).expect("the byte limit fits usize"),
        )
        .expect("the buffer validation limits are nonzero");
        let map = self.buffer.read(access)?;
        let mut continuation = None;
        let mut expected_start = head.entry_start;
        let mut retained_bytes = 0_u64;
        loop {
            let page = match map.scan(
                head.entry_start..state.tail,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
            ) {
                Ok(page) => page,
                Err(StoreError::ItemTooLarge { .. }) => {
                    return Err(invalid("buffer entry exceeds the delivery byte limit"));
                }
                Err(source) => return Err(source.into()),
            };
            if page.entries.is_empty() {
                break;
            }
            for (entry_start, encoded) in page.entries {
                if entry_start != expected_start {
                    return Err(invalid(format!("buffer entry {expected_start} is missing")));
                }
                let item_bytes = batch::encoded_item_bytes(&encoded)?;
                let change =
                    batch::decode_entry(entry_start, encoded, MAX_DELIVERY_BYTES, &self.codec)?;
                validate_event_sizes(&self.target, &change)?;
                let end = batch::entry_end(&change, entry_start)?;
                if end - entry_start > MAX_BUFFERED_EVENTS {
                    return Err(invalid("buffer entry exceeds the buffered-event capacity"));
                }
                if end > state.tail || (entry_start == head.entry_start && head.event_offset >= end)
                {
                    return Err(invalid(
                        "buffer head or entry span does not match its Change",
                    ));
                }
                retained_bytes = retained_bytes
                    .checked_add(item_bytes)
                    .ok_or_else(|| invalid("buffer retained-byte count exceeds u64"))?;
                if retained_bytes > state.retained_bytes {
                    return Err(invalid(
                        "buffer contents exceed their control-state accounting",
                    ));
                }
                expected_start = end;
            }
            let Some(next) = page.continuation else {
                break;
            };
            continuation = Some(next);
        }
        if expected_start != state.tail {
            return Err(invalid(format!("buffer entry {expected_start} is missing")));
        }
        if retained_bytes != state.retained_bytes {
            return Err(invalid(
                "buffer contents do not match their control-state accounting",
            ));
        }
        Ok(())
    }

    fn require_empty_range(
        &self,
        range: impl std::ops::RangeBounds<u64>,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        let limit = ScanLimit::new(
            1,
            usize::try_from(MAX_DELIVERY_BYTES).expect("the byte limit fits usize"),
        )
        .expect("the buffer scan limits are nonzero");
        match self
            .buffer
            .read(access)?
            .scan(range, ScanDirection::Ascending, None, limit)
        {
            Ok(page) if page.entries.is_empty() => Ok(()),
            Ok(_) | Err(StoreError::ItemTooLarge { .. }) => Err(invalid(
                "buffer contains an entry outside its control-state range",
            )),
            Err(source) => Err(source.into()),
        }
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn delivery_event_limit(&self) -> u64 {
        #[cfg(test)]
        {
            self.max_batch_events
        }
        #[cfg(not(test))]
        {
            relation::MAX_MUTATIONS_PER_BATCH as u64
        }
    }
}
impl<T: RelationTarget> SinkOperation for BufferedSink<T> {
    fn try_enqueue(
        &mut self,
        access: TransactionAccess<'_>,
        page: &Change,
    ) -> Result<bool, OperationError> {
        if self.codec.schema().as_ref() != page.records().schema_ref().as_ref() {
            return Err(invalid("input Schema differs from the bound Schema"));
        }
        let encoded = self
            .control
            .access(access)?
            .get_bounded(MAX_CONTROL_BYTES)?;
        let Some(encoded) = encoded else {
            return Ok(false);
        };
        let State::Ready(ready) = state::decode(&encoded)? else {
            return Ok(false);
        };
        validate_capacity(ready)?;
        let events = batch::event_count(page)?;
        // Permanent event positions cannot be reclaimed by draining the outbox.
        // Exhaustion is terminal and checked before any map or control write.
        let tail = ready
            .tail
            .checked_add(events)
            .ok_or_else(|| invalid("buffer event offset is exhausted"))?;
        let admission = prepare_admission(&self.codec, &self.target, page, events)?;
        if !fits(ready, &admission) {
            return Ok(false);
        }
        let entry_start = ready.tail;
        let next = BufferState {
            head: ready.head,
            tail,
            retained_bytes: ready.retained_bytes + admission.item_bytes,
        };
        next.validate()?;
        let mut map = self.buffer.access(access)?;
        if map.get_bounded(&entry_start, 0)?.is_some() {
            return Err(invalid("outbox entry_start already exists"));
        }
        map.put(&entry_start, &admission.encoded_change)?;
        self.control
            .access(access)?
            .set(&State::Ready(next).encode())?;
        Ok(true)
    }
    fn load(
        &mut self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<SinkPending>, OperationError> {
        let encoded = self.control.read(access)?.get_bounded(MAX_CONTROL_BYTES)?;
        let restored = self.decode_restored(encoded.as_deref(), access)?;
        self.recovered = true;
        let kind = match restored {
            None => PendingKind::Initialize { fresh: true },
            Some(State::Initialize) => PendingKind::Initialize { fresh: false },
            Some(State::Ready(ready)) => {
                if ready.is_empty() {
                    return Ok(None);
                }
                let batch = batch::load(
                    &self.buffer,
                    ready,
                    delivery_limits(self.delivery_event_limit()),
                    &mut self.head_cache,
                    &self.codec,
                    access,
                    |change, row| self.target.event_bytes(change, row),
                )?;
                PendingKind::Loaded {
                    before: ready,
                    batch,
                }
            }
        };
        Ok(Some(SinkPending { kind }))
    }
    fn prepare_initialize(&mut self, pending: &SinkPending) -> Result<bool, OperationError> {
        if matches!(pending.kind, PendingKind::Initialize { fresh: true }) {
            self.target.require_absent()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn persist_initialize(
        &self,
        access: TransactionAccess<'_>,
        pending: &SinkPending,
    ) -> Result<(), OperationError> {
        if !matches!(pending.kind, PendingKind::Initialize { fresh: true }) {
            return Err(invalid("only fresh initialization requires persistence"));
        }
        let current = self
            .control
            .access(access)?
            .get_bounded(MAX_CONTROL_BYTES)?;
        if let Some(encoded) = current {
            return match state::decode(&encoded)? {
                State::Initialize => Ok(()),
                State::Ready(_) => Err(invalid("outbox front changed during initialization")),
            };
        }
        self.control
            .access(access)?
            .set(&State::Initialize.encode())?;
        Ok(())
    }
    fn deliver(&mut self, pending: &SinkPending) -> Result<(), OperationError> {
        match &pending.kind {
            PendingKind::Initialize { .. } => self.target.initialize(),
            PendingKind::Loaded { before, batch } => self.target.deliver_prefix(
                &batch.delivery,
                before.tail,
                (before.head.entry_start, &batch.first_entry),
            ),
        }
    }
    fn settle(
        &mut self,
        access: TransactionAccess<'_>,
        pending: &SinkPending,
    ) -> Result<(), OperationError> {
        let encoded = self
            .control
            .access(access)?
            .get_bounded(MAX_CONTROL_BYTES)?
            .ok_or_else(|| invalid("outbox control is missing during settlement"))?;
        let current = state::decode(&encoded)?;
        let ready = match (&pending.kind, current) {
            (PendingKind::Initialize { .. }, State::Initialize) => BufferState::EMPTY,
            (PendingKind::Loaded { before, batch }, State::Ready(current)) => {
                validate_capacity(current)?;
                if current.head != before.head
                    || current.tail < before.tail
                    || current.retained_bytes < before.retained_bytes
                {
                    return Err(invalid("outbox front changed during settlement"));
                }
                let consumed_bytes = before
                    .retained_bytes
                    .checked_sub(batch.after.retained_bytes)
                    .ok_or_else(|| invalid("settlement retained-byte count underflow"))?;
                let next = BufferState {
                    head: batch.after.head,
                    tail: current.tail,
                    retained_bytes: current
                        .retained_bytes
                        .checked_sub(consumed_bytes)
                        .ok_or_else(|| invalid("settlement retained-byte count underflow"))?,
                };
                next.validate()?;
                let mut buffer = self.buffer.access(access)?;
                for key in &batch.consumed_keys {
                    if !buffer.remove(key)? {
                        return Err(invalid("settled outbox entry is missing"));
                    }
                }
                next
            }
            _ => return Err(invalid("outbox front differs during settlement")),
        };
        self.control
            .access(access)?
            .set(&State::Ready(ready).encode())?;
        if self
            .head_cache
            .as_ref()
            .is_some_and(|cached| cached.entry_start != ready.head.entry_start || ready.is_empty())
        {
            self.head_cache = None;
        }
        Ok(())
    }
}
fn prepare_admission(
    codec: &SchemaBoundChangeCodec,
    target: &impl RelationTarget,
    input: &Change,
    events: u64,
) -> Result<Admission, OperationError> {
    if events == 0 {
        return Err(invalid("Change contains no events"));
    }
    if events > MAX_BUFFERED_EVENTS {
        return Err(invalid("Change exceeds the buffered-event capacity"));
    }
    validate_event_sizes(target, input)?;
    let encoded_change = codec.encode_bounded(
        input,
        usize::try_from(MAX_DELIVERY_BYTES).expect("the delivery byte limit fits usize"),
    )?;
    let item_bytes = batch::encoded_item_bytes(&encoded_change)?;
    if item_bytes > MAX_DELIVERY_BYTES {
        return Err(invalid(
            "encoded Change exceeds the single-item byte capacity",
        ));
    }
    Ok(Admission {
        encoded_change,
        item_bytes,
        events,
    })
}

fn validate_event_sizes(
    target: &impl RelationTarget,
    input: &Change,
) -> Result<(), OperationError> {
    for row_index in 0..input.num_rows() {
        let bytes = target.event_bytes(input, row_index)?;
        if bytes == 0 {
            return Err(invalid("target event byte charge must be nonzero"));
        }
        if bytes > MAX_TARGET_BATCH_BYTES {
            return Err(invalid(
                "one target mutation exceeds the target batch byte limit",
            ));
        }
    }
    Ok(())
}

fn fits(ready: BufferState, admission: &Admission) -> bool {
    let Some(pending_events) = ready.pending_events().checked_add(admission.events) else {
        return false;
    };
    ready
        .retained_bytes
        .checked_add(admission.item_bytes)
        .is_some_and(|bytes| bytes <= MAX_RETAINED_BYTES)
        && pending_events <= MAX_BUFFERED_EVENTS
}

const fn delivery_limits(max_events: u64) -> batch::LoadLimits {
    batch::LoadLimits::new(
        max_events,
        MAX_DELIVERY_BYTES,
        MAX_DELIVERY_BYTES,
        MAX_TARGET_BATCH_BYTES,
    )
}

fn validate_capacity(buffer: BufferState) -> Result<(), OperationError> {
    if buffer.pending_events() > MAX_BUFFERED_EVENTS || buffer.retained_bytes > MAX_RETAINED_BYTES {
        Err(invalid("buffer control state exceeds its capacity"))
    } else {
        Ok(())
    }
}
