use super::{
    MAX_TARGET_BATCH_BYTES,
    batch::{self, DeliveryBatch, LoadedBatch},
    invalid,
    state::{self, BufferState, Header, MAX_CONTROL_BYTES, Prepared, State},
};
use crate::operation::sink::relation::{self, Batch, RelationTarget};
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
/// A bounded source prefix, or an already durable prepared front.
pub struct SinkPending {
    expected: Option<Vec<u8>>,
    kind: PendingKind,
}
enum PendingKind {
    Initialize {
        fresh: bool,
    },
    Loaded {
        ready: BufferState,
        batch: LoadedBatch,
    },
    Prepared(PlannedPhase),
}
/// The concrete fixed-ID target plan retained until local settlement.
pub struct SinkPrepared {
    expected: Option<Vec<u8>>,
    encoded: Vec<u8>,
    kind: PreparedKind,
}
enum PreparedKind {
    Initialize,
    Batch(Box<PlannedPhase>),
}
struct PlannedPhase {
    after: BufferState,
    delivery: DeliveryBatch,
    plan: Batch,
    consumed_keys: Vec<u64>,
}
enum Restored {
    New,
    Initialize,
    Ready(BufferState),
    Prepared(Box<PlannedPhase>),
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
    ) -> Result<Restored, OperationError> {
        let Some(encoded) = encoded else {
            self.require_empty_buffer(access)?;
            return Ok(Restored::New);
        };
        match state::decode_header(encoded)? {
            Header::Initialize => {
                self.require_empty_buffer(access)?;
                Ok(Restored::Initialize)
            }
            Header::Ready(ready) => {
                validate_capacity(ready)?;
                if !self.recovered {
                    self.validate_buffer(ready, access)?;
                }
                Ok(Restored::Ready(ready))
            }
            Header::Prepared {
                before,
                after,
                encoded_negative_ids,
            } => self.restore_prepared(before, after, encoded_negative_ids, access),
        }
    }

    fn restore_prepared(
        &mut self,
        before: BufferState,
        after: BufferState,
        encoded_negative_ids: &[u8],
        access: ReadTransactionAccess<'_>,
    ) -> Result<Restored, OperationError> {
        validate_capacity(before)?;
        validate_capacity(after)?;
        if !self.recovered {
            self.validate_buffer(before, access)?;
        }
        let delivered_events = after
            .head
            .event_offset
            .checked_sub(before.head.event_offset)
            .filter(|events| *events != 0)
            .ok_or_else(|| invalid("invalid prepared event settlement"))?;
        if delivered_events > self.delivery_event_limit() {
            return Err(invalid("prepared delivery exceeds the event limit"));
        }
        let loaded = batch::load(
            &self.buffer,
            before,
            delivery_limits(delivered_events),
            &mut self.head_cache,
            &self.codec,
            access,
            |change, row| self.target.event_bytes(change, row),
        )?;
        if loaded.after != after {
            return Err(invalid(
                "prepared settlement does not match its buffered batch",
            ));
        }
        let negative_ids = state::decode_negative_ids(encoded_negative_ids)?;
        let plan = relation::recover(
            &loaded.delivery,
            &negative_ids,
            (before.head.entry_start, &loaded.first_entry),
        )?;
        Ok(Restored::Prepared(Box::new(PlannedPhase {
            after,
            delivery: loaded.delivery,
            plan,
            consumed_keys: loaded.consumed_keys,
        })))
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
        // load validates the complete control before this runtime is serviced.
        // Our only writer produces canonical states: a value larger than Ready
        // can only be Prepared, whose ordered negative IDs need not be copied here.
        let encoded = match self
            .control
            .access(access)?
            .get_bounded(state::MAX_READY_BYTES)
        {
            Ok(encoded) => encoded,
            Err(StoreError::ItemTooLarge { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let Some(encoded) = encoded else {
            return Ok(false);
        };
        let Header::Ready(ready) = state::decode_header(&encoded)? else {
            return Ok(false);
        };
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
        let mut map = self.buffer.access(access)?;
        if map.get_bounded(&entry_start, 0)?.is_some() {
            return Err(invalid("outbox entry_start already exists"));
        }
        map.put(&entry_start, &admission.encoded_change)?;
        self.control
            .access(access)?
            .set(&encode_bounded(&State::Ready(next))?)?;
        Ok(true)
    }
    fn load(
        &mut self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<SinkPending>, OperationError> {
        let expected = self.control.read(access)?.get_bounded(MAX_CONTROL_BYTES)?;
        let restored = self.decode_restored(expected.as_deref(), access)?;
        self.recovered = true;
        let kind = match restored {
            Restored::New => PendingKind::Initialize { fresh: true },
            Restored::Initialize => PendingKind::Initialize { fresh: false },
            Restored::Prepared(planned) => PendingKind::Prepared(*planned),
            Restored::Ready(ready) => {
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
                PendingKind::Loaded { ready, batch }
            }
        };
        Ok(Some(SinkPending { expected, kind }))
    }
    fn prepare(&mut self, pending: SinkPending) -> Result<SinkPrepared, OperationError> {
        let SinkPending { expected, kind } = pending;
        let (encoded, kind) = match kind {
            PendingKind::Initialize { fresh } => {
                if fresh {
                    self.target.require_absent()?;
                }
                (State::Initialize.encode(), PreparedKind::Initialize)
            }
            PendingKind::Prepared(planned) => (
                expected
                    .as_ref()
                    .expect("restored Prepared has durable control bytes")
                    .clone(),
                PreparedKind::Batch(Box::new(planned)),
            ),
            PendingKind::Loaded { ready, batch } => {
                let plan = relation::prepare(
                    &mut self.target,
                    &batch.delivery,
                    (ready.head.entry_start, &batch.first_entry),
                )?;
                let prepared = Prepared {
                    before: ready,
                    after: batch.after,
                    negative_ids: plan.negative_ids(),
                };
                (
                    encode_bounded(&State::Prepared(prepared))?,
                    PreparedKind::Batch(Box::new(PlannedPhase {
                        after: batch.after,
                        delivery: batch.delivery,
                        plan,
                        consumed_keys: batch.consumed_keys,
                    })),
                )
            }
        };
        Ok(SinkPrepared {
            expected,
            encoded,
            kind,
        })
    }
    fn persist_prepared(
        &self,
        access: TransactionAccess<'_>,
        prepared: &SinkPrepared,
    ) -> Result<(), OperationError> {
        let current = self
            .control
            .access(access)?
            .get_bounded(MAX_CONTROL_BYTES)?;
        if current != prepared.expected && current.as_deref() != Some(prepared.encoded.as_slice()) {
            return Err(invalid("outbox front changed during planning"));
        }
        self.control.access(access)?.set(&prepared.encoded)?;
        Ok(())
    }
    fn deliver(&mut self, prepared: &SinkPrepared) -> Result<(), OperationError> {
        match &prepared.kind {
            PreparedKind::Initialize => self.target.initialize(),
            PreparedKind::Batch(planned) => self
                .target
                .write_batch(planned.delivery.change(), &planned.plan),
        }
    }
    fn settle(
        &mut self,
        access: TransactionAccess<'_>,
        prepared: &SinkPrepared,
    ) -> Result<(), OperationError> {
        if self
            .control
            .access(access)?
            .get_bounded(MAX_CONTROL_BYTES)?
            .as_deref()
            != Some(prepared.encoded.as_slice())
        {
            return Err(invalid("prepared front differs during settlement"));
        }
        let ready = match &prepared.kind {
            PreparedKind::Initialize => BufferState::EMPTY,
            PreparedKind::Batch(planned) => {
                let mut buffer = self.buffer.access(access)?;
                for key in &planned.consumed_keys {
                    if !buffer.remove(key)? {
                        return Err(invalid("settled outbox entry is missing"));
                    }
                }
                planned.after
            }
        };
        self.control
            .access(access)?
            .set(&encode_bounded(&State::Ready(ready))?)?;
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

fn encode_bounded(state: &State) -> Result<Vec<u8>, OperationError> {
    state.validate()?;
    let encoded = state.encode();
    if encoded.len() > MAX_CONTROL_BYTES {
        Err(invalid("control state exceeds its byte limit"))
    } else {
        Ok(encoded)
    }
}
