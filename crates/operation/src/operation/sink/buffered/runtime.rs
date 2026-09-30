use super::{
    MAX_TARGET_BATCH_BYTES,
    batch::{self, DeliveryBatch, LoadedBatch},
    invalid,
    state::{self, BufferState, Header, Prepared, Ready, State},
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
const MAX_CONTROL_BYTES: usize = 8 * 1024 * 1024;
const BUFFER_VALIDATION_ITEMS: usize = 256;
struct Admission {
    encoded_change: Vec<u8>,
    item_bytes: u64,
    events: u64,
    first_remaining: u64,
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
    Initialize { fresh: bool },
    Loaded { ready: Ready, batch: LoadedBatch },
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
    prepared: Prepared,
    delivery: DeliveryBatch,
}
enum Restored {
    New,
    Initialize,
    Ready(Ready),
    Prepared(Box<PlannedPhase>),
}
#[derive(Clone, Copy)]
struct PreparedRestore<'plan> {
    before: BufferState,
    after: BufferState,
    checkpoint: u64,
    encoded_plan: &'plan [u8],
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
                validate_capacity(ready.buffer)?;
                if !self.recovered {
                    let positive = self.validate_buffer(ready.buffer, access)?;
                    relation::validate_recovery(ready.checkpoint, positive)?;
                }
                Ok(Restored::Ready(ready))
            }
            Header::Prepared {
                before,
                after,
                checkpoint,
                encoded_plan,
            } => self.restore_prepared(
                PreparedRestore {
                    before,
                    after,
                    checkpoint,
                    encoded_plan,
                },
                access,
            ),
        }
    }

    fn restore_prepared(
        &mut self,
        restored: PreparedRestore<'_>,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Restored, OperationError> {
        validate_capacity(restored.before)?;
        validate_capacity(restored.after)?;
        let positive_before = if self.recovered {
            None
        } else {
            Some(self.validate_buffer(restored.before, access)?)
        };
        let delivered_events = restored
            .before
            .pending_events
            .checked_sub(restored.after.pending_events)
            .filter(|events| *events != 0)
            .ok_or_else(|| invalid("invalid prepared event settlement"))?;
        if delivered_events > self.delivery_event_limit() {
            return Err(invalid("prepared delivery exceeds the event limit"));
        }
        let loaded = batch::load(
            &self.buffer,
            restored.before,
            delivery_limits(delivered_events),
            &mut self.head_cache,
            &self.codec,
            access,
            |change, row| self.target.event_bytes(change, row),
        )?;
        if loaded.after != restored.after {
            return Err(invalid(
                "prepared settlement does not match its buffered batch",
            ));
        }
        if let Some(positive_before) = positive_before {
            let delivered_positive = batch::positive_event_count(loaded.delivery.change())?;
            let positive_after = positive_before
                .checked_sub(delivered_positive)
                .ok_or_else(|| invalid("prepared positive-event settlement underflow"))?;
            relation::validate_recovery(restored.checkpoint, positive_after)?;
        }
        let mut encoded_plan = restored.encoded_plan;
        let plan = relation::decode_plan(&mut encoded_plan, &loaded.delivery, restored.checkpoint)?;
        if !encoded_plan.is_empty() {
            return Err(invalid("trailing control-state bytes"));
        }
        Ok(Restored::Prepared(Box::new(PlannedPhase {
            prepared: Prepared {
                before: restored.before,
                after: restored.after,
                checkpoint: restored.checkpoint,
                plan,
            },
            delivery: loaded.delivery,
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
    ) -> Result<u64, OperationError> {
        let Some(head) = state.head else {
            self.require_empty_buffer(access)?;
            return Ok(0);
        };
        self.require_empty_range(..head.sequence, access)?;
        self.require_empty_range(state.tail.., access)?;

        let limit = ScanLimit::new(
            BUFFER_VALIDATION_ITEMS,
            usize::try_from(MAX_DELIVERY_BYTES).expect("the byte limit fits usize"),
        )
        .expect("the buffer validation limits are nonzero");
        let map = self.buffer.read(access)?;
        let mut continuation = None;
        let mut expected_sequence = head.sequence;
        let mut pending_events = 0_u64;
        let mut positive_events = 0_u64;
        let mut retained_bytes = 0_u64;
        loop {
            let page = match map.scan(
                head.sequence..state.tail,
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
            for (sequence, encoded) in page.entries {
                if sequence != expected_sequence {
                    return Err(invalid(format!(
                        "buffer entry {expected_sequence} is missing"
                    )));
                }
                let item_bytes = batch::encoded_item_bytes(&encoded)?;
                let change =
                    batch::decode_entry(sequence, encoded, MAX_DELIVERY_BYTES, &self.codec)?;
                validate_event_sizes(&self.target, &change)?;
                let (events, positive) = batch::remaining_event_counts(
                    &change,
                    if sequence == head.sequence {
                        head
                    } else {
                        state::Position::entry_start(sequence)
                    },
                )?;
                pending_events = pending_events
                    .checked_add(events)
                    .ok_or_else(|| invalid("buffer pending-event count exceeds u64"))?;
                positive_events = positive_events
                    .checked_add(positive)
                    .ok_or_else(|| invalid("buffer positive-event count exceeds u64"))?;
                retained_bytes = retained_bytes
                    .checked_add(item_bytes)
                    .ok_or_else(|| invalid("buffer retained-byte count exceeds u64"))?;
                if pending_events > state.pending_events || retained_bytes > state.retained_bytes {
                    return Err(invalid(
                        "buffer contents exceed their control-state accounting",
                    ));
                }
                expected_sequence = expected_sequence
                    .checked_add(1)
                    .ok_or_else(|| invalid("buffer sequence is exhausted"))?;
            }
            let Some(next) = page.continuation else {
                break;
            };
            continuation = Some(next);
        }
        if expected_sequence != state.tail {
            return Err(invalid(format!(
                "buffer entry {expected_sequence} is missing"
            )));
        }
        if pending_events != state.pending_events || retained_bytes != state.retained_bytes {
            return Err(invalid(
                "buffer contents do not match their control-state accounting",
            ));
        }
        Ok(positive_events)
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
        // can only be Prepared, whose fixed-ID plan need not be copied here.
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
        let admission = prepare_admission(
            &self.codec,
            &self.target,
            ready.checkpoint,
            ready.buffer.pending_events,
            page,
        )?;
        if !fits(&ready, &admission) {
            return Ok(false);
        }
        let sequence = ready.buffer.tail;
        let next = Ready {
            checkpoint: ready.checkpoint,
            buffer: BufferState {
                head: ready.buffer.head.or(Some(state::Position {
                    sequence,
                    row_index: 0,
                    remaining: admission.first_remaining,
                })),
                tail: sequence + 1,
                pending_events: ready.buffer.pending_events + admission.events,
                retained_bytes: ready.buffer.retained_bytes + admission.item_bytes,
            },
        };
        let mut map = self.buffer.access(access)?;
        if map.get_bounded(&sequence, 0)?.is_some() {
            return Err(invalid("outbox sequence already exists"));
        }
        map.put(&sequence, &admission.encoded_change)?;
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
                if ready.buffer.is_empty() {
                    return Ok(None);
                }
                let batch = batch::load(
                    &self.buffer,
                    ready.buffer,
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
        let (encoded, kind) = match pending.kind {
            PendingKind::Initialize { fresh } => {
                if fresh {
                    self.target.require_absent()?;
                }
                (State::Initialize.encode(), PreparedKind::Initialize)
            }
            PendingKind::Prepared(planned) => (
                encode_bounded(&State::Prepared(planned.prepared.clone()))?,
                PreparedKind::Batch(Box::new(planned)),
            ),
            PendingKind::Loaded { ready, batch } => {
                let (checkpoint, plan) =
                    relation::prepare(&mut self.target, &batch.delivery, ready.checkpoint)?;
                let prepared = Prepared {
                    before: ready.buffer,
                    after: batch.after,
                    checkpoint,
                    plan,
                };
                (
                    encode_bounded(&State::Prepared(prepared.clone()))?,
                    PreparedKind::Batch(Box::new(PlannedPhase {
                        prepared,
                        delivery: batch.delivery,
                    })),
                )
            }
        };
        Ok(SinkPrepared {
            expected: pending.expected,
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
                .write_batch(planned.delivery.change(), &planned.prepared.plan),
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
            PreparedKind::Initialize => Ready {
                buffer: BufferState::EMPTY,
                checkpoint: relation::FIRST_TECHNICAL_ID,
            },
            PreparedKind::Batch(planned) => {
                let prepared = &planned.prepared;
                let remove_end = prepared
                    .after
                    .head
                    .map_or(prepared.before.tail, |head| head.sequence);
                let mut buffer = self.buffer.access(access)?;
                for sequence in prepared
                    .before
                    .head
                    .expect("prepared front is nonempty")
                    .sequence..remove_end
                {
                    if !buffer.remove(&sequence)? {
                        return Err(invalid("settled outbox entry is missing"));
                    }
                }
                Ready {
                    buffer: prepared.after,
                    checkpoint: prepared.checkpoint,
                }
            }
        };
        self.control
            .access(access)?
            .set(&encode_bounded(&State::Ready(ready))?)?;
        self.head_cache = None;
        Ok(())
    }
}
fn prepare_admission(
    codec: &SchemaBoundChangeCodec,
    target: &impl RelationTarget,
    checkpoint: u64,
    buffered_events: u64,
    input: &Change,
) -> Result<Admission, OperationError> {
    let events = batch::event_count(input)?;
    if events > MAX_BUFFERED_EVENTS {
        return Err(invalid("Change exceeds the buffered-event capacity"));
    }
    relation::validate_admission(input, checkpoint, buffered_events)?;
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
        first_remaining: input.diffs().value(0).unsigned_abs(),
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

fn fits(ready: &Ready, admission: &Admission) -> bool {
    let Some(pending_events) = ready.buffer.pending_events.checked_add(admission.events) else {
        return false;
    };
    ready
        .buffer
        .retained_bytes
        .checked_add(admission.item_bytes)
        .is_some_and(|bytes| bytes <= MAX_RETAINED_BYTES)
        && pending_events <= MAX_BUFFERED_EVENTS
        && ready.buffer.tail.checked_add(1).is_some()
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
    if buffer.pending_events > MAX_BUFFERED_EVENTS || buffer.retained_bytes > MAX_RETAINED_BYTES {
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
