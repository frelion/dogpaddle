use std::sync::Arc;

use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, StoreError, TransactionAccess};

use super::{
    MAX_TARGET_BATCH_BYTES,
    batch::{self, DeliveryBatch, LoadedBatch},
    invalid,
    state::{self, BufferState, Header, Prepared, Ready, State},
};
use crate::operation::sink::relation::{self, RelationTarget};
use crate::operation::{Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation};

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
    phase: Phase,
    #[cfg(test)]
    max_batch_events: u64,
}

enum Phase {
    Restore,
    New,
    Initialized,
    Ready(Ready),
    Loaded(Box<LoadedPhase>),
    Delivered(Delivered),
    Failed,
}

struct LoadedPhase {
    ready: Ready,
    batch: LoadedBatch,
}

struct PlannedPhase {
    prepared: Prepared,
    delivery: DeliveryBatch,
}

#[derive(Clone, Copy)]
struct Delivered {
    before: BufferState,
    after: BufferState,
    checkpoint: u64,
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
            phase: Phase::Restore,
            #[cfg(test)]
            max_batch_events: relation::MAX_MUTATIONS_PER_BATCH as u64,
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let encoded = self
                .control
                .access(access)?
                .get_bounded(MAX_CONTROL_BYTES)?;
            let restored = self.decode_restored(encoded.as_deref(), access)?;
            Ok((
                Action::Commit(None),
                AfterCommit::durable(move || {
                    self.phase = Phase::Failed;
                    match restored {
                        Restored::New => self.phase = Phase::New,
                        Restored::Initialize => {
                            self.target.initialize()?;
                            self.phase = Phase::Initialized;
                        }
                        Restored::Ready(ready) => self.phase = Phase::Ready(ready),
                        Restored::Prepared(planned) => {
                            self.target
                                .write_batch(planned.delivery.change(), &planned.prepared.plan)?;
                            self.phase = Phase::Delivered(delivered(&planned.prepared));
                        }
                    }
                    Ok(())
                }),
            ))
        })
    }

    fn decode_restored(
        &mut self,
        encoded: Option<&[u8]>,
        access: TransactionAccess<'_>,
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
                let positive = self.validate_buffer(ready.buffer, access)?;
                relation::validate_recovery(ready.checkpoint, positive)?;
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
        access: TransactionAccess<'_>,
    ) -> Result<Restored, OperationError> {
        validate_capacity(restored.before)?;
        validate_capacity(restored.after)?;
        let positive_before = self.validate_buffer(restored.before, access)?;
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
        let delivered_positive = batch::positive_event_count(loaded.delivery.change())?;
        let positive_after = positive_before
            .checked_sub(delivered_positive)
            .ok_or_else(|| invalid("prepared positive-event settlement underflow"))?;
        relation::validate_recovery(restored.checkpoint, positive_after)?;
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

    fn require_empty_buffer(&self, access: TransactionAccess<'_>) -> Result<(), OperationError> {
        self.require_empty_range(.., access)
    }

    fn validate_buffer(
        &self,
        state: BufferState,
        access: TransactionAccess<'_>,
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
        let map = self.buffer.access(access)?;
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
        access: TransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        let limit = ScanLimit::new(
            1,
            usize::try_from(MAX_DELIVERY_BYTES).expect("the byte limit fits usize"),
        )
        .expect("the buffer scan limits are nonzero");
        match self
            .buffer
            .access(access)?
            .scan(range, ScanDirection::Ascending, None, limit)
        {
            Ok(page) if page.entries.is_empty() => Ok(()),
            Ok(_) | Err(StoreError::ItemTooLarge { .. }) => Err(invalid(
                "buffer contains an entry outside its control-state range",
            )),
            Err(source) => Err(source.into()),
        }
    }

    fn initialize(&mut self) -> Result<Turn<'_>, OperationError> {
        self.target.require_absent()?;
        let encoded = State::Initialize.encode();
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::durable(move || {
                    self.phase = Phase::Failed;
                    self.target.initialize()?;
                    self.phase = Phase::Initialized;
                    Ok(())
                }),
            ))
        }))
    }

    fn publish_ready(&mut self) -> Result<Turn<'_>, OperationError> {
        let ready = Ready {
            buffer: BufferState::EMPTY,
            checkpoint: relation::FIRST_TECHNICAL_ID,
        };
        let encoded = encode_bounded(&State::Ready(ready))?;
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::local(move || {
                    self.phase = Phase::Ready(ready);
                    Ok(())
                }),
            ))
        }))
    }

    fn admit(&mut self, admission: Admission, ready: Ready) -> Result<Turn<'_>, OperationError> {
        let retained_bytes = ready
            .buffer
            .retained_bytes
            .checked_add(admission.item_bytes)
            .filter(|bytes| *bytes <= MAX_RETAINED_BYTES)
            .ok_or_else(|| invalid("buffer retained-byte capacity is exhausted"))?;
        let pending_events = ready
            .buffer
            .pending_events
            .checked_add(admission.events)
            .filter(|events| *events <= MAX_BUFFERED_EVENTS)
            .ok_or_else(|| invalid("buffered-event capacity is exhausted"))?;
        let sequence = ready.buffer.tail;
        let tail = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("buffer sequence is exhausted"))?;
        let buffer = BufferState {
            head: ready.buffer.head.or(Some(state::Position {
                sequence,
                row_index: 0,
                remaining: admission.first_remaining,
            })),
            tail,
            pending_events,
            retained_bytes,
        };
        buffer.validate()?;
        let next = Ready { buffer, ..ready };
        let encoded_control = encode_bounded(&State::Ready(next))?;
        Ok(Turn::ready(move |access| {
            let mut map = self.buffer.access(access)?;
            if map.get(&sequence)?.is_some() {
                return Err(invalid(format!("buffer entry {sequence} already exists")));
            }
            map.put(&sequence, &admission.encoded_change)?;
            self.control.access(access)?.set(&encoded_control)?;
            Ok((
                Action::Complete(None),
                AfterCommit::local(move || {
                    self.phase = Phase::Ready(next);
                    Ok(())
                }),
            ))
        }))
    }

    fn load(&mut self, ready: Ready) -> Turn<'_> {
        let max_events = self.delivery_event_limit();
        Turn::ready(move |access| {
            let loaded = batch::load(
                &self.buffer,
                ready.buffer,
                delivery_limits(max_events),
                &mut self.head_cache,
                &self.codec,
                access,
                |change, row| self.target.event_bytes(change, row),
            )?;
            Ok((
                Action::Commit(None),
                AfterCommit::local(move || {
                    self.phase = Phase::Loaded(Box::new(LoadedPhase {
                        ready,
                        batch: loaded,
                    }));
                    Ok(())
                }),
            ))
        })
    }

    fn plan(&mut self) -> Result<Turn<'_>, OperationError> {
        let (ready, loaded) = match &self.phase {
            Phase::Loaded(loaded) => (loaded.ready, loaded.batch.clone()),
            _ => unreachable!("plan is called only for a loaded batch"),
        };
        let (checkpoint, plan) =
            relation::prepare(&mut self.target, &loaded.delivery, ready.checkpoint)?;
        let prepared = Prepared {
            before: ready.buffer,
            after: loaded.after,
            checkpoint,
            plan,
        };
        let delivery = loaded.delivery;
        let encoded = encode_bounded(&State::Prepared(prepared.clone()))?;
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::durable(move || {
                    self.phase = Phase::Failed;
                    self.target.write_batch(delivery.change(), &prepared.plan)?;
                    self.phase = Phase::Delivered(delivered(&prepared));
                    Ok(())
                }),
            ))
        }))
    }

    fn settle(&mut self, delivered: Delivered) -> Result<Turn<'_>, OperationError> {
        let ready = Ready {
            buffer: delivered.after,
            checkpoint: delivered.checkpoint,
        };
        let encoded = encode_bounded(&State::Ready(ready))?;
        let remove_end = delivered
            .after
            .head
            .map_or(delivered.before.tail, |head| head.sequence);
        Ok(Turn::ready(move |access| {
            let mut buffer = self.buffer.access(access)?;
            for sequence in delivered
                .before
                .head
                .expect("delivered buffer is nonempty")
                .sequence..remove_end
            {
                if !buffer.remove(&sequence)? {
                    return Err(invalid(format!(
                        "settled buffer entry {sequence} is missing"
                    )));
                }
            }
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::local(move || {
                    let head = ready.buffer.head.map(|position| position.sequence);
                    if self
                        .head_cache
                        .as_ref()
                        .is_some_and(|cached| Some(cached.sequence) != head)
                    {
                        self.head_cache = None;
                    }
                    self.phase = Phase::Ready(ready);
                    Ok(())
                }),
            ))
        }))
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))] // Tests use a smaller per-fixture batch.
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

impl<T: RelationTarget> TurnOperation for BufferedSink<T> {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let bound_schema = self.codec.schema();
        let input = input
            .map(|input| {
                if input.port != 0 {
                    return Err(invalid("only input port zero is supported"));
                }
                let actual = input.change.records().schema_ref();
                if !Arc::ptr_eq(&bound_schema, actual) && bound_schema.as_ref() != actual.as_ref() {
                    return Err(invalid("input Schema differs from the bound Schema"));
                }
                Ok(input.change)
            })
            .transpose()?;

        match &self.phase {
            Phase::Restore => Ok(self.restore()),
            Phase::New => self.initialize(),
            Phase::Initialized => self.publish_ready(),
            Phase::Ready(ready) => {
                let ready = *ready;
                if ready.buffer.is_empty() {
                    match input {
                        Some(input) => self.admit(
                            prepare_admission(
                                &self.codec,
                                &self.target,
                                ready.checkpoint,
                                ready.buffer.pending_events,
                                input,
                            )?,
                            ready,
                        ),
                        None => Ok(Turn::Idle),
                    }
                } else if input.is_none() || should_drain(&ready, self.delivery_event_limit()) {
                    Ok(self.load(ready))
                } else {
                    match prepare_admission(
                        &self.codec,
                        &self.target,
                        ready.checkpoint,
                        ready.buffer.pending_events,
                        input.expect("the offered input was checked"),
                    ) {
                        Ok(admission) if fits(&ready, &admission) => self.admit(admission, ready),
                        Ok(_) | Err(_) => Ok(self.load(ready)),
                    }
                }
            }
            Phase::Loaded(_) => self.plan(),
            Phase::Delivered(delivered) => self.settle(*delivered),
            Phase::Failed => Err(invalid(
                "runtime must be reopened after a post-commit failure",
            )),
        }
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

fn should_drain(ready: &Ready, event_limit: u64) -> bool {
    ready.buffer.retained_bytes >= MAX_DELIVERY_BYTES || ready.buffer.pending_events >= event_limit
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

fn delivered(prepared: &Prepared) -> Delivered {
    Delivered {
        before: prepared.before,
        after: prepared.after,
        checkpoint: prepared.checkpoint,
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

#[cfg(test)]
mod tests;
