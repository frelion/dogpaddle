use std::cmp;

use arrow_schema::SchemaRef;
use dogpaddle_change::{Change, encode_change_bounded};
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, StoreError, TransactionAccess};

use super::{
    MAX_TARGET_BATCH_BYTES, SinkTarget,
    batch::{self, DeliveryBatch, LoadedBatch},
    invalid,
    state::{self, BufferState, Header, Prepared, Ready, State},
};
use crate::operation::{Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation};

const MAX_BUFFERED_EVENTS: u64 = 1_048_576;
const MAX_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DELIVERY_BYTES: u64 = 8 * 1024 * 1024;
const MAX_DELIVERY_EVENTS: u64 = 65_536;
const MAX_CONTROL_BYTES: usize = 8 * 1024 * 1024;
const BUFFER_VALIDATION_ITEMS: usize = 256;

struct Admission {
    encoded_change: Vec<u8>,
    item_bytes: u64,
    events: u64,
    first_remaining: u64,
}

pub(crate) struct BufferedSink<T: SinkTarget> {
    schema: SchemaRef,
    target: T,
    control: Cell<Vec<u8>>,
    buffer: OrderedMap<u64, Vec<u8>>,
    head_cache: Option<batch::EntryCache>,
    phase: Phase<T::Checkpoint, T::Plan>,
}

enum Phase<C, P> {
    Restore,
    New,
    Initialized,
    Ready(Ready<C>),
    Loaded(Box<LoadedPhase<C>>),
    Planned(Box<PlannedPhase<C, P>>),
    Delivered(Delivered<C>),
    Failed,
}

struct LoadedPhase<C> {
    ready: Ready<C>,
    batch: LoadedBatch,
}

struct PlannedPhase<C, P> {
    prepared: Prepared<C, P>,
    delivery: DeliveryBatch,
}

#[derive(Clone)]
struct Delivered<C> {
    before: BufferState,
    after: BufferState,
    checkpoint: C,
}

enum Restored<C, P> {
    New,
    Initialize,
    Ready(Ready<C>),
    Prepared(Box<PlannedPhase<C, P>>),
}

struct PreparedRestore<'plan, C> {
    before: BufferState,
    after: BufferState,
    checkpoint: C,
    encoded_plan: &'plan [u8],
}

impl<T: SinkTarget> BufferedSink<T> {
    pub(crate) const fn new(
        schema: SchemaRef,
        target: T,
        control: Cell<Vec<u8>>,
        buffer: OrderedMap<u64, Vec<u8>>,
    ) -> Self {
        Self {
            schema,
            target,
            control,
            buffer,
            head_cache: None,
            phase: Phase::Restore,
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
                AfterCommit::new(move || {
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
                                .deliver(&planned.delivery, &planned.prepared.plan)?;
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
    ) -> Result<Restored<T::Checkpoint, T::Plan>, OperationError> {
        let Some(encoded) = encoded else {
            self.require_empty_buffer(access)?;
            return Ok(Restored::New);
        };
        match state::decode_header::<T>(encoded)? {
            Header::Initialize => {
                self.require_empty_buffer(access)?;
                Ok(Restored::Initialize)
            }
            Header::Ready(ready) => {
                validate_capacity(ready.buffer)?;
                let positive = self.validate_buffer(ready.buffer, access)?;
                self.target.validate_recovery(&ready.checkpoint, positive)?;
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
        restored: PreparedRestore<'_, T::Checkpoint>,
        access: TransactionAccess<'_>,
    ) -> Result<Restored<T::Checkpoint, T::Plan>, OperationError> {
        validate_capacity(restored.before)?;
        validate_capacity(restored.after)?;
        let positive_before = self.validate_buffer(restored.before, access)?;
        let delivered_events = restored
            .before
            .pending_events
            .checked_sub(restored.after.pending_events)
            .filter(|events| *events != 0)
            .ok_or_else(|| invalid("invalid prepared event settlement"))?;
        if delivered_events > delivery_event_limit::<T>() {
            return Err(invalid("prepared delivery exceeds the event limit"));
        }
        let loaded = batch::load(
            &self.buffer,
            restored.before,
            delivery_limits(delivered_events),
            &mut self.head_cache,
            &self.schema,
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
        self.target
            .validate_recovery(&restored.checkpoint, positive_after)?;
        let mut encoded_plan = restored.encoded_plan;
        let plan = T::decode_plan(&mut encoded_plan, &loaded.delivery, &restored.checkpoint)?;
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
                    batch::decode_entry(sequence, encoded, MAX_DELIVERY_BYTES, &self.schema)?;
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
        let encoded = State::<T::Checkpoint, T::Plan>::Initialize.encode::<T>();
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
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
            checkpoint: self.target.initial_checkpoint(),
        };
        let encoded = encode_bounded::<T>(&State::Ready(ready.clone()))?;
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Ready(ready);
                    Ok(())
                }),
            ))
        }))
    }

    fn admit(
        &mut self,
        admission: Admission,
        ready: Ready<T::Checkpoint>,
    ) -> Result<Turn<'_>, OperationError> {
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
        let encoded_control = encode_bounded::<T>(&State::Ready(next.clone()))?;
        Ok(Turn::ready(move |access| {
            let mut map = self.buffer.access(access)?;
            if map.get(&sequence)?.is_some() {
                return Err(invalid(format!("buffer entry {sequence} already exists")));
            }
            map.put(&sequence, &admission.encoded_change)?;
            self.control.access(access)?.set(&encoded_control)?;
            Ok((
                Action::Complete(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Ready(next);
                    Ok(())
                }),
            ))
        }))
    }

    fn load(&mut self, ready: Ready<T::Checkpoint>) -> Turn<'_> {
        let max_events = delivery_event_limit::<T>();
        Turn::ready(move |access| {
            let loaded = batch::load(
                &self.buffer,
                ready.buffer,
                delivery_limits(max_events),
                &mut self.head_cache,
                &self.schema,
                access,
                |change, row| self.target.event_bytes(change, row),
            )?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
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
            Phase::Loaded(loaded) => (loaded.ready.clone(), loaded.batch.clone()),
            _ => unreachable!("plan is called only for a loaded batch"),
        };
        let (checkpoint, plan) = self.target.prepare(&loaded.delivery, &ready.checkpoint)?;
        let prepared = Prepared {
            before: ready.buffer,
            after: loaded.after,
            checkpoint,
            plan,
        };
        self.phase = Phase::Planned(Box::new(PlannedPhase {
            prepared,
            delivery: loaded.delivery,
        }));
        self.persist_planned()
    }

    fn persist_planned(&mut self) -> Result<Turn<'_>, OperationError> {
        let (prepared, delivery) = match &self.phase {
            Phase::Planned(planned) => (planned.prepared.clone(), planned.delivery.clone()),
            _ => unreachable!("persist is called only for a planned batch"),
        };
        let encoded = encode_bounded::<T>(&State::Prepared(prepared.clone()))?;
        Ok(Turn::ready(move |access| {
            self.control.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Failed;
                    self.target.deliver(&delivery, &prepared.plan)?;
                    self.phase = Phase::Delivered(delivered(&prepared));
                    Ok(())
                }),
            ))
        }))
    }

    fn settle(&mut self, delivered: Delivered<T::Checkpoint>) -> Result<Turn<'_>, OperationError> {
        let ready = Ready {
            buffer: delivered.after,
            checkpoint: delivered.checkpoint,
        };
        let encoded = encode_bounded::<T>(&State::Ready(ready.clone()))?;
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
                AfterCommit::new(move || {
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
}

impl<T: SinkTarget> TurnOperation for BufferedSink<T> {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let input = input
            .map(|input| {
                if input.port != 0 {
                    return Err(invalid("only input port zero is supported"));
                }
                if input.change.records().schema() != self.schema {
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
                let ready = ready.clone();
                if ready.buffer.is_empty() {
                    match input {
                        Some(input) => self.admit(
                            prepare_admission(
                                &self.target,
                                &ready.checkpoint,
                                ready.buffer.pending_events,
                                input,
                            )?,
                            ready,
                        ),
                        None => Ok(Turn::Idle),
                    }
                } else if input.is_none() || should_drain::<T>(&ready) {
                    Ok(self.load(ready))
                } else {
                    match prepare_admission(
                        &self.target,
                        &ready.checkpoint,
                        ready.buffer.pending_events,
                        input.expect("the offered input was checked"),
                    ) {
                        Ok(admission) if fits(&ready, &admission) => self.admit(admission, ready),
                        Ok(_) | Err(_) => Ok(self.load(ready)),
                    }
                }
            }
            Phase::Loaded(_) => self.plan(),
            Phase::Planned(_) => self.persist_planned(),
            Phase::Delivered(delivered) => self.settle(delivered.clone()),
            Phase::Failed => Err(invalid(
                "runtime must be reopened after a post-commit failure",
            )),
        }
    }
}

fn prepare_admission<T: SinkTarget>(
    target: &T,
    checkpoint: &T::Checkpoint,
    buffered_events: u64,
    input: &Change,
) -> Result<Admission, OperationError> {
    let events = batch::event_count(input)?;
    if events > MAX_BUFFERED_EVENTS {
        return Err(invalid("Change exceeds the buffered-event capacity"));
    }
    target.validate_admission(input, checkpoint, buffered_events)?;
    validate_event_sizes(target, input)?;
    let encoded_change = encode_change_bounded(
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

fn validate_event_sizes<T: SinkTarget>(target: &T, input: &Change) -> Result<(), OperationError> {
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

fn fits<C>(ready: &Ready<C>, admission: &Admission) -> bool {
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

fn should_drain<T: SinkTarget>(ready: &Ready<T::Checkpoint>) -> bool {
    ready.buffer.retained_bytes >= MAX_DELIVERY_BYTES
        || ready.buffer.pending_events >= delivery_event_limit::<T>()
}

fn delivery_event_limit<T: SinkTarget>() -> u64 {
    cmp::min(u64::from(T::MAX_BATCH_EVENTS.get()), MAX_DELIVERY_EVENTS)
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

fn delivered<C: Clone, P>(prepared: &Prepared<C, P>) -> Delivered<C> {
    Delivered {
        before: prepared.before,
        after: prepared.after,
        checkpoint: prepared.checkpoint.clone(),
    }
}

fn encode_bounded<T: SinkTarget>(
    state: &State<T::Checkpoint, T::Plan>,
) -> Result<Vec<u8>, OperationError> {
    state.validate()?;
    let encoded = state.encode::<T>();
    if encoded.len() > MAX_CONTROL_BYTES {
        Err(invalid("control state exceeds its byte limit"))
    } else {
        Ok(encoded)
    }
}

#[cfg(test)]
mod tests;
