use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
    sync::Arc,
};

use arrow_schema::SchemaRef;
use dogpaddle_change::{Change, decode_change, encode_change};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactionAccess, ReadTransactions, ScanLimit, Small,
    TransactionAccess, Transactions,
};

use super::protocol::StationError;
use crate::flow::{InputStatus, OutputStatus};

pub(super) const ACTIVE_INPUT_KEY: &[u8] = b"input/active";
pub(super) const CURSOR_ORIGIN: u64 = 0;
const RECLAIM_ONE: NonZeroUsize = NonZeroUsize::new(1).expect("one reclaimed input is nonzero");

/// One durable input identity with an owned decoded value.
pub(super) struct Claim {
    port: usize,
    offset: u64,
    change: Change,
}

struct EncodedClaim {
    port: usize,
    offset: u64,
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CompletionPlan {
    pub(super) next_cursor: u64,
    pub(super) next_active: usize,
    pub(super) reclaim_to: Option<u64>,
}

/// One input edge and its acknowledgement capability.
pub(crate) struct InputPort {
    output: Arc<Output>,
    consumer_slot: usize,
}

/// A Station's complete input-delivery state.
pub(super) struct Inbox {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    ports: Vec<InputPort>,
    claim: Option<Claim>,
}

/// A read capability for one consumer edge's durable frontier.
pub(crate) struct ConsumerCursor {
    state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
    input: usize,
}

/// One producer's append, input-read, and physical-retention capability.
pub(crate) struct Output {
    log: AppendLog<Vec<u8>>,
    capacity_bytes: NonZeroU64,
    schema: SchemaRef,
    consumers: Box<[ConsumerCursor]>,
}

impl Claim {
    pub(super) const fn port(&self) -> usize {
        self.port
    }

    #[cfg(test)]
    pub(super) const fn offset(&self) -> u64 {
        self.offset
    }

    pub(super) const fn change(&self) -> &Change {
        &self.change
    }
}

impl Inbox {
    pub(super) fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(Option<usize>, Vec<InputStatus>), StationError> {
        if self.ports.is_empty() {
            return Ok((None, Vec::new()));
        }
        let state = self.state.read(access)?;
        let active = state
            .get(&ACTIVE_INPUT_KEY.to_vec())?
            .ok_or(StationError::MissingActiveInput)?;
        let active = decode_active_input(&active).ok_or(StationError::MalformedActiveInput)?;
        if active >= self.ports.len() {
            return Err(StationError::ActiveInputOutOfRange {
                input: active,
                input_count: self.ports.len(),
            });
        }
        let inputs = self
            .ports
            .iter()
            .map(|port| {
                let cursor = port.output.consumers[port.consumer_slot]
                    .read_snapshot(port.consumer_slot, access)?;
                let bounds = port.output.log.read(access)?.bounds()?;
                if cursor < bounds.start || cursor > bounds.end {
                    return Err(StationError::ConsumerCursorOutOfRange {
                        consumer: port.consumer_slot,
                        offset: cursor,
                        head: bounds.start,
                        tail: bounds.end,
                    });
                }
                Ok(InputStatus {
                    cursor,
                    tail: bounds.end,
                })
            })
            .collect::<Result<Vec<_>, StationError>>()?;
        Ok((Some(active), inputs))
    }

    pub(super) const fn new(
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        ports: Vec<InputPort>,
    ) -> Self {
        Self {
            state,
            ports,
            claim: None,
        }
    }

    pub(super) const fn is_input_free(&self) -> bool {
        self.ports.is_empty()
    }

    pub(super) const fn claim(&self) -> Option<&Claim> {
        self.claim.as_ref()
    }

    pub(super) fn complete(&self, access: TransactionAccess<'_>) -> Result<(), StationError> {
        let claim = self
            .claim
            .as_ref()
            .expect("an input completion requires an offered claim");
        let durable_active = read_active_input(&self.state, self.ports.len(), access)?;
        validate_claim_active(claim.port, durable_active)?;
        let port = &self.ports[claim.port];
        let (bounds, cursors) = port.output.state(access)?;
        let plan = plan_complete(
            claim.port,
            claim.offset,
            durable_active,
            NonZeroUsize::new(self.ports.len()).expect("a claimed Inbox has input ports"),
            port.consumer_slot,
            bounds,
            &cursors,
        )?;
        let mut state = self.state.access(access)?;
        state.put(
            &cursor_key(claim.port),
            &encode_cursor(plan.next_cursor).to_vec(),
        )?;
        state.put(
            &ACTIVE_INPUT_KEY.to_vec(),
            &encode_active_input(plan.next_active).to_vec(),
        )?;
        if let Some(target) = plan.reclaim_to {
            let actual = port
                .output
                .log
                .access(access)?
                .truncate_before(target, RECLAIM_ONE)?;
            if actual != target {
                return Err(StationError::RetentionTruncateMismatch { target, actual });
            }
        }
        Ok(())
    }

    pub(super) fn clear_claim(&mut self) {
        self.claim = None;
    }

    #[cfg(test)]
    pub(super) fn ports(&self) -> &[InputPort] {
        &self.ports
    }

    #[cfg(test)]
    pub(super) const fn state(&self) -> &OrderedMap<Vec<u8>, Vec<u8>, Small> {
        &self.state
    }

    #[cfg(test)]
    pub(super) fn cached_claim(&self) -> Option<&Claim> {
        self.claim.as_ref()
    }

    #[cfg(test)]
    pub(super) fn clear_cached_claim(&mut self) {
        self.claim = None;
    }
}

impl InputPort {
    #[cfg(test)]
    pub(super) const fn output(&self) -> &Arc<Output> {
        &self.output
    }
}

impl ConsumerCursor {
    pub(crate) const fn new(
        state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
        input: usize,
    ) -> Self {
        Self { state, input }
    }

    fn read(&self, consumer: usize, access: TransactionAccess<'_>) -> Result<u64, StationError> {
        let state = self.state.access(access)?;
        Self::decode(consumer, state.get(&cursor_key(self.input))?)
    }

    fn read_snapshot(
        &self,
        consumer: usize,
        access: ReadTransactionAccess<'_>,
    ) -> Result<u64, StationError> {
        let state = self.state.read(access)?;
        Self::decode(consumer, state.get(&cursor_key(self.input))?)
    }

    fn decode(consumer: usize, encoded: Option<Vec<u8>>) -> Result<u64, StationError> {
        let encoded = encoded.ok_or(StationError::MissingConsumerCursor { consumer })?;
        decode_cursor(&encoded).ok_or(StationError::MalformedConsumerCursor { consumer })
    }
}

impl Output {
    pub(super) fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<OutputStatus, StationError> {
        let log = self.log.read(access)?;
        let bounds = log.bounds()?;
        Ok(OutputStatus {
            head: bounds.start,
            tail: bounds.end,
            retained_bytes: log.retained_bytes()?,
            capacity_bytes: self.capacity_bytes.get(),
        })
    }

    pub(super) fn new(
        log: AppendLog<Vec<u8>>,
        capacity_bytes: NonZeroU64,
        schema: SchemaRef,
        consumers: Vec<ConsumerCursor>,
    ) -> Self {
        assert!(
            !consumers.is_empty(),
            "a validated producer output must have at least one consumer edge"
        );
        Self {
            log,
            capacity_bytes,
            schema,
            consumers: consumers.into_boxed_slice(),
        }
    }

    pub(crate) fn port(self: &Arc<Self>, consumer_slot: usize) -> InputPort {
        assert!(
            consumer_slot < self.consumers.len(),
            "an assembled input port must reference its producer consumer slot"
        );
        InputPort {
            output: Arc::clone(self),
            consumer_slot,
        }
    }

    pub(super) fn validate_snapshot(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StationError> {
        let bounds = self.log.read(access)?.bounds()?;
        let cursors = self
            .consumers
            .iter()
            .enumerate()
            .map(|(consumer, cursor)| cursor.read_snapshot(consumer, access))
            .collect::<Result<Vec<_>, _>>()?;
        validate_state(&bounds, &cursors)
    }

    pub(super) fn try_append(
        &self,
        change: &Change,
        access: TransactionAccess<'_>,
    ) -> Result<bool, StationError> {
        let actual = change.schema();
        if !schemas_match(&self.schema, &actual) {
            return Err(StationError::OutputSchemaMismatch {
                expected: Arc::clone(&self.schema),
                actual,
            });
        }
        let encoded =
            encode_change(change).map_err(|source| StationError::InvalidOutputChange { source })?;
        Ok(self
            .log
            .access(access)?
            .try_append(&encoded, self.capacity_bytes)?
            .is_some())
    }

    fn read_one(
        &self,
        offset: u64,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<(u64, Vec<u8>)>, StationError> {
        let mut selected = None;
        self.log.read(access)?.scan(
            offset,
            ScanLimit::new(1, usize::MAX)?,
            |entry| -> Result<(), StationError> {
                selected = Some((entry.offset(), entry.decode_owned()?));
                Ok(())
            },
        )?;
        Ok(selected)
    }

    fn state(&self, access: TransactionAccess<'_>) -> Result<(Range<u64>, Vec<u64>), StationError> {
        let bounds = self.log.access(access)?.bounds()?;
        let cursors = self
            .consumers
            .iter()
            .enumerate()
            .map(|(consumer, cursor)| cursor.read(consumer, access))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((bounds, cursors))
    }

    #[cfg(test)]
    pub(super) const fn log(&self) -> &AppendLog<Vec<u8>> {
        &self.log
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Inbox {
    /// Idempotently loads at most one durable input Claim into the Inbox.
    pub(super) fn intake(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<bool, StationError> {
        if self.claim.is_some() || self.ports.is_empty() {
            return Ok(false);
        }

        let (active, selected) = {
            let transaction = reads.begin()?;
            let state = self.state.read(transaction.access())?;
            let encoded = state
                .get(&ACTIVE_INPUT_KEY.to_vec())?
                .ok_or(StationError::MissingActiveInput)?;
            let active = decode_active_input(&encoded).ok_or(StationError::MalformedActiveInput)?;
            if active >= self.ports.len() {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count: self.ports.len(),
                });
            }

            let mut selected = None;
            for index in (active..self.ports.len()).chain(0..active) {
                let encoded = state
                    .get(&cursor_key(index))?
                    .ok_or(StationError::MissingCursor { input: index })?;
                let offset = decode_cursor(&encoded)
                    .ok_or(StationError::MalformedCursor { input: index })?;
                if let Some((offset, encoded)) = self.ports[index]
                    .output
                    .read_one(offset, transaction.access())?
                {
                    selected = Some(EncodedClaim {
                        port: index,
                        offset,
                        encoded,
                    });
                    break;
                }
            }
            (active, selected)
        };

        let Some(selected) = selected else {
            return Ok(false);
        };
        let change = decode_change(&selected.encoded).map_err(|source| {
            StationError::InvalidInputChange {
                input: selected.port,
                source,
            }
        })?;
        let actual = change.schema();
        let expected = self.ports[selected.port].output.schema();
        if !schemas_match(expected, &actual) {
            return Err(StationError::InputSchemaMismatch {
                input: selected.port,
                expected: Arc::clone(expected),
                actual,
            });
        }
        let pinned = selected.port != active;
        if pinned {
            let transaction = transactions.begin()?;
            self.state.access(transaction.access())?.put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(selected.port).to_vec(),
            )?;
            transaction.commit()?;
        }
        self.claim = Some(Claim {
            port: selected.port,
            offset: selected.offset,
            change,
        });
        Ok(pinned)
    }
}

fn schemas_match(expected: &SchemaRef, actual: &SchemaRef) -> bool {
    Arc::ptr_eq(expected, actual) || expected.as_ref() == actual.as_ref()
}

pub(super) fn plan_complete(
    claim_port: usize,
    claim_offset: u64,
    durable_active: usize,
    input_count: NonZeroUsize,
    consumer_slot: usize,
    bounds: Range<u64>,
    cursors: &[u64],
) -> Result<CompletionPlan, StationError> {
    validate_claim_active(claim_port, durable_active)?;
    validate_state(&bounds, cursors)?;
    let current = cursors
        .get(consumer_slot)
        .expect("an assembled input port must reference its producer consumer slot");
    if *current != claim_offset {
        return Err(StationError::ClaimCursorMismatch {
            claimed: claim_offset,
            durable: *current,
        });
    }
    if claim_offset == bounds.end {
        return Err(StationError::ClaimAtTail {
            offset: claim_offset,
            tail: bounds.end,
        });
    }
    let next_cursor = claim_offset
        .checked_add(1)
        .expect("an AppendLog entry offset before tail always has a successor");
    let target = cursors
        .iter()
        .enumerate()
        .map(|(slot, cursor)| {
            if slot == consumer_slot {
                next_cursor
            } else {
                *cursor
            }
        })
        .min()
        .expect("a validated producer has at least one consumer");
    debug_assert!(target == bounds.start || target == bounds.start + 1);
    Ok(CompletionPlan {
        next_cursor,
        next_active: (claim_port + 1) % input_count.get(),
        reclaim_to: (target != bounds.start).then_some(target),
    })
}

fn validate_claim_active(claimed: usize, durable: usize) -> Result<(), StationError> {
    if durable != claimed {
        return Err(StationError::ClaimActiveInputMismatch { claimed, durable });
    }
    Ok(())
}

fn validate_state(bounds: &Range<u64>, cursors: &[u64]) -> Result<(), StationError> {
    for (consumer, offset) in cursors.iter().copied().enumerate() {
        if offset < bounds.start || offset > bounds.end {
            return Err(StationError::ConsumerCursorOutOfRange {
                consumer,
                offset,
                head: bounds.start,
                tail: bounds.end,
            });
        }
    }
    let minimum = *cursors
        .iter()
        .min()
        .expect("a validated producer has at least one consumer");
    if minimum != bounds.start {
        return Err(StationError::RetentionHeadMismatch {
            head: bounds.start,
            minimum,
        });
    }
    Ok(())
}

fn read_active_input(
    state: &OrderedMap<Vec<u8>, Vec<u8>, Small>,
    input_count: usize,
    access: TransactionAccess<'_>,
) -> Result<usize, StationError> {
    let encoded = state
        .access(access)?
        .get(&ACTIVE_INPUT_KEY.to_vec())?
        .ok_or(StationError::MissingActiveInput)?;
    let active = decode_active_input(&encoded).ok_or(StationError::MalformedActiveInput)?;
    if active >= input_count {
        return Err(StationError::ActiveInputOutOfRange {
            input: active,
            input_count,
        });
    }
    Ok(active)
}

pub(super) fn encode_active_input(input: usize) -> [u8; size_of::<u32>()] {
    u32::try_from(input)
        .expect("validated input count fits the Flow format")
        .to_be_bytes()
}

pub(super) fn decode_active_input(encoded: &[u8]) -> Option<usize> {
    let input = u32::from_be_bytes(encoded.try_into().ok()?);
    usize::try_from(input).ok()
}

pub(super) fn cursor_key(index: usize) -> Vec<u8> {
    let index = u32::try_from(index).expect("validated input count fits the Flow format");
    format!("input/{index:08x}/cursor").into_bytes()
}

pub(super) const fn encode_cursor(offset: u64) -> [u8; size_of::<u64>()] {
    offset.to_be_bytes()
}

pub(super) fn decode_cursor(encoded: &[u8]) -> Option<u64> {
    encoded.try_into().ok().map(u64::from_be_bytes)
}
