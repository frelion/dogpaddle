use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, ScanLimit, Small, TransactionAccess,
    Transactions,
};

use super::{protocol::StationError, runtime::Station};

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

/// One input edge and its acknowledgement capability.
pub(crate) struct InputPort {
    log: ReadOnly<AppendLog<Vec<u8>>>,
    retention: Arc<OutputRetention>,
    consumer_slot: usize,
}

/// A Station's complete input-delivery state.
pub(super) struct Inbox {
    ports: Vec<InputPort>,
    claim: Option<Claim>,
}

/// A read capability for one consumer edge's durable frontier.
pub(crate) struct ConsumerCursor {
    state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
    input: usize,
}

/// Shared acknowledgement and physical-retention state for one producer output.
pub(crate) struct OutputRetention {
    output: AppendLog<Vec<u8>>,
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

impl InputPort {
    pub(crate) fn new(
        log: ReadOnly<AppendLog<Vec<u8>>>,
        retention: Arc<OutputRetention>,
        consumer_slot: usize,
    ) -> Self {
        Self {
            log,
            retention,
            consumer_slot,
        }
    }
}

impl Inbox {
    pub(super) const fn new(ports: Vec<InputPort>) -> Self {
        Self { ports, claim: None }
    }

    pub(super) const fn is_input_free(&self) -> bool {
        self.ports.is_empty()
    }

    pub(super) const fn claim(&self) -> Option<&Claim> {
        self.claim.as_ref()
    }

    pub(super) fn complete(
        &self,
        state: &OrderedMap<Vec<u8>, Vec<u8>, Small>,
        access: TransactionAccess<'_>,
    ) -> Result<(), StationError> {
        let claim = self
            .claim
            .as_ref()
            .expect("an input completion requires an offered claim");
        let durable_active = read_active_input(state, self.ports.len(), access)?;
        if durable_active != claim.port {
            return Err(StationError::ClaimActiveInputMismatch {
                claimed: claim.port,
                durable: durable_active,
            });
        }

        let port = &self.ports[claim.port];
        let (head, target) =
            port.retention
                .target_after(port.consumer_slot, claim.offset, access)?;
        let next_offset = claim
            .offset
            .checked_add(1)
            .expect("an AppendLog entry offset always has a successor");
        state.access(access)?.put(
            &cursor_key(claim.port),
            &encode_cursor(next_offset).to_vec(),
        )?;
        let next_port = if claim.port + 1 == self.ports.len() {
            0
        } else {
            claim.port + 1
        };
        state.access(access)?.put(
            &ACTIVE_INPUT_KEY.to_vec(),
            &encode_active_input(next_port).to_vec(),
        )?;
        port.retention.reclaim(head, target, access)
    }

    pub(super) fn clear_claim(&mut self) {
        self.claim = None;
    }

    #[cfg(test)]
    pub(super) fn ports(&self) -> &[InputPort] {
        &self.ports
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

impl ConsumerCursor {
    pub(crate) const fn new(
        state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
        input: usize,
    ) -> Self {
        Self { state, input }
    }

    fn read(&self, consumer: usize, access: TransactionAccess<'_>) -> Result<u64, StationError> {
        let state = self.state.access(access)?;
        let encoded = state
            .get(&cursor_key(self.input))?
            .ok_or(StationError::MissingConsumerCursor { consumer })?;
        decode_cursor(&encoded).ok_or(StationError::MalformedConsumerCursor { consumer })
    }
}

impl OutputRetention {
    pub(crate) fn new(output: AppendLog<Vec<u8>>, consumers: Vec<ConsumerCursor>) -> Self {
        assert!(
            !consumers.is_empty(),
            "a validated producer output must have at least one consumer edge"
        );
        Self {
            output,
            consumers: consumers.into_boxed_slice(),
        }
    }

    pub(crate) fn validate(&self, access: TransactionAccess<'_>) -> Result<(), StationError> {
        self.validated_state(access).map(|_| ())
    }

    fn target_after(
        &self,
        consumer_slot: usize,
        expected_offset: u64,
        access: TransactionAccess<'_>,
    ) -> Result<(u64, u64), StationError> {
        let (bounds, mut cursors) = self.validated_state(access)?;
        let current = cursors
            .get_mut(consumer_slot)
            .expect("an assembled input port must reference its producer consumer slot");
        if *current != expected_offset {
            return Err(StationError::ClaimCursorMismatch {
                claimed: expected_offset,
                durable: *current,
            });
        }
        if expected_offset == bounds.end {
            return Err(StationError::ClaimAtTail {
                offset: expected_offset,
                tail: bounds.end,
            });
        }
        *current = expected_offset
            .checked_add(1)
            .expect("an AppendLog entry offset always has a successor");
        let target = *cursors
            .iter()
            .min()
            .expect("a validated producer has at least one consumer");
        let next_head = bounds
            .start
            .checked_add(1)
            .expect("a completable retained head always has a successor");
        if target > next_head {
            return Err(StationError::RetentionTargetJump {
                head: bounds.start,
                target,
            });
        }
        Ok((bounds.start, target))
    }

    fn validated_state(
        &self,
        access: TransactionAccess<'_>,
    ) -> Result<(Range<u64>, Vec<u64>), StationError> {
        let bounds = self.output.access(access)?.bounds()?;
        let mut cursors = Vec::with_capacity(self.consumers.len());
        for (consumer, cursor) in self.consumers.iter().enumerate() {
            let offset = cursor.read(consumer, access)?;
            if offset < bounds.start || offset > bounds.end {
                return Err(StationError::ConsumerCursorOutOfRange {
                    consumer,
                    offset,
                    head: bounds.start,
                    tail: bounds.end,
                });
            }
            cursors.push(offset);
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
        Ok((bounds, cursors))
    }

    fn reclaim(
        &self,
        head: u64,
        target: u64,
        access: TransactionAccess<'_>,
    ) -> Result<(), StationError> {
        if target == head {
            return Ok(());
        }
        let actual = self
            .output
            .access(access)?
            .truncate_before(target, RECLAIM_ONE)?;
        if actual != target {
            return Err(StationError::RetentionTruncateMismatch { target, actual });
        }
        Ok(())
    }
}

impl Station {
    /// Idempotently loads at most one durable input Claim into the Inbox.
    pub(crate) fn intake(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<bool, StationError> {
        if self.inbox.claim.is_some() || self.inbox.ports.is_empty() {
            return Ok(false);
        }

        let (active, selected) = {
            let transaction = reads.begin()?;
            let state = self.state.read(transaction.access())?;
            let encoded = state
                .get(&ACTIVE_INPUT_KEY.to_vec())?
                .ok_or(StationError::MissingActiveInput)?;
            let active = decode_active_input(&encoded).ok_or(StationError::MalformedActiveInput)?;
            if active >= self.inbox.ports.len() {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count: self.inbox.ports.len(),
                });
            }

            let mut selected = None;
            for index in (active..self.inbox.ports.len()).chain(0..active) {
                let encoded = state
                    .get(&cursor_key(index))?
                    .ok_or(StationError::MissingCursor { input: index })?;
                let offset = decode_cursor(&encoded)
                    .ok_or(StationError::MalformedCursor { input: index })?;
                let input_log = self.inbox.ports[index].log.read(transaction.access())?;
                input_log.scan(
                    offset,
                    ScanLimit::new(1, usize::MAX)?,
                    |entry| -> Result<(), StationError> {
                        selected = Some(EncodedClaim {
                            port: index,
                            offset: entry.offset(),
                            encoded: entry.decode_owned()?,
                        });
                        Ok(())
                    },
                )?;
                if selected.is_some() {
                    break;
                }
            }
            (active, selected)
        };

        let Some(selected) = selected else {
            return Ok(false);
        };
        let pinned = selected.port != active;
        if pinned {
            let transaction = transactions.begin()?;
            self.state.access(transaction.access())?.put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(selected.port).to_vec(),
            )?;
            transaction.commit()?;
        }
        let change = decode_change(&selected.encoded).map_err(|source| {
            StationError::InvalidInputChange {
                input: selected.port,
                source,
            }
        })?;
        self.inbox.claim = Some(Claim {
            port: selected.port,
            offset: selected.offset,
            change,
        });
        Ok(pinned)
    }
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
