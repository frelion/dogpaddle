use std::{num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_change::{Change, decode_change, encode_change};
use dogpaddle_store::{
    Cell, ReadTransactionAccess, ReadTransactions, SubscribedLogWriter, Subscription,
    TransactionAccess, Transactions,
};

use super::protocol::StationError;
use crate::flow::{InputStatus, OutputStatus};

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
    output: Arc<Output>,
    subscription: Subscription<Vec<u8>>,
}

/// A Station's complete input-delivery state.
pub(super) struct Inbox {
    active: Option<Cell<u32>>,
    ports: Vec<InputPort>,
    claim: Option<Claim>,
}

/// One producer's append capability and exact output Schema.
pub(crate) struct Output {
    writer: SubscribedLogWriter<Vec<u8>>,
    capacity_bytes: NonZeroU64,
    schema: SchemaRef,
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
    pub(super) const fn new(active: Option<Cell<u32>>, ports: Vec<InputPort>) -> Self {
        Self {
            active,
            ports,
            claim: None,
        }
    }

    pub(super) fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(Option<usize>, Vec<InputStatus>), StationError> {
        let Some(active) = self.read_active_snapshot(access)? else {
            return Ok((None, Vec::new()));
        };
        let inputs = self
            .ports
            .iter()
            .map(|port| {
                let status = port.subscription.status(access)?;
                Ok(InputStatus {
                    position: status.position,
                    tail: status.tail,
                })
            })
            .collect::<Result<Vec<_>, StationError>>()?;
        Ok((Some(active), inputs))
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
        self.ports[claim.port]
            .subscription
            .acknowledge(claim.offset, access)?;
        if let Some(active) = &self.active {
            let next = u32::try_from((claim.port + 1) % self.ports.len())
                .expect("validated input count fits the Flow format");
            active.access(access)?.set(&next)?;
        }
        Ok(())
    }

    pub(super) fn clear_claim(&mut self) {
        self.claim = None;
    }

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
            let transaction = reads.begin();
            let active = self
                .read_active_snapshot(transaction.access())?
                .expect("a non-empty Inbox has an active input");
            let mut selected = None;
            for index in (active..self.ports.len()).chain(0..active) {
                if let Some((offset, encoded)) =
                    self.ports[index].subscription.peek(transaction.access())?
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
        let pinned = self.active.is_some() && selected.port != active;
        if pinned {
            let transaction = transactions.begin();
            let selected_port =
                u32::try_from(selected.port).expect("validated input count fits the Flow format");
            self.active
                .as_ref()
                .expect("only a multi-input Inbox durably pins its active input")
                .access(transaction.access())?
                .set(&selected_port)?;
            transaction
                .commit()
                .map_err(|source| StationError::Commit { source })?;
        }
        self.claim = Some(Claim {
            port: selected.port,
            offset: selected.offset,
            change,
        });
        Ok(pinned)
    }

    fn read_active_snapshot(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<usize>, StationError> {
        match self.ports.len() {
            0 => Ok(None),
            1 => Ok(Some(0)),
            input_count => {
                let active = self
                    .active
                    .as_ref()
                    .expect("a multi-input Inbox has durable active state")
                    .read(access)?
                    .get()?
                    .ok_or(StationError::MissingActiveInput)?;
                Ok(Some(validate_active(active, input_count)?))
            }
        }
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

impl InputPort {
    #[cfg(test)]
    pub(super) const fn output(&self) -> &Arc<Output> {
        &self.output
    }
}

impl Output {
    pub(super) const fn new(
        writer: SubscribedLogWriter<Vec<u8>>,
        capacity_bytes: NonZeroU64,
        schema: SchemaRef,
    ) -> Self {
        Self {
            writer,
            capacity_bytes,
            schema,
        }
    }

    pub(crate) fn port(self: &Arc<Self>, subscription: Subscription<Vec<u8>>) -> InputPort {
        InputPort {
            output: Arc::clone(self),
            subscription,
        }
    }

    pub(super) fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<OutputStatus, StationError> {
        let status = self.writer.status(access)?;
        Ok(OutputStatus {
            head: status.head,
            tail: status.tail,
            retained_bytes: status.retained_bytes,
            capacity_bytes: self.capacity_bytes.get(),
        })
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
            .writer
            .try_append(&encoded, self.capacity_bytes, access)?)
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

fn validate_active(active: u32, input_count: usize) -> Result<usize, StationError> {
    let active = usize::try_from(active).expect("u32 fits usize on supported Rust targets");
    if active >= input_count {
        Err(StationError::ActiveInputOutOfRange {
            input: active,
            input_count,
        })
    } else {
        Ok(active)
    }
}

fn schemas_match(expected: &SchemaRef, actual: &SchemaRef) -> bool {
    Arc::ptr_eq(expected, actual) || expected.as_ref() == actual.as_ref()
}
