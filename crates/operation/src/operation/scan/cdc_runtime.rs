//! Source-owned sealed bootstrap and durable published input queue.
use crate::operation::{OperationError, SourceOperation};
use arrow_schema::SchemaRef;
use dogpaddle_change::{Change, CodecError, SchemaBoundChangeCodec};
use dogpaddle_debezium::{Checkpoint, Connector, Delivery, Record};
use dogpaddle_store::{Cell, Queue, ReadTransactionAccess, TransactionAccess};
use std::{num::NonZeroU64, time::Duration};
const MAX_CAPTURE_BYTES: usize = 8 * 1024 * 1024;
const PUBLISHED_BYTES: u64 = 64 * 1024 * 1024;
const CAPTURING: u32 = 1;
const PUBLISHING: u32 = 2;
const STREAMING: u32 = 3;
const RESETTING: u32 = 4;
const RESET_BATCH_ENTRIES: usize = 256;

const STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Fresh,
    Capturing,
    Publishing,
    Streaming,
    Resetting,
}

impl Phase {
    fn decode<B: Source>(value: Option<u32>) -> Result<Self, OperationError> {
        match value {
            None => Ok(Self::Fresh),
            Some(CAPTURING) => Ok(Self::Capturing),
            Some(PUBLISHING) => Ok(Self::Publishing),
            Some(STREAMING) => Ok(Self::Streaming),
            Some(RESETTING) => Ok(Self::Resetting),
            Some(_) => Err(B::invalid_state("CDC scan phase is invalid")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NextStep {
    Restore,
    BeginCapture,
    Capture,
    PrepareReset,
    Reset,
    Publish,
    Stream,
}

pub(super) struct Captured<P> {
    pub(super) change: Option<Change>,
    pub(super) sealed: bool,
    pub(super) progress: P,
}

/// Only database-specific work lives here; the runtime owns Connector and typed capture state; Flow owns transactions.
pub(super) trait Source: Send + 'static {
    type Progress: Copy + Default + Send + 'static;
    /// `PostgreSQL` must remove its abandoned slot before durably entering reset.
    const RESET_REQUIRES_SOURCE_CLEANUP: bool;
    fn source_fields(&self) -> usize;
    fn data_envelopes(&self, records: &[Record]) -> usize {
        records.len()
    }
    fn start_snapshot(&self) -> Result<Connector, OperationError>;
    fn start_streaming(&self, checkpoint: &Checkpoint) -> Result<Connector, OperationError>;
    fn cleanup_snapshot(&self) -> Result<(), OperationError>;
    fn capture(
        &self,
        schema: SchemaRef,
        records: &[Record],
        progress: Self::Progress,
    ) -> Result<Captured<Self::Progress>, OperationError>;
    fn stream(
        &self,
        schema: SchemaRef,
        records: &[Record],
    ) -> Result<Option<Change>, OperationError>;
    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        spool_empty: bool,
    ) -> Result<Option<Checkpoint>, OperationError>;
    fn invalid_state(message: &'static str) -> OperationError;
    fn runtime_error(message: String) -> OperationError;
    fn spool_full() -> OperationError;
    fn codec_error(error: dogpaddle_change::CodecError) -> OperationError;
}

/// Original linear delivery plus its bounded capture, or concrete source maintenance.
pub struct SourceDelivery {
    pub(crate) kind: DeliveryKind,
    finished: bool,
}
pub(crate) enum DeliveryKind {
    BeginCapture,
    Reset,
    Publish,
    Cdc {
        delivery: Delivery,
        encoded: Option<Vec<u8>>,
        sealed: bool,
        streaming: bool,
    },
    Sequence {
        next: u64,
        encoded: Vec<u8>,
    },
}
impl SourceDelivery {
    /// Reports whether completion acknowledges a real external source delivery.
    ///
    /// This delivery requires a durable WAL barrier before `SourceOperation::ack`.
    /// Other source maintenance still commits before completion and is durable
    /// before the caller returns from its scheduling round.
    #[must_use]
    pub fn requires_ack_barrier(&self) -> bool {
        matches!(self.kind, DeliveryKind::Cdc { .. })
    }
    pub(crate) fn sequence(next: u64, encoded: Vec<u8>) -> Self {
        Self {
            kind: DeliveryKind::Sequence { next, encoded },
            finished: false,
        }
    }
}
pub(super) struct CdcRuntime<B: Source> {
    source: B,
    pub(super) codec: SchemaBoundChangeCodec,
    phase_cell: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    spool: Queue<Vec<u8>>,
    published: Queue<Vec<u8>>,
    capacity: NonZeroU64,
    pub(super) next_step: NextStep,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    progress: B::Progress,
    pending_progress: Option<B::Progress>,
}
impl<B: Source> CdcRuntime<B> {
    pub(super) fn new(
        source: B,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: Queue<Vec<u8>>,
        published: Queue<Vec<u8>>,
        capacity: NonZeroU64,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            source,
            codec: SchemaBoundChangeCodec::try_new(output_schema)?,
            phase_cell,
            checkpoint,
            spool,
            published,
            capacity,
            next_step: NextStep::Restore,
            resume: None,
            connector: None,
            progress: B::Progress::default(),
            pending_progress: None,
        })
    }
    fn stop_connector(&mut self, stage: &str) -> Result<(), OperationError> {
        if let Some(connector) = self.connector.as_mut() {
            connector.stop(STOP_TIMEOUT).map_err(|error| {
                B::runtime_error(format!("Debezium {stage} stop failed ({:?})", error.kind()))
            })?;
        }
        self.connector = None;
        Ok(())
    }
    fn poll_delivery(&mut self, streaming: bool) -> Result<Option<SourceDelivery>, OperationError> {
        if self.connector.is_none() {
            self.connector = Some(if streaming {
                self.source.start_streaming(
                    self.resume
                        .as_ref()
                        .ok_or_else(|| B::invalid_state("streaming source has no checkpoint"))?,
                )?
            } else {
                self.source.start_snapshot()?
            });
        }
        let Some(delivery) = self
            .connector
            .as_mut()
            .expect("connector started")
            .poll(Duration::ZERO)
            .map_err(|error| {
                B::runtime_error(format!("Debezium poll failed ({:?})", error.kind()))
            })?
        else {
            return Ok(None);
        };
        // Account every envelope before conversion; an update can produce two physical rows.
        let envelopes = self.source.data_envelopes(delivery.records());
        if envelopes > 2048
            || envelopes
                .saturating_mul(2)
                .saturating_mul(self.source.source_fields())
                > 65_536
        {
            return Err(B::invalid_state(
                "source delivery exceeds envelope or scalar-slot admission",
            ));
        }
        let checkpoint_bytes = delivery.checkpoint().as_bytes().len();
        let remaining = MAX_CAPTURE_BYTES
            .checked_sub(checkpoint_bytes)
            .ok_or_else(|| B::invalid_state("source checkpoint exceeds capture admission"))?;
        let (change, sealed) = if streaming {
            (
                self.source
                    .stream(self.codec.schema(), delivery.records())?,
                false,
            )
        } else {
            let captured =
                self.source
                    .capture(self.codec.schema(), delivery.records(), self.progress)?;
            self.pending_progress = Some(captured.progress);
            (captured.change, captured.sealed)
        };
        if change
            .as_ref()
            .is_some_and(|change| change.num_rows() > 4096)
        {
            return Err(B::invalid_state(
                "source delivery exceeds physical-row admission",
            ));
        }
        let encoded = change
            .as_ref()
            .map(|change| self.codec.encode_bounded(change, remaining))
            .transpose()
            .map_err(B::codec_error)?;
        Ok(Some(SourceDelivery {
            kind: DeliveryKind::Cdc {
                delivery,
                encoded,
                sealed,
                streaming,
            },
            finished: false,
        }))
    }
}
impl<B: Source> SourceOperation for CdcRuntime<B> {
    fn restore(&mut self, access: ReadTransactionAccess<'_>) -> Result<(), OperationError> {
        if self.next_step != NextStep::Restore {
            return Ok(());
        }
        let phase = Phase::decode::<B>(self.phase_cell.read(access)?.get_bounded(4)?)?;
        let checkpoint = self
            .checkpoint
            .read(access)?
            .get_bounded(MAX_CAPTURE_BYTES)?;
        let spool = self.spool.read(access)?;
        if spool.queued_bytes()? > self.capacity.get()
            || self.published.read(access)?.queued_bytes()? > PUBLISHED_BYTES
        {
            return Err(B::invalid_state("source queue accounting exceeds capacity"));
        }
        let empty = spool.is_empty()?;
        if (phase == Phase::Fresh && (checkpoint.is_some() || !empty))
            || (phase == Phase::Streaming && !empty)
            || (phase == Phase::Capturing && !empty && checkpoint.is_none())
        {
            return Err(B::invalid_state("CDC phase and bootstrap data disagree"));
        }
        self.resume = self.source.restore_checkpoint(phase, checkpoint, empty)?;
        self.next_step = match phase {
            Phase::Fresh => NextStep::BeginCapture,
            Phase::Capturing => NextStep::PrepareReset,
            Phase::Resetting => NextStep::Reset,
            Phase::Publishing => NextStep::Publish,
            Phase::Streaming => NextStep::Stream,
        };
        Ok(())
    }
    fn poll(&mut self) -> Result<Option<SourceDelivery>, OperationError> {
        let kind = match self.next_step {
            NextStep::Restore => {
                return Err(B::invalid_state("source restore must precede polling"));
            }
            NextStep::BeginCapture => DeliveryKind::BeginCapture,
            NextStep::PrepareReset => {
                self.stop_connector("snapshot")?;
                if B::RESET_REQUIRES_SOURCE_CLEANUP {
                    self.source.cleanup_snapshot()?;
                }
                DeliveryKind::Reset
            }
            NextStep::Reset => DeliveryKind::Reset,
            NextStep::Publish => {
                self.stop_connector("snapshot")?;
                DeliveryKind::Publish
            }
            NextStep::Capture => return self.poll_delivery(false),
            NextStep::Stream => return self.poll_delivery(true),
        };
        Ok(Some(SourceDelivery {
            kind,
            finished: false,
        }))
    }
    fn record(
        &self,
        access: TransactionAccess<'_>,
        capture: &mut SourceDelivery,
    ) -> Result<bool, OperationError> {
        match &capture.kind {
            DeliveryKind::BeginCapture => self.phase_cell.access(access)?.set(&CAPTURING)?,
            DeliveryKind::Reset => {
                self.phase_cell.access(access)?.set(&RESETTING)?;
                capture.finished = self
                    .spool
                    .access(access)?
                    .discard_front(RESET_BATCH_ENTRIES)?;
                if capture.finished {
                    self.checkpoint.access(access)?.clear()?;
                    self.phase_cell.access(access)?.clear()?;
                }
            }
            DeliveryKind::Publish => {
                // Pop and publish are one transaction. Capacity rejection requires rollback.
                if let Some((encoded, empty_after)) = self
                    .spool
                    .access(access)?
                    .pop_front_bounded(8 * 1024 * 1024)?
                {
                    if !self.published.access(access)?.try_push(
                        &encoded,
                        NonZeroU64::new(PUBLISHED_BYTES).expect("nonzero capacity"),
                    )? {
                        return Ok(false);
                    }
                    capture.finished = empty_after;
                } else {
                    capture.finished = true;
                }
                if capture.finished {
                    self.phase_cell.access(access)?.set(&STREAMING)?;
                }
            }
            DeliveryKind::Cdc {
                delivery,
                encoded,
                sealed,
                streaming,
            } => {
                if let Some(encoded) = encoded {
                    if *streaming {
                        if !self.published.access(access)?.try_push(
                            encoded,
                            NonZeroU64::new(PUBLISHED_BYTES).expect("nonzero capacity"),
                        )? {
                            return Ok(false);
                        }
                    } else if !self
                        .spool
                        .access(access)?
                        .try_push(encoded, self.capacity)?
                    {
                        return Err(B::spool_full());
                    }
                }
                self.checkpoint
                    .access(access)?
                    .set(&delivery.checkpoint().as_bytes().to_vec())?;
                if *sealed {
                    self.phase_cell.access(access)?.set(&PUBLISHING)?;
                }
            }
            DeliveryKind::Sequence { .. } => {
                return Err(B::invalid_state("sequence delivery supplied to CDC source"));
            }
        }
        Ok(true)
    }
    fn ack(&mut self, capture: SourceDelivery) -> Result<(), OperationError> {
        match capture.kind {
            DeliveryKind::BeginCapture => {
                self.next_step = NextStep::Capture;
                self.progress = B::Progress::default();
            }
            DeliveryKind::Reset => {
                self.next_step = if capture.finished {
                    NextStep::BeginCapture
                } else {
                    NextStep::Reset
                };
                self.resume = None;
                self.progress = B::Progress::default();
            }
            DeliveryKind::Publish => {
                if capture.finished {
                    self.next_step = NextStep::Stream;
                }
            }
            DeliveryKind::Cdc {
                delivery,
                sealed,
                streaming,
                ..
            } => {
                let resumed = delivery.checkpoint().clone();
                self.connector
                    .as_mut()
                    .ok_or_else(|| B::invalid_state("delivery connector is absent"))?
                    .ack(delivery)
                    .map_err(|error| {
                        B::runtime_error(format!("Debezium ACK failed ({:?})", error.kind()))
                    })?;
                self.resume = Some(resumed);
                if sealed {
                    self.next_step = NextStep::Publish;
                } else if !streaming {
                    self.progress = self
                        .pending_progress
                        .take()
                        .expect("capture progress prepared");
                }
            }
            DeliveryKind::Sequence { .. } => {
                return Err(B::invalid_state("sequence delivery supplied to CDC source"));
            }
        }
        Ok(())
    }
    fn published(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<Vec<u8>>, OperationError> {
        Ok(self
            .published
            .read(access)?
            .front_bounded(8 * 1024 * 1024)?)
    }
    fn consume_published(&self, access: TransactionAccess<'_>) -> Result<(), OperationError> {
        self.published.access(access)?.discard_front(1)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
