//! Durable buffering and delivery protocol shared by external sinks.

mod batch;
mod runtime;
mod state;

use std::{fmt, num::NonZeroU32};

use dogpaddle_store::{Cell, OrderedMap, Store, StoreSetup};

use crate::{
    operation::{Operation, OperationError},
    setup::{OperationSetupError, create_data, open_data},
};

pub(crate) use batch::DeliveryBatch;
pub(crate) use runtime::BufferedSink;

pub(crate) const CONTROL: &str = "sink.control";
pub(crate) const BUFFER: &str = "sink.buffer";

pub(crate) fn create<T: SinkTarget>(
    schema: arrow_schema::SchemaRef,
    target: T,
    setup: &mut StoreSetup,
    prefix: &str,
) -> Result<Operation, OperationSetupError> {
    let control = create_data::<Cell<Vec<u8>>>(setup, prefix, CONTROL)?;
    let buffer = create_data::<OrderedMap<u64, Vec<u8>>>(setup, prefix, BUFFER)?;
    Ok(Operation::Turn(Box::new(BufferedSink::new(
        schema, target, control, buffer,
    ))))
}
pub(crate) fn open<T: SinkTarget>(
    schema: arrow_schema::SchemaRef,
    target: T,
    store: &Store,
    prefix: &str,
) -> Result<Operation, OperationSetupError> {
    let control = open_data::<Cell<Vec<u8>>>(store, prefix, CONTROL)?;
    let buffer = open_data::<OrderedMap<u64, Vec<u8>>>(store, prefix, BUFFER)?;
    Ok(Operation::Turn(Box::new(BufferedSink::new(
        schema, target, control, buffer,
    ))))
}
pub(crate) const MAX_TARGET_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// Target-specific part of the durable sink protocol.
///
/// The shared runtime owns input buffering, batch boundaries, pacing and
/// settlement. An adapter owns target initialization, planning, delivery and
/// the stable codecs for its small checkpoint and prepared plan.
pub(crate) trait SinkTarget: Send + 'static {
    type Checkpoint: Clone + Send + 'static;
    type Plan: Clone + Send + 'static;

    /// A backend safety ceiling combined with the shared delivery limits.
    const MAX_BATCH_EVENTS: NonZeroU32;

    /// Proves a fresh target is absent before initialization intent is durable.
    fn require_absent(&mut self) -> Result<(), OperationError>;
    /// Creates or verifies the owned empty target after intent is durable.
    ///
    /// A process can exit or receive an uncertain result after this call. The
    /// same call on reopen must therefore be idempotent and must reject a
    /// target with incompatible ownership or layout.
    fn initialize(&mut self) -> Result<(), OperationError>;
    /// Returns the stable checkpoint for the initialized empty target.
    fn initial_checkpoint(&self) -> Self::Checkpoint;

    /// Returns the deterministic target-side byte charge for one row event.
    ///
    /// This method must be pure, must not perform target I/O, and must return a
    /// nonzero value. The shared runtime calls it before input acknowledgement
    /// and while slicing durable input so one target transaction cannot expand
    /// a compact Change into unbounded work.
    fn event_bytes(
        &self,
        input: &dogpaddle_change::Change,
        row_index: usize,
    ) -> Result<u64, OperationError>;

    /// Rejects input that cannot eventually be represented from this target
    /// checkpoint, before the shared buffer acknowledges it.
    fn validate_admission(
        &self,
        input: &dogpaddle_change::Change,
        checkpoint: &Self::Checkpoint,
        buffered_events: u64,
    ) -> Result<(), OperationError>;

    /// Validates target-specific capacity for all positive events remaining
    /// after a recovered checkpoint, before any target I/O is attempted.
    fn validate_recovery(
        &self,
        checkpoint: &Self::Checkpoint,
        remaining_positive_events: u64,
    ) -> Result<(), OperationError>;

    /// Plans one exact bounded delivery without changing the target.
    ///
    /// The same batch and checkpoint can be prepared again after a
    /// local rollback or process exit. Any target session poisoned by a failed
    /// read must be reset before returning an error.
    fn prepare(
        &mut self,
        input: &DeliveryBatch,
        checkpoint: &Self::Checkpoint,
    ) -> Result<(Self::Checkpoint, Self::Plan), OperationError>;

    /// Atomically and idempotently confirms one durably prepared delivery.
    ///
    /// Reopen repeats this call after process exit, an explicit error, or an
    /// uncertain target commit. Repeating the exact durable plan must be a
    /// no-op success; adapters use the plan's fixed mutation identities to
    /// recognize that replay.
    fn deliver(&mut self, input: &DeliveryBatch, plan: &Self::Plan) -> Result<(), OperationError>;

    /// Appends one stable, self-delimiting checkpoint encoding.
    fn encode_checkpoint(checkpoint: &Self::Checkpoint, output: &mut Vec<u8>);
    /// Consumes exactly one checkpoint from the control-state suffix.
    fn decode_checkpoint(input: &mut &[u8]) -> Result<Self::Checkpoint, OperationError>;
    /// Appends one stable prepared-plan encoding.
    fn encode_plan(plan: &Self::Plan, output: &mut Vec<u8>);
    /// Consumes and validates one plan against its exact reconstructed batch.
    fn decode_plan(
        input: &mut &[u8],
        change: &DeliveryBatch,
        checkpoint: &Self::Checkpoint,
    ) -> Result<Self::Plan, OperationError>;
}

#[derive(Debug)]
struct BufferedSinkError(String);

impl fmt::Display for BufferedSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "buffered sink: {}", self.0)
    }
}

impl std::error::Error for BufferedSinkError {}

fn invalid(message: impl Into<String>) -> OperationError {
    Box::new(BufferedSinkError(message.into()))
}
