//! Durable buffering and delivery protocol shared by external sinks.

mod batch;
mod runtime;
mod state;

use std::{fmt, num::NonZeroU32};

use dogpaddle_change::Change;
use dogpaddle_store::{Cell, OrderedMap};

use crate::{
    DataDeclaration,
    definition::DataName,
    operation::OperationError,
};

pub(crate) use runtime::BufferedSink;

pub(crate) const CONTROL: DataName<Cell<Vec<u8>>> = DataName::new("sink.control");
pub(crate) const BUFFER: DataName<OrderedMap<u64, Vec<u8>>> = DataName::new("sink.buffer");
pub(crate) const DATA: &[DataDeclaration] = &[CONTROL.declaration(), BUFFER.declaration()];

/// Target-specific part of the durable sink protocol.
///
/// The shared runtime owns input buffering, batch boundaries, pacing and
/// settlement. An adapter owns target initialization, planning, delivery and
/// the stable codecs for its small checkpoint and prepared plan.
pub(crate) trait SinkTarget: Send + 'static {
    type Checkpoint: Clone;
    type Plan: Clone;

    /// A backend safety ceiling applied after the user batch preference.
    const MAX_BATCH_EVENTS: NonZeroU32;

    fn require_absent(&mut self) -> Result<(), OperationError>;
    fn initialize(&mut self) -> Result<(), OperationError>;
    fn initial_checkpoint(&self) -> Self::Checkpoint;

    fn prepare(
        &mut self,
        input: &Change,
        checkpoint: &Self::Checkpoint,
        batch_id: u64,
    ) -> Result<(Self::Checkpoint, Self::Plan), OperationError>;

    fn deliver(
        &mut self,
        input: &Change,
        batch_id: u64,
        plan: &Self::Plan,
    ) -> Result<(), OperationError>;

    fn encode_checkpoint(checkpoint: &Self::Checkpoint, output: &mut Vec<u8>);
    fn decode_checkpoint(input: &mut &[u8]) -> Result<Self::Checkpoint, OperationError>;
    fn encode_plan(plan: &Self::Plan, output: &mut Vec<u8>);
    fn decode_plan(
        input: &mut &[u8],
        change: &Change,
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
