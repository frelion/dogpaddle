use super::{
    Frames,
    frame::{CONTROL_BYTES, Frame, FramePhase, PAGE_BYTES},
};
use crate::{
    build::{FlowDefinition, ResolvedTopology, validate::MAX_DEPTH},
    error::FlowError,
};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_operation::operation::{
    Operation, OperationError, OperationInput, Progress, SourceDelivery,
};
use dogpaddle_store::{
    ReadTransactionAccess, ReadTransactions, ScanDirection, ScanLimit, Transactions,
};
use std::path::{Path, PathBuf};

/// A persistent logical DAG driven through one bounded durable call stack.
pub struct Flow {
    pub(super) path: PathBuf,
    pub(super) runtime: Runtime,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
}
pub(crate) struct Runtime {
    pub(crate) definition: FlowDefinition,
    pub(crate) topology: ResolvedTopology,
    pub(crate) operations: Vec<Operation>,
    pub(crate) codecs: Vec<Option<SchemaBoundChangeCodec>>,
    pub(crate) frames: Frames,
    pub(crate) sources: Vec<usize>,
    pub(crate) sinks: Vec<usize>,
    pub(crate) source_cursor: usize,
    pub(crate) sink_cursor: usize,
    pub(crate) root_cursor: usize,
    pub(crate) pending: Vec<Option<SourceDelivery>>,
    pub(crate) needs_reopen: bool,
}
impl Flow {
    pub(crate) const fn from_parts(
        path: PathBuf,
        runtime: Runtime,
        transactions: Transactions,
        reads: ReadTransactions,
    ) -> Self {
        Self {
            path,
            runtime,
            transactions,
            reads,
        }
    }
    /// Returns the Store directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Returns the number of logical Operations, including fused tails.
    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.runtime.operations.len()
    }
    /// Returns stable logical IDs in declaration order.
    #[must_use]
    pub fn operation_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.runtime
            .definition
            .operations
            .iter()
            .map(|node| node.id.as_str())
    }
}
impl Runtime {
    pub(super) fn input(
        &self,
        depth: u32,
        frame: &Frame,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Vec<u8>, OperationError> {
        if depth != 0 {
            return self.frames.output(depth - 1, access);
        }
        let Operation::Source(source) = &self.operations[frame.head] else {
            return Err("root frame is not a source".into());
        };
        source
            .published(access)?
            .ok_or_else(|| "root source has no published input".into())
    }

    pub(super) fn input_codec(&self, frame: &Frame) -> &SchemaBoundChangeCodec {
        let producer = frame.input_port.map_or(frame.head, |port| {
            self.definition.operations[frame.head].inputs[port]
        });
        self.codecs[producer]
            .as_ref()
            .expect("validated input has schema")
    }
    pub(super) fn output_codec(&self, head: usize) -> &SchemaBoundChangeCodec {
        let last = self.topology.tails[head].last().copied().unwrap_or(head);
        self.codecs[last]
            .as_ref()
            .expect("computation has output schema")
    }
    pub(crate) fn restore(&mut self, access: ReadTransactionAccess<'_>) -> Result<(), FlowError> {
        self.validate_frames(access)
            .map_err(|error| FlowError::InvalidRuntimeState {
                reason: error.to_string(),
            })?;
        for operation in &mut self.operations {
            let result = match operation {
                Operation::Source(source) => source.restore(access),
                Operation::Sink(sink) => sink.load(access).map(|_| ()),
                _ => Ok(()),
            };
            result.map_err(|error| FlowError::InvalidRuntimeState {
                reason: error.to_string(),
            })?;
        }
        Ok(())
    }
    fn validate_frames(&self, access: ReadTransactionAccess<'_>) -> Result<(), OperationError> {
        let controls = self.frames.controls.read(access)?.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(MAX_DEPTH + 1, (MAX_DEPTH + 1) * (CONTROL_BYTES + 4))?,
        )?;
        if controls.continuation.is_some() || controls.entries.len() > MAX_DEPTH {
            return Err("call stack exceeds depth bound".into());
        }
        let count = controls.entries.len();
        if let Some((last, _)) = self
            .frames
            .outputs
            .read(access)?
            .scan(
                ..,
                ScanDirection::Descending,
                None,
                ScanLimit::new(1, PAGE_BYTES + 4)?,
            )?
            .entries
            .last()
            && *last as usize >= count
        {
            return Err("orphan frame output".into());
        }
        for (expected, (depth, frame)) in controls.entries.iter().enumerate() {
            if *depth as usize != expected {
                return Err("call stack depths are not continuous".into());
            }
            let Some(node) = self.definition.operations.get(frame.head) else {
                return Err("frame refers to unknown head".into());
            };
            if !self.topology.heads[frame.head] || node.definition.kind().is_sink() {
                return Err("frame does not refer to a computation head".into());
            }
            match frame.input_port {
                None if expected == 0 && node.definition.kind().is_scan() => {}
                Some(port)
                    if expected > 0
                        && !node.definition.kind().is_scan()
                        && port < node.inputs.len() => {}
                _ => return Err("frame has invalid input port or source depth".into()),
            }
            let input_bytes = self.input(*depth, frame, access)?;
            if expected > 0 {
                let parent_frame = &controls.entries[expected - 1].1;
                let FramePhase::Send { next_consumer, .. } = parent_frame.phase else {
                    return Err("a running parent has a child".into());
                };
                let consumer = next_consumer
                    .checked_sub(1)
                    .and_then(|index| self.topology.consumers[parent_frame.head].get(index))
                    .ok_or("parent has invalid child ordinal")?;
                if consumer.operation != frame.head || Some(consumer.port) != frame.input_port {
                    return Err("child does not match its parent's pending call".into());
                }
            }
            let input = self.input_codec(frame).decode_owned(input_bytes)?;
            check_shape(
                &input,
                if expected == 0 { 4096 } else { 256 },
                if expected == 0 { 65536 } else { 16384 },
            )?;
            let input = OperationInput {
                port: frame.input_port.unwrap_or(0),
                change: &input,
            };
            match &frame.phase {
                FramePhase::Run(resume) => {
                    self.operations[frame.head].validate_resume(input, resume)?;
                    if self
                        .frames
                        .outputs
                        .read(access)?
                        .get_bounded(depth, 0)?
                        .is_some()
                    {
                        return Err("running frame retains an output".into());
                    }
                    if expected + 1 < count {
                        return Err("running frame is not the top".into());
                    }
                }
                FramePhase::Send {
                    next_consumer,
                    after,
                } => {
                    if *next_consumer > self.topology.consumers[frame.head].len() {
                        return Err("frame consumer ordinal exceeds fanout".into());
                    }
                    if let Progress::More(resume) = after {
                        self.operations[frame.head].validate_resume(input, resume)?;
                    }
                    let output = self.frames.output(*depth, access)?;
                    check_shape(&self.output_codec(frame.head).decode(&output)?, 256, 16384)?;
                }
            }
        }
        Ok(())
    }
}
pub(super) fn check_shape(
    change: &Change,
    max_rows: usize,
    max_slots: usize,
) -> Result<(), OperationError> {
    if change.num_rows() > max_rows
        || change
            .num_rows()
            .checked_mul(change.records().num_columns())
            .is_none_or(|slots| slots > max_slots)
    {
        return Err(dogpaddle_operation::operation::BudgetExceeded.into());
    }
    Ok(())
}
