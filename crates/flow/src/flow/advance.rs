use super::{
    Flow, Runtime,
    frame::{CONTROL_BYTES, Frame, FramePhase, PAGE_BYTES, ROOT_BYTES, STEP_BYTES},
    runtime::check_shape,
};
use crate::error::FlowRunError;
use dogpaddle_change::{Change, CodecError};
use dogpaddle_operation::operation::{
    BudgetExceeded, Operation, OperationError, OperationInput, Progress, StepBudget,
};
use dogpaddle_store::{BatchedTransaction, DurabilityBatch, ReadTransactions, TransactionAccess};

/// Aggregate outcome of one bounded scheduling round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    /// No durable progress or capacity rejection occurred.
    Idle,
    /// Capacity blocked progress.
    Backpressured,
    /// At least one atomic transition committed.
    Progressed,
}
impl AdvanceOutcome {
    const fn join(self, other: Self) -> Self {
        match (self, other) {
            (Self::Progressed, _) | (_, Self::Progressed) => Self::Progressed,
            (Self::Backpressured, _) | (_, Self::Backpressured) => Self::Backpressured,
            _ => Self::Idle,
        }
    }
}
const STACK_ACTIONS: usize = 32;
// Stack actions have their own allowance; source and sink boundaries enforce
// their owner limits separately.
const STACK_ROUND_BYTES: usize = 80 * PAGE_BYTES;
impl Flow {
    /// Services one source, at most 32 stack actions, and one sink, in rotation.
    /// Sources and sinks each rotate in declaration order, starting anew on reopen.
    ///
    /// Capturing and draining continue while computation is backpressured. One
    /// stack action may retry a page at most nine times, halving head work from
    /// 256 to one. Every successful page and all its fused tails are atomic.
    /// ACK and delivery force durability; other commits share a final barrier.
    ///
    /// # Errors
    /// Semantic errors preserve earlier committed pages. Commit, durability and
    /// external-effect uncertainty fail-stop the entire runtime until reopen.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        if self.runtime.needs_reopen {
            return Err(FlowRunError::new(
                "flow",
                "flow must be reopened".into(),
                true,
            ));
        }
        let mut batch = self.transactions.durability_batch();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime.round(&self.reads, &mut batch)
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                self.runtime.needs_reopen = true;
                std::panic::resume_unwind(payload)
            }
        };
        if let Err(error) = batch.finish() {
            self.runtime.needs_reopen = true;
            return Err(FlowRunError::new("flow", error.into(), true));
        }
        result.map_err(|(index, error)| {
            FlowRunError::new(
                &self.runtime.nodes[index].id,
                error,
                self.runtime.needs_reopen,
            )
        })
    }
}
impl Runtime {
    fn round(
        &mut self,
        reads: &ReadTransactions,
        batch: &mut DurabilityBatch<'_>,
    ) -> Result<AdvanceOutcome, (usize, OperationError)> {
        let source = self.sources[self.source_cursor];
        self.source_cursor = (self.source_cursor + 1) % self.sources.len();
        let mut outcome = self
            .capture(source, batch)
            .map_err(|error| (source, error))?;
        let mut remaining = STACK_ROUND_BYTES;
        for _ in 0..STACK_ACTIONS {
            // A complete retry sequence is one action; never restart its limit
            // on the next scheduling round.
            if remaining < CONTROL_BYTES {
                break;
            }
            remaining -= CONTROL_BYTES;
            let top = self
                .frames
                .top(reads.begin().access())
                .map_err(|error| (source, error.into()))?;
            let allowance = match top.as_ref().map(|(_, frame)| &frame.phase) {
                Some(FramePhase::Run(_)) => ROOT_BYTES + 9 * STEP_BYTES + 64,
                Some(FramePhase::Send { .. }) => STEP_BYTES,
                None => ROOT_BYTES + 9 * STEP_BYTES + CONTROL_BYTES,
            };
            if remaining < allowance {
                break;
            }
            let (index, result) = if let Some((depth, frame)) = top {
                (
                    frame.head,
                    self.advance_frame(depth, frame, reads, batch, &mut remaining),
                )
            } else {
                let (index, outcome) = self.advance_next_root(reads, batch, &mut remaining)?;
                (index, Ok(outcome))
            };
            let action = result.map_err(|error| (index, error))?;
            outcome = outcome.join(action);
            if action != AdvanceOutcome::Progressed {
                break;
            }
        }
        let sink = self.sinks[self.sink_cursor];
        self.sink_cursor = (self.sink_cursor + 1) % self.sinks.len();
        outcome = outcome.join(
            self.drain(sink, reads, batch)
                .map_err(|error| (sink, error))?,
        );
        Ok(outcome)
    }
    fn capture(
        &mut self,
        index: usize,
        batch: &mut DurabilityBatch<'_>,
    ) -> Result<AdvanceOutcome, OperationError> {
        let node = &mut self.nodes[index];
        let Operation::Source(source) = &mut node.operation else {
            unreachable!("source role checked at construction")
        };
        if node.pending.is_none() {
            node.pending = source.poll().inspect_err(|_| self.needs_reopen = true)?;
        }
        let Some(delivery) = node.pending.as_mut() else {
            return Ok(AdvanceOutcome::Idle);
        };
        let transaction = batch.begin();
        if !source.record(transaction.access(), delivery)? {
            return Ok(AdvanceOutcome::Backpressured);
        }
        let requires_barrier = delivery.requires_ack_barrier();
        commit(transaction, &mut self.needs_reopen)?;
        if requires_barrier {
            sync(batch, &mut self.needs_reopen)?;
        }
        let delivery = node.pending.take().expect("captured delivery exists");
        source
            .ack(delivery)
            .inspect_err(|_| self.needs_reopen = true)?;
        Ok(AdvanceOutcome::Progressed)
    }
    fn advance_next_root(
        &mut self,
        reads: &ReadTransactions,
        batch: &mut DurabilityBatch<'_>,
        remaining: &mut usize,
    ) -> Result<(usize, AdvanceOutcome), (usize, OperationError)> {
        let mut index = self.sources[self.root_cursor];
        for _ in 0..self.sources.len() {
            if *remaining < ROOT_BYTES + 9 * STEP_BYTES + CONTROL_BYTES + 64 {
                break;
            }
            index = self.sources[self.root_cursor];
            self.root_cursor = (self.root_cursor + 1) % self.sources.len();
            *remaining -= 64;
            let Operation::Source(source) = &self.nodes[index].operation else {
                unreachable!("source role")
            };
            let Some(bytes) = source
                .published(reads.begin().access())
                .map_err(|error| (index, error))?
            else {
                continue;
            };
            *remaining -= bytes.len();
            let frame = Frame {
                head: index,
                input_port: None,
                phase: FramePhase::Run(self.nodes[index].operation.initial_resume()),
            };
            let input = self
                .input_codec(&frame)
                .decode_owned(bytes)
                .map_err(|error| (index, error.into()))?;
            check_shape(&input, 4096, 65536).map_err(|error| (index, error))?;
            let result = self.run_frame(0, &frame, &input, batch, remaining);
            if result.is_err() && !self.needs_reopen {
                // The failed page has rolled back. Pin only its identity so
                // reopening retries this source front before another root.
                *remaining -= CONTROL_BYTES;
                let transaction = batch.begin();
                self.frames
                    .put(0, &frame, transaction.access())
                    .map_err(|error| (index, error))?;
                commit(transaction, &mut self.needs_reopen).map_err(|error| (index, error))?;
            }
            return result
                .map(|outcome| (index, outcome))
                .map_err(|error| (index, error));
        }
        Ok((index, AdvanceOutcome::Idle))
    }
    fn drain(
        &mut self,
        index: usize,
        reads: &ReadTransactions,
        batch: &mut DurabilityBatch<'_>,
    ) -> Result<AdvanceOutcome, OperationError> {
        let Operation::Sink(sink) = &mut self.nodes[index].operation else {
            unreachable!("sink role")
        };
        let Some(pending) = sink.load(reads.begin().access())? else {
            return Ok(AdvanceOutcome::Idle);
        };
        let prepared = sink
            .prepare(pending)
            .inspect_err(|_| self.needs_reopen = true)?;
        {
            let transaction = batch.begin();
            sink.persist_prepared(transaction.access(), &prepared)?;
            commit(transaction, &mut self.needs_reopen)?;
        }
        sync(batch, &mut self.needs_reopen)?;
        sink.deliver(&prepared)
            .inspect_err(|_| self.needs_reopen = true)?;
        {
            let transaction = batch.begin();
            sink.settle(transaction.access(), &prepared)
                .inspect_err(|_| self.needs_reopen = true)?;
            commit(transaction, &mut self.needs_reopen)?;
        }
        Ok(AdvanceOutcome::Progressed)
    }
    fn advance_frame(
        &mut self,
        depth: u32,
        frame: Frame,
        reads: &ReadTransactions,
        batch: &mut DurabilityBatch<'_>,
        remaining: &mut usize,
    ) -> Result<AdvanceOutcome, OperationError> {
        match &frame.phase {
            FramePhase::Run(_) => {
                if depth == 0 {
                    *remaining -= 64;
                }
                let bytes = self.input(depth, &frame, reads.begin().access())?;
                *remaining -= bytes.len();
                let input = self.input_codec(&frame).decode_owned(bytes)?;
                self.run_frame(depth, &frame, &input, batch, remaining)
            }
            FramePhase::Send { .. } => {
                *remaining -= STEP_BYTES;
                self.send_page(depth, frame, reads, batch)
            }
        }
    }
    fn run_frame(
        &mut self,
        depth: u32,
        frame: &Frame,
        input: &Change,
        batch: &mut DurabilityBatch<'_>,
        remaining: &mut usize,
    ) -> Result<AdvanceOutcome, OperationError> {
        let mut head_items = 256;
        loop {
            let transaction = batch.begin();
            let mut budget = StepBudget::new(head_items, STEP_BYTES);
            let result = self.run_page(depth, frame, input, transaction.access(), &mut budget);
            *remaining -= STEP_BYTES - budget.remaining_bytes();
            match result {
                Ok(outcome) => {
                    commit(transaction, &mut self.needs_reopen)?;
                    return Ok(outcome);
                }
                Err(error) if is_budget_error(error.as_ref()) && head_items > 1 => {
                    head_items /= 2;
                }
                Err(error) => return Err(error),
            }
            // Dropping the failed transaction rolls back the head, all tails,
            // cursor and payload together before the next try.
        }
    }
    fn run_page(
        &mut self,
        depth: u32,
        frame: &Frame,
        input: &Change,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<AdvanceOutcome, OperationError> {
        let FramePhase::Run(resume) = &frame.phase else {
            unreachable!("running frame")
        };
        budget.charge(CONTROL_BYTES)?;
        let mut step = self.nodes[frame.head].operation.step(
            OperationInput {
                port: frame.input_port.unwrap_or(0),
                change: input,
            },
            resume,
            access,
            budget,
        )?;
        if matches!(&step.progress, Progress::More(next) if next == resume) {
            return Err("computation returned an unchanged position".into());
        }
        for &tail in &self.topology.tails[frame.head] {
            let Some(change) = step.output.as_ref() else {
                break;
            };
            let Operation::Atomic(operation) = &self.nodes[tail].operation else {
                unreachable!("validated atomic tail")
            };
            step.output = operation.apply(OperationInput { port: 0, change }, access, budget)?;
        }
        if let Some(output) = step.output {
            check_shape(&output, 256, 16384)?;
            let limit = PAGE_BYTES.min(budget.remaining_bytes());
            let encoded = match self.output_codec(frame.head).encode_bounded(&output, limit) {
                Err(CodecError::EncodedSizeLimitExceeded { .. }) => {
                    budget.charge(limit)?;
                    return Err(BudgetExceeded.into());
                }
                result => result?,
            };
            // Encoding and a possible pending write, plus both controls and
            // returns, are prepaid even when routing finishes without them.
            budget.charge(encoded.len())?;
            budget.charge(encoded.len().saturating_add(2 * CONTROL_BYTES + 12))?;
            let sending = Frame {
                phase: FramePhase::Send {
                    next_consumer: 0,
                    after: step.progress,
                },
                ..frame.clone()
            };
            self.route_page(depth, sending, Some(output), &encoded, access, budget)
        } else {
            budget.charge(2 * CONTROL_BYTES + 12)?;
            self.finish_frame(depth, frame, step.progress, access)?;
            Ok(AdvanceOutcome::Progressed)
        }
    }
    fn send_page(
        &mut self,
        depth: u32,
        frame: Frame,
        reads: &ReadTransactions,
        batch: &mut DurabilityBatch<'_>,
    ) -> Result<AdvanceOutcome, OperationError> {
        let FramePhase::Send {
            next_consumer,
            after,
        } = &frame.phase
        else {
            unreachable!("sending frame")
        };
        let mut budget = StepBudget::new(1, STEP_BYTES);
        budget.charge(2 * CONTROL_BYTES + 12)?;
        if *next_consumer == self.topology.consumers[frame.head].len() {
            let transaction = batch.begin();
            self.finish_frame(depth, &frame, after.clone(), transaction.access())?;
            commit(transaction, &mut self.needs_reopen)?;
            return Ok(AdvanceOutcome::Progressed);
        }
        let encoded = self.frames.output(depth, reads.begin().access())?;
        budget.charge(encoded.len())?;
        let transaction = batch.begin();
        let outcome = self.route_page(
            depth,
            frame,
            None,
            &encoded,
            transaction.access(),
            &mut budget,
        )?;
        commit(transaction, &mut self.needs_reopen)?;
        Ok(outcome)
    }
    fn route_page(
        &mut self,
        depth: u32,
        mut frame: Frame,
        mut output: Option<Change>,
        encoded: &Vec<u8>,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<AdvanceOutcome, OperationError> {
        let FramePhase::Send {
            mut next_consumer,
            after,
        } = frame.phase.clone()
        else {
            unreachable!("routing a computed page")
        };
        let fresh = output.is_some();
        let first_consumer = next_consumer;
        let mut blocked = false;
        let child = loop {
            let Some(consumer) = self.topology.consumers[frame.head]
                .get(next_consumer)
                .copied()
            else {
                self.finish_frame(depth, &frame, after, access)?;
                return Ok(AdvanceOutcome::Progressed);
            };
            if matches!(self.nodes[consumer.operation].operation, Operation::Sink(_)) {
                let bytes = encoded.len().saturating_mul(2).saturating_add(128);
                if bytes > budget.remaining_bytes() {
                    break None;
                }
                budget.charge(bytes)?;
                if output.is_none() {
                    output = Some(self.output_codec(frame.head).decode(encoded)?);
                }
                let Operation::Sink(sink) = &mut self.nodes[consumer.operation].operation else {
                    unreachable!()
                };
                // Capacity rejection guarantees no writes, so this page and
                // any earlier enqueue can safely remain committed.
                if !sink.try_enqueue(access, output.as_ref().expect("page decoded for sink"))? {
                    blocked = true;
                    break None;
                }
                next_consumer += 1;
            } else {
                next_consumer += 1;
                break Some(Frame {
                    head: consumer.operation,
                    input_port: Some(consumer.port),
                    phase: FramePhase::Run(
                        self.nodes[consumer.operation].operation.initial_resume(),
                    ),
                });
            }
        };
        let progressed = fresh || next_consumer != first_consumer;
        if fresh {
            self.frames.outputs.access(access)?.put(&depth, encoded)?;
        }
        if progressed {
            frame.phase = FramePhase::Send {
                next_consumer,
                after,
            };
            self.frames.put(depth, &frame, access)?;
        }
        if let Some(child) = child {
            self.frames.put(depth + 1, &child, access)?;
        }
        Ok(if progressed {
            AdvanceOutcome::Progressed
        } else if blocked {
            AdvanceOutcome::Backpressured
        } else {
            AdvanceOutcome::Idle
        })
    }
    fn finish_frame(
        &mut self,
        depth: u32,
        frame: &Frame,
        after: Progress,
        access: TransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        match after {
            Progress::Done => {
                if depth == 0 {
                    let Operation::Source(source) = &self.nodes[frame.head].operation else {
                        unreachable!("root source role")
                    };
                    source.consume_published(access)?;
                }
                self.frames.pop(depth, access)?;
            }
            Progress::More(resume) => {
                self.frames.outputs.access(access)?.remove(&depth)?;
                self.frames.put(
                    depth,
                    &Frame {
                        phase: FramePhase::Run(resume),
                        head: frame.head,
                        input_port: frame.input_port,
                    },
                    access,
                )?;
            }
        }
        Ok(())
    }
}
fn commit(
    transaction: BatchedTransaction<'_>,
    needs_reopen: &mut bool,
) -> Result<(), OperationError> {
    transaction.commit().map_err(|error| {
        *needs_reopen = true;
        error.into()
    })
}
fn sync(batch: &mut DurabilityBatch<'_>, needs_reopen: &mut bool) -> Result<(), OperationError> {
    batch.sync().map_err(|error| {
        *needs_reopen = true;
        error.into()
    })
}

fn is_budget_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error.is::<BudgetExceeded>() {
            return true;
        }
        let Some(source) = error.source() else {
            return false;
        };
        error = source;
    }
}

#[cfg(test)]
mod tests;
