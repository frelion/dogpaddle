use std::{num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    InlineBinding,
    operation::{Action, Operation, OperationInput, Turn},
};
use dogpaddle_store::{
    Cell, ReadTransactionAccess, ReadTransactions, StoreError, SubscribedLog, Subscription,
    TransactionAccess, Transactions,
};

use crate::flow::{AdvanceOutcome, StationStatus};

use super::{
    input::{Inbox, InputPort, Output},
    program::StationProgram,
    protocol::StationError,
};

pub(crate) struct StationParts {
    active: Option<Cell<u32>>,
    program: StationProgram,
    output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
}

pub(crate) struct Station {
    program: StationProgram,
    pub(super) inbox: Inbox,
    pub(super) output: Option<Arc<Output>>,
    needs_reopen: bool,
    last_outcome: Option<AdvanceOutcome>,
}

impl Station {
    pub(crate) fn advance(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<AdvanceOutcome, StationError> {
        let pinned = match self.inbox.intake(reads, transactions) {
            Ok(pinned) => pinned,
            Err(error) => {
                if error.requires_reopen() {
                    self.needs_reopen = true;
                }
                return Err(error);
            }
        };
        let outcome = self.process(transactions)?;
        self.last_outcome = Some(outcome);
        if pinned {
            Ok(AdvanceOutcome::Progressed)
        } else {
            Ok(outcome)
        }
    }

    pub(super) fn process(
        &mut self,
        transactions: &mut Transactions,
    ) -> Result<AdvanceOutcome, StationError> {
        self.ensure_runnable()?;
        if !self.inbox.is_input_free() && self.inbox.claim().is_none() {
            return Ok(AdvanceOutcome::Idle);
        }

        let claim = self.inbox.claim();
        let (input_pipelines, core, output_pipeline) = self.program.parts_mut();
        let transformed_input = if let Some(claim) = claim {
            let input = input_pipelines
                .get_mut(claim.port())
                .expect("an offered Claim has a validated input port");
            let Some(change) = input.apply_borrowed(claim.change()).map_err(|error| {
                StationError::InlineInput {
                    input: claim.port(),
                    stage: error.stage,
                    source: error.source,
                }
            })?
            else {
                let transaction = transactions.begin();
                self.inbox.complete(transaction.access())?;
                if let Err(source) = transaction.commit() {
                    self.needs_reopen = true;
                    return Err(StationError::Commit { source });
                }
                self.inbox.clear_claim();
                return Ok(AdvanceOutcome::Progressed);
            };
            if let Some(expected) = input.output_schema() {
                let actual = change.as_ref().schema();
                if expected.as_ref() != actual.as_ref() {
                    return Err(StationError::InputSchemaMismatch {
                        input: claim.port(),
                        expected: Arc::clone(expected),
                        actual,
                    });
                }
            }
            Some((claim.port(), change))
        } else {
            None
        };

        let (completes_input, after_commit) = {
            let input = transformed_input
                .as_ref()
                .map(|(port, change)| OperationInput {
                    port: *port,
                    change: change.as_ref(),
                });
            let prepared = match core.turn(input)? {
                Turn::Idle => return Ok(AdvanceOutcome::Idle),
                Turn::Ready(prepared) => prepared,
            };

            let transaction = transactions.begin();
            let access = transaction.access();
            let (action, after_commit) = prepared.apply(access)?;
            let (output, completes_input) = match action {
                Action::Idle => return Ok(AdvanceOutcome::Idle),
                Action::Commit(output) => (output, false),
                Action::Complete(output) => {
                    if self.inbox.is_input_free() {
                        return Err(StationError::OperationCompletedWithoutInput);
                    }
                    (output, true)
                }
            };

            let output = output
                .map(|change| {
                    output_pipeline.apply_owned(change).map_err(|error| {
                        StationError::InlineOutput {
                            stage: error.stage,
                            source: error.source,
                        }
                    })
                })
                .transpose()?
                .flatten();
            if !append_output(self.output.as_deref(), output, access)? {
                return Ok(AdvanceOutcome::Backpressured);
            }
            if completes_input {
                self.inbox.complete(access)?;
            }
            if let Err(source) = transaction.commit() {
                self.needs_reopen = true;
                return Err(StationError::Commit { source });
            }

            (completes_input, after_commit)
        };

        // Arm before calling user code: unwinding must also prevent reuse.
        self.needs_reopen = true;
        let after_commit_result = after_commit.run();
        // The completion may borrow the input, so release it before the Claim.
        if completes_input {
            self.inbox.clear_claim();
        }
        if let Err(source) = after_commit_result {
            return Err(StationError::AfterCommit { source });
        }
        self.needs_reopen = false;
        Ok(AdvanceOutcome::Progressed)
    }

    pub(crate) fn ensure_runnable(&self) -> Result<(), StationError> {
        if self.needs_reopen {
            Err(StationError::NeedsReopen)
        } else {
            Ok(())
        }
    }

    pub(crate) fn clear_outcome(&mut self) {
        self.last_outcome = None;
    }

    pub(crate) fn status(
        &self,
        id: &str,
        access: ReadTransactionAccess<'_>,
    ) -> Result<StationStatus, StationError> {
        let (active_input, inputs) = self.inbox.status(access)?;
        Ok(StationStatus {
            id: id.to_owned(),
            needs_reopen: self.needs_reopen,
            last_outcome: self.last_outcome,
            active_input,
            inputs,
            output: self
                .output
                .as_ref()
                .map(|output| output.status(access))
                .transpose()?,
        })
    }

    #[cfg(test)]
    pub(crate) fn replace_operation(&mut self, operation: Box<dyn Operation>) {
        self.program.replace_core(operation);
    }
}

fn append_output(
    output: Option<&Output>,
    emitted: Option<Change>,
    access: TransactionAccess<'_>,
) -> Result<bool, StationError> {
    let Some(change) = emitted else {
        return Ok(true);
    };
    let output = output.ok_or(StationError::UnexpectedOutput)?;
    output.try_append(&change, access)
}

impl StationParts {
    pub(crate) fn new(
        active: Option<Cell<u32>>,
        operation: Box<dyn Operation>,
        input_pipelines: Vec<Vec<InlineBinding>>,
        output_pipeline: Vec<InlineBinding>,
        output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
    ) -> Self {
        Self {
            active,
            program: StationProgram::new(operation, input_pipelines, output_pipeline),
            output,
        }
    }

    pub(crate) fn initialize(
        &self,
        subscriber_count: u64,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        if let Some(active) = &self.active {
            active.access(access)?.set(&0)?;
        }
        match (&self.output, NonZeroU64::new(subscriber_count)) {
            (Some((log, _, _)), Some(subscriber_count)) => {
                log.initialize(subscriber_count, access)?;
            }
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output ownership must match direct consumer count")
            }
        }
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        subscriber_count: u64,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StationError> {
        if let Some(active) = &self.active {
            let active = active
                .read(access)?
                .get()?
                .ok_or(StationError::MissingActiveInput)?;
            let input_count = self.program.input_count();
            let active = usize::try_from(active).expect("u32 fits usize on supported targets");
            if active >= input_count {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count,
                });
            }
        }
        match (&self.output, NonZeroU64::new(subscriber_count)) {
            (Some((log, _, _)), Some(subscriber_count)) => {
                log.validate(subscriber_count, access)?;
                Ok(())
            }
            (None, None) => Ok(()),
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output ownership must match direct consumer count")
            }
        }
    }

    pub(crate) fn subscription(&self, subscriber: u64) -> Subscription<Vec<u8>> {
        self.output
            .as_ref()
            .expect("validated input Station must produce output")
            .0
            .subscription(subscriber)
    }

    pub(crate) fn prepare_output(&mut self) -> Option<Arc<Output>> {
        self.output.take().map(|(log, capacity_bytes, schema)| {
            Arc::new(Output::new(log.writer(), capacity_bytes, schema))
        })
    }

    pub(crate) fn finish(self, inputs: Vec<InputPort>, output: Option<Arc<Output>>) -> Station {
        assert_eq!(
            inputs.len(),
            self.program.input_count(),
            "station input capabilities must match its operation definition"
        );
        assert!(
            self.output.is_none(),
            "station output must be moved exactly once during assembly"
        );
        Station {
            program: self.program,
            inbox: Inbox::new(self.active, inputs),
            output,
            needs_reopen: false,
            last_outcome: None,
        }
    }
}
