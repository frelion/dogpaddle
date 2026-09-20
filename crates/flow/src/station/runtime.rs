#[cfg(test)]
use dogpaddle_operation::operation::Operation;
use std::sync::Arc;

use dogpaddle_change::Change;
use dogpaddle_operation::operation::{Action, OperationInput, Turn};
use dogpaddle_store::{ReadTransactionAccess, ReadTransactions, TransactionAccess, Transactions};

use crate::flow::{AdvanceOutcome, StationStatus};

use super::{
    input::{Inbox, Output},
    program::StationProgram,
    protocol::StationError,
};

pub(crate) struct Station {
    program: StationProgram,
    pub(super) inbox: Inbox,
    pub(super) output: Option<Arc<Output>>,
    needs_reopen: bool,
    last_outcome: Option<AdvanceOutcome>,
}

impl Station {
    pub(crate) fn new(program: StationProgram, inbox: Inbox, output: Option<Arc<Output>>) -> Self {
        Self {
            program,
            inbox,
            output,
            needs_reopen: false,
            last_outcome: None,
        }
    }

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

        let (completes_input, after_commit) = {
            let input = self.inbox.claim().map(|claim| OperationInput {
                port: claim.port(),
                change: claim.change(),
            });
            let (head, tail) = self.program.operations_mut();
            let prepared = match head.turn(input).map_err(|source| StationError::Operation {
                operation: 0,
                source,
            })? {
                Turn::Idle => return Ok(AdvanceOutcome::Idle),
                Turn::Ready(prepared) => prepared,
            };

            let transaction = transactions.begin();
            let access = transaction.access();
            let (action, after_commit) =
                prepared
                    .apply(access)
                    .map_err(|source| StationError::Operation {
                        operation: 0,
                        source,
                    })?;
            let (mut output, completes_input) = match action {
                Action::Idle => return Ok(AdvanceOutcome::Idle),
                Action::Commit(output) => (output, false),
                Action::Complete(output) => {
                    if self.inbox.claim().is_none() {
                        return Err(StationError::OperationCompletedWithoutInput);
                    }
                    (output, true)
                }
            };
            for (operation, atomic) in tail.iter_mut().enumerate() {
                let Some(change) = output.as_ref() else {
                    break;
                };
                output = atomic
                    .apply(OperationInput { port: 0, change }, access)
                    .map_err(|source| StationError::Operation {
                        operation: operation + 1,
                        source,
                    })?;
            }
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
    pub(crate) fn replace_operation(
        &mut self,
        operation: Box<dyn dogpaddle_operation::operation::TurnOperation>,
    ) {
        self.program.replace_head(Operation::Turn(operation));
    }

    #[cfg(test)]
    pub(crate) fn replace_tail(
        &mut self,
        tail: Vec<Box<dyn dogpaddle_operation::operation::AtomicOperation>>,
    ) {
        self.program.replace_tail(tail);
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
