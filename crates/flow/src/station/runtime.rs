use std::{num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationKind,
    operation::{Action, Operation, OperationInput, Turn},
};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadTransactionAccess, ReadTransactions, Small, StoreError,
    TransactionAccess, Transactions,
};

use crate::flow::{AdvanceOutcome, StationStatus};

use super::{
    input::{
        ACTIVE_INPUT_KEY, CURSOR_ORIGIN, ConsumerCursor, Inbox, InputPort, Output, cursor_key,
        encode_active_input, encode_cursor,
    },
    protocol::StationError,
};

pub(crate) struct StationParts {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    kind: OperationKind,
    output: Option<(AppendLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
}

pub(crate) struct Station {
    pub(super) operation: Box<dyn Operation>,
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
        self.ensure_runnable()?;
        let pinned = self.inbox.intake(reads, transactions)?;
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

        let (completes_input, after_commit) = {
            let input = self.inbox.claim().map(|claim| OperationInput {
                port: claim.port(),
                change: claim.change(),
            });
            let prepared = match self.operation.turn(input)? {
                Turn::Idle => return Ok(AdvanceOutcome::Idle),
                Turn::Ready(prepared) => prepared,
            };

            let transaction = transactions.begin()?;
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

            if !append_output(self.output.as_deref(), output, access)? {
                return Ok(AdvanceOutcome::Backpressured);
            }
            if completes_input {
                self.inbox.complete(access)?;
            }
            transaction.commit()?;

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
        self.operation = operation;
    }

    pub(crate) fn validate_output(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StationError> {
        self.output
            .as_deref()
            .map_or(Ok(()), |output| output.validate_snapshot(access))
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
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        operation: Box<dyn Operation>,
        kind: OperationKind,
        output: Option<(AppendLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
    ) -> Self {
        Self {
            state,
            operation,
            kind,
            output,
        }
    }

    pub(crate) fn initialize_input_state(
        &self,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let mut state = self.state.access(access)?;
        let input_count =
            usize::try_from(self.kind.input_count()).expect("an Operation input count fits usize");
        if input_count > 0 {
            state.put(&ACTIVE_INPUT_KEY.to_vec(), &encode_active_input(0).to_vec())?;
        }
        let origin = encode_cursor(CURSOR_ORIGIN).to_vec();
        for index in 0..input_count {
            state.put(&cursor_key(index), &origin)?;
        }
        Ok(())
    }

    pub(crate) fn state(&self) -> &OrderedMap<Vec<u8>, Vec<u8>, Small> {
        &self.state
    }

    pub(crate) fn prepare_output(&mut self, consumers: Vec<ConsumerCursor>) -> Option<Arc<Output>> {
        self.output.take().map(|(log, capacity_bytes, schema)| {
            Arc::new(Output::new(log, capacity_bytes, schema, consumers))
        })
    }

    pub(crate) fn finish(self, inputs: Vec<InputPort>, output: Option<Arc<Output>>) -> Station {
        assert_eq!(
            inputs.len(),
            usize::try_from(self.kind.input_count()).expect("an Operation input count fits usize"),
            "station input capabilities must match its operation definition"
        );
        assert_eq!(
            output.is_some(),
            self.kind.has_output(),
            "station output capability must match its operation definition"
        );
        assert!(
            self.output.is_none(),
            "station output must be moved exactly once during assembly"
        );
        Station {
            operation: self.operation,
            inbox: Inbox::new(self.state, inputs),
            output,
            needs_reopen: false,
            last_outcome: None,
        }
    }
}
