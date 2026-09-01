use std::num::NonZeroU64;

use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::{
    OperationKind,
    operation::{Action, Operation, OperationInput},
};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadTransactions, Small, StoreError, TransactionAccess, Transactions,
};

use crate::flow::AdvanceOutcome;

use super::{
    input::{
        ACTIVE_INPUT_KEY, CURSOR_ORIGIN, Inbox, InputPort, cursor_key, encode_active_input,
        encode_cursor,
    },
    protocol::StationError,
};

pub(crate) struct StationParts {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    kind: OperationKind,
    output: Option<StationOutput>,
}

pub(crate) struct Station {
    pub(super) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(super) operation: Box<dyn Operation>,
    pub(super) inbox: Inbox,
    pub(super) output: Option<StationOutput>,
}

pub(super) struct StationOutput {
    log: AppendLog<Vec<u8>>,
    capacity_bytes: NonZeroU64,
}

impl Station {
    pub(crate) fn advance(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<AdvanceOutcome, StationError> {
        let pinned = self.intake(reads, transactions)?;
        let outcome = self.process(transactions)?;
        if pinned {
            Ok(AdvanceOutcome::Progressed)
        } else {
            Ok(outcome)
        }
    }

    pub(crate) fn process(
        &mut self,
        transactions: &mut Transactions,
    ) -> Result<AdvanceOutcome, StationError> {
        if !self.inbox.is_input_free() && self.inbox.claim().is_none() {
            return Ok(AdvanceOutcome::Idle);
        }

        let transaction = transactions.begin()?;
        let access = transaction.access();
        let input = self.inbox.claim().map(|claim| OperationInput {
            port: claim.port(),
            change: claim.change(),
        });
        let action = self.operation.turn(input, access)?;
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

        if !append_output(self.output.as_ref(), output, access)? {
            return Ok(AdvanceOutcome::Backpressured);
        }
        if completes_input {
            self.inbox.complete(&self.state, access)?;
        }
        transaction.commit()?;

        if completes_input {
            self.inbox.clear_claim();
        }
        Ok(AdvanceOutcome::Progressed)
    }
}

fn append_output(
    output: Option<&StationOutput>,
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
        output: Option<(AppendLog<Vec<u8>>, NonZeroU64)>,
    ) -> Self {
        Self {
            state,
            operation,
            kind,
            output: output.map(|(log, capacity_bytes)| StationOutput::new(log, capacity_bytes)),
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

    pub(crate) fn output(&self) -> Option<&AppendLog<Vec<u8>>> {
        self.output.as_ref().map(|output| &output.log)
    }

    pub(crate) fn state(&self) -> &OrderedMap<Vec<u8>, Vec<u8>, Small> {
        &self.state
    }

    pub(crate) fn finish(self, inputs: Vec<InputPort>) -> Station {
        assert_eq!(
            inputs.len(),
            usize::try_from(self.kind.input_count()).expect("an Operation input count fits usize"),
            "station input capabilities must match its operation definition"
        );
        assert_eq!(
            self.output.is_some(),
            self.kind.has_output(),
            "station output capability must match its operation definition"
        );
        Station {
            state: self.state,
            operation: self.operation,
            inbox: Inbox::new(inputs),
            output: self.output,
        }
    }
}

impl StationOutput {
    pub(super) const fn new(log: AppendLog<Vec<u8>>, capacity_bytes: NonZeroU64) -> Self {
        Self {
            log,
            capacity_bytes,
        }
    }

    #[cfg(test)]
    pub(super) const fn log(&self) -> &AppendLog<Vec<u8>> {
        &self.log
    }

    fn try_append(
        &self,
        change: &Change,
        access: TransactionAccess<'_>,
    ) -> Result<bool, StationError> {
        let encoded =
            encode_change(change).map_err(|source| StationError::InvalidOutputChange { source })?;
        Ok(self
            .log
            .access(access)?
            .try_append(&encoded, self.capacity_bytes)?
            .is_some())
    }
}
