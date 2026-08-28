use std::num::NonZeroU64;

use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::operation::{InputProgress, Operation, OperationInput, TurnDecision};
use dogpaddle_store::{
    AppendLog, AppendLogAccess, OrderedMap, ReadOnly, ReadTransactions, Small, StoreError,
    TransactionAccess, Transactions,
};

use super::{
    gc::ConsumerCursor,
    input::{
        ACTIVE_INPUT_KEY, CURSOR_ORIGIN, Inputs, cursor_key, decode_cursor, encode_active_input,
        encode_cursor,
    },
    protocol::{ProcessOutcome, StationError},
};

pub(crate) struct StationParts {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    output: Option<StationOutput>,
}

pub(crate) struct Station {
    pub(super) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(super) operation: Box<dyn Operation>,
    pub(super) inputs: Inputs,
    pub(super) output: Option<StationOutput>,
    pub(super) consumers: Vec<ConsumerCursor>,
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
    ) -> Result<ProcessOutcome, StationError> {
        let pinned = self.intake(reads, transactions)?;
        let outcome = self.process(transactions)?;
        if pinned {
            Ok(ProcessOutcome::Progressed)
        } else {
            Ok(outcome)
        }
    }

    pub(crate) fn process(
        &mut self,
        transactions: &mut Transactions,
    ) -> Result<ProcessOutcome, StationError> {
        if !self.inputs.logs.is_empty() && self.inputs.cache.is_none() {
            return Ok(ProcessOutcome::Idle);
        }

        let transaction = transactions.begin()?;
        let access = transaction.access();

        let input = if let Some(cached) = self.inputs.cache.as_ref() {
            let input = cached.input;
            let offset = cached.offset;
            let state = self.state.access(access)?;
            let encoded = state
                .get(&ACTIVE_INPUT_KEY.to_vec())?
                .ok_or(StationError::MissingActiveInput)?;
            let active = super::input::decode_active_input(&encoded)
                .ok_or(StationError::MalformedActiveInput)?;
            if active >= self.inputs.logs.len() {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count: self.inputs.logs.len(),
                });
            }
            if active != input {
                return Err(StationError::CachedActiveInputMismatch {
                    cached: input,
                    durable: active,
                });
            }

            let key = cursor_key(input);
            let encoded = state
                .get(&key)?
                .ok_or(StationError::MissingCursor { input })?;
            let durable = decode_cursor(&encoded).ok_or(StationError::MalformedCursor { input })?;
            if durable != offset {
                return Err(StationError::CachedCursorMismatch {
                    input,
                    cached: offset,
                    durable,
                });
            }

            Some(OperationInput {
                port: input,
                change: &cached.change,
            })
        } else {
            None
        };

        let decision = self.operation.turn(input, access)?;
        let TurnDecision::Commit(commit) = decision else {
            return Ok(ProcessOutcome::Idle);
        };
        let offered_input = self.inputs.cache.is_some();
        if offered_input != commit.input.is_some() {
            return Err(StationError::OperationInputProgressMismatch {
                offered_input,
                returned_input: commit.input.is_some(),
            });
        }

        if !append_output(self.output.as_ref(), commit.output, access)? {
            return Ok(ProcessOutcome::Backpressured);
        }
        if let Some(InputProgress::Complete) = commit.input {
            let cached = self
                .inputs
                .cache
                .as_ref()
                .expect("input progress shape was validated");
            let next_offset = cached
                .offset
                .checked_add(1)
                .expect("an AppendLog entry offset always has a successor");
            let next_input = if cached.input + 1 == self.inputs.logs.len() {
                0
            } else {
                cached.input + 1
            };
            let mut state = self.state.access(access)?;
            state.put(
                &cursor_key(cached.input),
                &encode_cursor(next_offset).to_vec(),
            )?;
            state.put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(next_input).to_vec(),
            )?;
        }
        transaction.commit()?;

        if matches!(commit.input, Some(InputProgress::Complete)) {
            self.inputs.cache = None;
        }
        Ok(ProcessOutcome::Progressed)
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
        output: Option<(AppendLog<Vec<u8>>, NonZeroU64)>,
    ) -> Self {
        Self {
            state,
            operation,
            output: output.map(|(log, capacity_bytes)| StationOutput::new(log, capacity_bytes)),
        }
    }

    pub(crate) fn initialize_input_state(
        &self,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let mut state = self.state.access(access)?;
        let input_count = self.operation.definition().input_count();
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

    pub(crate) fn finish(
        self,
        inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
        consumers: Vec<ConsumerCursor>,
    ) -> Station {
        assert_eq!(
            inputs.len(),
            self.operation.definition().input_count(),
            "station input capabilities must match its operation definition"
        );
        assert_eq!(
            self.output.is_some(),
            self.operation.definition().category().has_output(),
            "station output capability must match its operation definition"
        );
        assert!(
            self.output.is_some() || consumers.is_empty(),
            "a station without output cannot have consumers"
        );
        Station {
            state: self.state,
            operation: self.operation,
            inputs: Inputs::new(inputs),
            output: self.output,
            consumers,
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

    #[cfg(test)]
    pub(super) const fn capacity_bytes(&self) -> NonZeroU64 {
        self.capacity_bytes
    }

    pub(super) fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<AppendLogAccess<'transaction, Vec<u8>>, StoreError> {
        self.log.access(access)
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
