use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::operation::{Operation, OperationInput};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, Small, StoreError, TransactionAccess,
    Transactions,
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
    output: Option<AppendLog<Vec<u8>>>,
}

pub(crate) struct Station {
    pub(super) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(super) operation: Box<dyn Operation>,
    pub(super) inputs: Inputs,
    pub(super) output: Option<AppendLog<Vec<u8>>>,
    pub(super) consumers: Vec<ConsumerCursor>,
}

impl Station {
    pub(crate) fn advance(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<ProcessOutcome, StationError> {
        self.intake(reads)?;
        self.process(transactions)
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
            let next_offset = offset
                .checked_add(1)
                .expect("an AppendLog entry offset always has a successor");
            let next_input = if input + 1 == self.inputs.logs.len() {
                0
            } else {
                input + 1
            };

            let mut state = self.state.access(access)?;
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

            state.put(&key, &encode_cursor(next_offset).to_vec())?;
            state.put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(next_input).to_vec(),
            )?;

            Some(OperationInput {
                port: input,
                change: &cached.change,
            })
        } else {
            None
        };

        let emitted = self.operation.turn(input, access)?;
        append_output(self.output.as_ref(), emitted, access)?;
        transaction.commit()?;

        self.inputs.cache = None;
        Ok(ProcessOutcome::Progressed)
    }
}

fn append_output(
    output: Option<&AppendLog<Vec<u8>>>,
    emitted: Option<Change>,
    access: TransactionAccess<'_>,
) -> Result<(), StationError> {
    let Some(change) = emitted else {
        return Ok(());
    };
    let output = output.ok_or(StationError::UnexpectedOutput)?;
    let encoded =
        encode_change(&change).map_err(|source| StationError::InvalidOutputChange { source })?;
    output.access(access)?.append(&encoded)?;
    Ok(())
}

impl StationParts {
    pub(crate) fn new(
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        operation: Box<dyn Operation>,
        output: Option<AppendLog<Vec<u8>>>,
    ) -> Self {
        Self {
            state,
            operation,
            output,
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
        self.output.as_ref()
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
            self.operation.definition().produces_output(),
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
