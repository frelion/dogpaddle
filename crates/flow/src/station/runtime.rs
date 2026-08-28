use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, Small, StoreError, TransactionAccess,
    Transactions,
};

use super::{
    gc::ConsumerCursor,
    input::{
        ACTIVE_INPUT_KEY, CURSOR_ORIGIN, Inputs, cursor_key, encode_active_input, encode_cursor,
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
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the future processing protocol")
    )]
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
        let _ = transactions;
        todo!("station processing awaits the Station-Operation batch protocol")
    }
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
