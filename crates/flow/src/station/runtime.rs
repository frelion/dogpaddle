use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, Small, StoreError, TransactionAccess, Transactions,
};

use super::{
    input::{Cursor, Input, cursor_key},
    protocol::{ProcessOutcome, StationError},
};

pub(crate) struct StationParts {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    output: Option<AppendLog<Vec<u8>>>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "station instances are consumed by the future scheduling phase"
    )
)]
pub(crate) struct Station {
    pub(super) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(super) operation: Box<dyn Operation>,
    pub(super) inputs: Vec<Input>,
    pub(super) output: Option<AppendLog<Vec<u8>>>,
}

impl Station {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the Station-Operation batch protocol is not defined yet"
        )
    )]
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

    pub(crate) fn initialize_cursors(
        &self,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let mut state = self.state.access(access)?;
        let origin = Cursor::ORIGIN.encode().to_vec();
        for index in 0..self.operation.definition().input_count() {
            state.put(&cursor_key(index), &origin)?;
        }
        Ok(())
    }

    pub(crate) fn output(&self) -> Option<&AppendLog<Vec<u8>>> {
        self.output.as_ref()
    }

    pub(crate) fn finish(self, inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>) -> Station {
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
        Station {
            state: self.state,
            operation: self.operation,
            inputs: inputs.into_iter().map(Input::new).collect(),
            output: self.output,
        }
    }
}
