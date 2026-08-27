use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{AppendLog, OrderedMap, ReadOnly, Small, Transactions};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "station work outcomes are returned by the future scheduling phase"
    )
)]
pub(crate) enum WorkOutcome {
    Idle,
    Progressed,
}

#[derive(Debug)]
pub(crate) enum StationError {}

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
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
    output: Option<AppendLog<Vec<u8>>>,
}

impl Station {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Flow scheduler is not implemented yet")
    )]
    pub(crate) fn work(
        &mut self,
        transactions: &mut Transactions,
    ) -> Result<WorkOutcome, StationError> {
        let _ = transactions;
        todo!("station work is not implemented yet")
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
            inputs,
            output: self.output,
        }
    }
}

#[cfg(test)]
mod tests;
