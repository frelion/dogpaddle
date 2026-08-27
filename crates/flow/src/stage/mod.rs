use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{AppendLog, OrderedMap, ReadOnly, Small, Transactions};

pub(crate) struct StageParts {
    pub(crate) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(crate) operation: Box<dyn Operation>,
    pub(crate) output: Option<AppendLog<Vec<u8>>>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stage instances are consumed by the next run phase"
    )
)]
pub(crate) struct Stage {
    pub(crate) transactions: Transactions,
    pub(crate) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    pub(crate) operation: Box<dyn Operation>,
    pub(crate) inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
    pub(crate) output: Option<AppendLog<Vec<u8>>>,
}

impl Stage {
    pub(crate) fn new(
        transactions: Transactions,
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        operation: Box<dyn Operation>,
        inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
        output: Option<AppendLog<Vec<u8>>>,
    ) -> Self {
        assert_eq!(
            inputs.len(),
            operation.definition().input_count(),
            "stage input capabilities must match its operation definition"
        );
        assert_eq!(
            output.is_some(),
            operation.definition().produces_output(),
            "stage output capability must match its operation definition"
        );
        Self {
            transactions,
            state,
            operation,
            inputs,
            output,
        }
    }
}

#[cfg(test)]
mod tests;
