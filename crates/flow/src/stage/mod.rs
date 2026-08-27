use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{AppendLog, OrderedMap, ReadOnly, Small};

pub(crate) struct StageParts {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    output: Option<AppendLog<Vec<u8>>>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stage instances are consumed by the next run phase"
    )
)]
pub(crate) struct Stage {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    operation: Box<dyn Operation>,
    inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
    output: Option<AppendLog<Vec<u8>>>,
}

impl StageParts {
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

    pub(crate) fn finish(self, inputs: Vec<ReadOnly<AppendLog<Vec<u8>>>>) -> Stage {
        assert_eq!(
            inputs.len(),
            self.operation.definition().input_count(),
            "stage input capabilities must match its operation definition"
        );
        assert_eq!(
            self.output.is_some(),
            self.operation.definition().produces_output(),
            "stage output capability must match its operation definition"
        );
        Stage {
            state: self.state,
            operation: self.operation,
            inputs,
            output: self.output,
        }
    }
}

#[cfg(test)]
mod tests;
