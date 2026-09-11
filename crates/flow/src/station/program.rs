use std::borrow::Cow;

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    InlineBinding,
    operation::{Operation, OperationError},
};

/// The executable contents of one Station.
///
/// A program keeps exactly one core Operation and zero or more pure inline
/// stages around it without changing the Flow topology or Station transaction
/// boundary.
pub(crate) struct StationProgram {
    core: Box<dyn Operation>,
    inputs: Vec<InlinePipeline>,
    output: InlinePipeline,
}

impl StationProgram {
    pub(crate) fn new(
        core: Box<dyn Operation>,
        inputs: Vec<Vec<InlineBinding>>,
        output: Vec<InlineBinding>,
    ) -> Self {
        Self {
            core,
            inputs: inputs.into_iter().map(InlinePipeline::new).collect(),
            output: InlinePipeline::new(output),
        }
    }

    pub(crate) const fn input_count(&self) -> usize {
        self.inputs.len()
    }

    pub(crate) fn parts_mut(
        &mut self,
    ) -> (
        &mut [InlinePipeline],
        &mut dyn Operation,
        &mut InlinePipeline,
    ) {
        (&mut self.inputs, self.core.as_mut(), &mut self.output)
    }

    #[cfg(test)]
    pub(crate) fn replace_core(&mut self, core: Box<dyn Operation>) {
        self.core = core;
    }
}

pub(crate) struct InlinePipeline {
    stages: Vec<InlineBinding>,
}

impl InlinePipeline {
    fn new(stages: Vec<InlineBinding>) -> Self {
        Self { stages }
    }

    pub(crate) fn apply_borrowed<'input>(
        &mut self,
        input: &'input Change,
    ) -> Result<Option<Cow<'input, Change>>, InlinePipelineError> {
        self.apply(Cow::Borrowed(input))
    }

    pub(crate) fn apply_owned(
        &mut self,
        input: Change,
    ) -> Result<Option<Change>, InlinePipelineError> {
        self.apply(Cow::Owned(input))
            .map(|output| output.map(Cow::into_owned))
    }

    pub(crate) fn output_schema(&self) -> Option<&SchemaRef> {
        self.stages.last().map(InlineBinding::output_schema)
    }

    fn apply<'input>(
        &mut self,
        mut current: Cow<'input, Change>,
    ) -> Result<Option<Cow<'input, Change>>, InlinePipelineError> {
        for (stage, transform) in self.stages.iter_mut().enumerate() {
            let Some(output) = transform
                .apply(current.as_ref())
                .map_err(|source| InlinePipelineError { stage, source })?
            else {
                return Ok(None);
            };
            current = Cow::Owned(output);
        }
        Ok(Some(current))
    }
}

pub(crate) struct InlinePipelineError {
    pub(crate) stage: usize,
    pub(crate) source: OperationError,
}
