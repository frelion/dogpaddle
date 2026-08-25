use dogpaddle_operation::OperationDefinition;

#[derive(Debug)]
pub(crate) struct FlowDefinition {
    pub(super) stages: Vec<StageDefinition>,
}

#[derive(Debug)]
pub(crate) struct StageDefinition {
    pub(super) id: String,
    pub(super) operation: Box<dyn OperationDefinition>,
    pub(super) sources: Vec<String>,
}

impl FlowDefinition {
    pub(super) const fn new(stages: Vec<StageDefinition>) -> Self {
        Self { stages }
    }

    pub(crate) fn stages(&self) -> &[StageDefinition] {
        &self.stages
    }
}

impl StageDefinition {
    pub(super) fn new(id: String, operation: Box<dyn OperationDefinition>) -> Self {
        Self {
            id,
            operation,
            sources: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn operation(&self) -> &dyn OperationDefinition {
        self.operation.as_ref()
    }

    pub(crate) fn sources(&self) -> impl ExactSizeIterator<Item = &str> {
        self.sources.iter().map(String::as_str)
    }
}
