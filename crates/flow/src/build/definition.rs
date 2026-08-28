use std::num::NonZeroU64;

use dogpaddle_operation::OperationDefinition;

#[derive(Debug)]
pub(crate) struct FlowDefinition {
    pub(super) stations: Vec<StationDefinition>,
}

#[derive(Debug)]
pub(crate) struct StationDefinition {
    pub(super) id: String,
    pub(super) operation: Box<dyn OperationDefinition>,
    pub(super) output_capacity_bytes: Option<NonZeroU64>,
    pub(super) sources: Vec<String>,
}

impl FlowDefinition {
    pub(super) const fn new(stations: Vec<StationDefinition>) -> Self {
        Self { stations }
    }

    pub(crate) fn stations(&self) -> &[StationDefinition] {
        &self.stations
    }
}

impl StationDefinition {
    pub(super) fn new(id: String, operation: Box<dyn OperationDefinition>) -> Self {
        Self {
            id,
            operation,
            output_capacity_bytes: None,
            sources: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn operation(&self) -> &dyn OperationDefinition {
        self.operation.as_ref()
    }

    pub(crate) fn input_count(&self) -> usize {
        self.operation.input_count()
    }

    pub(crate) fn is_source(&self) -> bool {
        self.operation.category().is_source()
    }

    pub(crate) fn is_sink(&self) -> bool {
        self.operation.category().is_sink()
    }

    pub(crate) fn has_output(&self) -> bool {
        self.operation.category().has_output()
    }

    pub(crate) const fn output_capacity_bytes(&self) -> Option<NonZeroU64> {
        self.output_capacity_bytes
    }

    pub(crate) fn sources(&self) -> impl ExactSizeIterator<Item = &str> {
        self.sources.iter().map(String::as_str)
    }
}
