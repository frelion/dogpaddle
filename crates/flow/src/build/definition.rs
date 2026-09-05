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
    pub(super) inputs: Vec<String>,
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
            inputs: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn operation(&self) -> &dyn OperationDefinition {
        self.operation.as_ref()
    }

    pub(crate) fn input_count(&self) -> usize {
        usize::try_from(self.operation.kind().input_count())
            .expect("an Operation input count fits usize")
    }

    pub(crate) fn is_scan(&self) -> bool {
        self.operation.kind().is_scan()
    }

    pub(crate) fn is_sink(&self) -> bool {
        self.operation.kind().is_sink()
    }

    pub(crate) fn has_output(&self) -> bool {
        self.operation.kind().has_output()
    }

    pub(crate) const fn output_capacity_bytes(&self) -> Option<NonZeroU64> {
        self.output_capacity_bytes
    }

    pub(crate) fn inputs(&self) -> impl ExactSizeIterator<Item = &str> {
        self.inputs.iter().map(String::as_str)
    }
}
