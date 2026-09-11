use std::num::NonZeroU64;

use dogpaddle_operation::{InlineDefinition, OperationDefinition};

#[derive(Debug)]
pub(crate) struct FlowDefinition {
    pub(super) stations: Vec<StationDefinition>,
}

#[derive(Debug)]
pub(crate) struct StationDefinition {
    pub(super) id: String,
    pub(super) core: Box<dyn OperationDefinition>,
    pub(super) output_capacity_bytes: Option<NonZeroU64>,
    pub(super) inputs: Vec<InputDefinition>,
    pub(super) output_inline: Vec<InlineDefinition>,
}

#[derive(Debug)]
pub(crate) struct InputDefinition {
    pub(super) station_id: String,
    pub(super) inline: Vec<InlineDefinition>,
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
    pub(super) fn new(id: String, core: Box<dyn OperationDefinition>) -> Self {
        Self {
            id,
            core,
            output_capacity_bytes: None,
            inputs: Vec::new(),
            output_inline: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn core(&self) -> &dyn OperationDefinition {
        self.core.as_ref()
    }

    pub(crate) fn input_count(&self) -> usize {
        usize::try_from(self.core.kind().input_count())
            .expect("an Operation input count fits usize")
    }

    pub(crate) fn is_scan(&self) -> bool {
        self.core.kind().is_scan()
    }

    pub(crate) fn is_sink(&self) -> bool {
        self.core.kind().is_sink()
    }

    pub(crate) fn has_output(&self) -> bool {
        self.core.kind().has_output()
    }

    pub(crate) const fn output_capacity_bytes(&self) -> Option<NonZeroU64> {
        self.output_capacity_bytes
    }

    pub(crate) fn inputs(&self) -> impl ExactSizeIterator<Item = &str> {
        self.inputs.iter().map(InputDefinition::station_id)
    }

    pub(crate) fn input_definitions(&self) -> &[InputDefinition] {
        &self.inputs
    }

    pub(crate) fn output_inline(&self) -> &[InlineDefinition] {
        &self.output_inline
    }
}

impl InputDefinition {
    pub(super) const fn new(station_id: String, inline: Vec<InlineDefinition>) -> Self {
        Self { station_id, inline }
    }

    pub(crate) fn station_id(&self) -> &str {
        &self.station_id
    }

    pub(crate) fn inline(&self) -> &[InlineDefinition] {
        &self.inline
    }
}
