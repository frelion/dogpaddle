use std::num::NonZeroU64;

use dogpaddle_operation::OperationDefinition;

#[derive(Debug)]
pub(crate) struct FlowDefinition {
    pub(super) stations: Vec<StationDefinition>,
}

#[derive(Debug)]
pub(crate) struct StationDefinition {
    pub(super) id: String,
    pub(super) operations: Vec<Box<dyn OperationDefinition>>,
    pub(super) output_capacity_bytes: Option<NonZeroU64>,
    pub(super) inputs: Vec<InputDefinition>,
}

#[derive(Debug)]
pub(crate) struct InputDefinition {
    pub(super) station_id: String,
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
            operations: vec![operation],
            output_capacity_bytes: None,
            inputs: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn operations(&self) -> &[Box<dyn OperationDefinition>] {
        &self.operations
    }

    pub(crate) fn first_operation(&self) -> &dyn OperationDefinition {
        self.operations
            .first()
            .expect("a validated Station program is nonempty")
            .as_ref()
    }

    pub(crate) fn input_count(&self) -> usize {
        usize::try_from(self.first_operation().kind().input_count())
            .expect("an Operation input count fits usize")
    }

    pub(crate) fn is_scan(&self) -> bool {
        self.first_operation().kind().is_scan()
    }

    pub(crate) fn is_sink(&self) -> bool {
        self.first_operation().kind().is_sink()
    }

    pub(crate) fn has_output(&self) -> bool {
        self.operations
            .last()
            .expect("a validated Station program is nonempty")
            .kind()
            .has_output()
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
}

impl InputDefinition {
    pub(super) const fn new(station_id: String) -> Self {
        Self { station_id }
    }

    pub(crate) fn station_id(&self) -> &str {
        &self.station_id
    }
}
