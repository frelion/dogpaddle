use dogpaddle_operation::{OperationBindError, OperationBinding};
use thiserror::Error;

use crate::assembly::ResolvedTopology;

use super::FlowDefinition;

/// Failure while binding one Station to its exact ordered input Schemas.
#[derive(Debug, Error)]
#[error("station {station_id:?} has an invalid schema binding: {source}")]
pub struct FlowSchemaError {
    station_id: String,
    #[source]
    source: OperationBindError,
}

impl FlowSchemaError {
    /// Returns the stable ID of the Station whose Schema binding failed.
    #[must_use]
    pub fn station_id(&self) -> &str {
        &self.station_id
    }

    /// Returns the Operation-level binding failure.
    #[must_use]
    pub const fn operation_error(&self) -> &OperationBindError {
        &self.source
    }
}

pub(super) fn bind_operations(
    definition: &FlowDefinition,
    topology: &ResolvedTopology,
) -> Result<Vec<OperationBinding>, FlowSchemaError> {
    let mut bindings = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<_>>();

    for &station in topology.schedule() {
        let input_schemas = topology
            .sources(station)
            .iter()
            .map(|&source| {
                bindings[source]
                    .as_ref()
                    .and_then(OperationBinding::output_schema)
                    .expect("a scheduled, validated upstream must have a bound output Schema")
                    .clone()
            })
            .collect::<Vec<_>>();
        let station_definition = &definition.stations()[station];
        let binding = station_definition
            .operation()
            .bind(&input_schemas)
            .map_err(|source| FlowSchemaError {
                station_id: station_definition.id().to_owned(),
                source,
            })?;
        bindings[station] = Some(binding);
    }

    Ok(bindings
        .into_iter()
        .map(|binding| binding.expect("every validated Station must be scheduled and bound"))
        .collect())
}
