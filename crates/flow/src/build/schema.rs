use arrow_schema::SchemaRef;
use dogpaddle_operation::{OperationBindError, OperationBinding};
use thiserror::Error;

use crate::assembly::ResolvedTopology;

use super::FlowDefinition;

/// Failure while binding one Operation in a Station's linear program.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowSchemaError {
    /// One Operation rejected its exact ordered inputs.
    #[error("station {station_id:?} operation {operation} has an invalid schema binding: {source}")]
    Operation {
        /// Stable ID of the Station whose binding failed.
        station_id: String,
        /// Zero-based Operation ordinal inside the Station.
        operation: usize,
        /// Operation-level binding failure.
        #[source]
        source: OperationBindError,
    },
}

impl FlowSchemaError {
    /// Returns the stable ID of the Station whose Schema binding failed.
    #[must_use]
    pub fn station_id(&self) -> &str {
        match self {
            Self::Operation { station_id, .. } => station_id,
        }
    }

    /// Returns the zero-based ordinal of the Operation whose binding failed.
    #[must_use]
    pub const fn operation_index(&self) -> usize {
        match self {
            Self::Operation { operation, .. } => *operation,
        }
    }

    /// Returns the Operation binding failure.
    #[must_use]
    pub const fn operation_error(&self) -> &OperationBindError {
        match self {
            Self::Operation { source, .. } => source,
        }
    }
}

pub(crate) struct StationBinding {
    operations: Vec<OperationBinding>,
}

impl StationBinding {
    pub(crate) fn operations(&self) -> &[OperationBinding] {
        &self.operations
    }

    pub(crate) fn output_schema(&self) -> Option<&SchemaRef> {
        self.operations
            .last()
            .expect("a validated Station binding is nonempty")
            .output_schema()
    }

    pub(crate) fn into_operations(self) -> Vec<OperationBinding> {
        self.operations
    }
}

pub(super) fn bind_operations(
    definition: &FlowDefinition,
    topology: &ResolvedTopology,
) -> Result<Vec<StationBinding>, FlowSchemaError> {
    let mut bindings = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<_>>();

    for &station in topology.schedule() {
        let station_definition = &definition.stations()[station];
        let input_schemas = topology
            .inputs(station)
            .iter()
            .map(|producer| {
                bindings[*producer]
                    .as_ref()
                    .and_then(StationBinding::output_schema)
                    .expect("a scheduled, validated input must have a bound output Schema")
                    .clone()
            })
            .collect::<Vec<_>>();

        let mut operation_bindings = Vec::with_capacity(station_definition.operations().len());
        for (operation, definition) in station_definition.operations().iter().enumerate() {
            let binding_inputs = if operation == 0 {
                input_schemas.as_slice()
            } else {
                let schema = operation_bindings
                    .last()
                    .and_then(OperationBinding::output_schema)
                    .expect("a validated intermediate Operation has an output Schema");
                std::slice::from_ref(schema)
            };
            let binding =
                definition
                    .bind(binding_inputs)
                    .map_err(|source| FlowSchemaError::Operation {
                        station_id: station_definition.id().to_owned(),
                        operation,
                        source,
                    })?;
            operation_bindings.push(binding);
        }
        bindings[station] = Some(StationBinding {
            operations: operation_bindings,
        });
    }

    Ok(bindings
        .into_iter()
        .map(|binding| binding.expect("every validated Station must be scheduled and bound"))
        .collect())
}
