use arrow_schema::SchemaRef;
use dogpaddle_operation::{OperationBindError, OperationSetupError, RuntimeResource};
use dogpaddle_store::{Cell, DataScope, StoreError, SubscribedLog};
use thiserror::Error;

use crate::{
    assembly::ResolvedTopology,
    error::{FlowError, operation_setup_error},
    station::StationParts,
};

use super::{FlowDefinition, codec};

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

pub(super) fn construct_stations(
    definition: &FlowDefinition,
    topology: &ResolvedTopology,
    data: &mut DataScope<'_>,
    resources: Vec<RuntimeResource>,
) -> Result<Vec<StationParts>, FlowError> {
    let mut resources = resources.into_iter().map(Some).collect::<Vec<_>>();
    let mut output_schemas = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<Option<SchemaRef>>>();
    let mut stations = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<Option<StationParts>>>();

    for &station_index in topology.schedule() {
        let station = &definition.stations()[station_index];
        let input_schemas = topology
            .inputs(station_index)
            .iter()
            .map(|producer| {
                output_schemas[*producer]
                    .as_ref()
                    .expect("a scheduled, validated input must have a constructed output Schema")
                    .clone()
            })
            .collect::<Vec<_>>();
        let active = (station.inputs().len() > 1)
            .then(|| data.data::<Cell<u32>>(&codec::station_active_input_name(station_index)))
            .transpose()
            .map_err(map_store_error)?;
        let mut station_resource = resources[station_index]
            .take()
            .expect("each Station resource is consumed exactly once");
        let mut operations = Vec::with_capacity(station.operations().len());
        let mut current_schema = None;

        for (operation_index, definition) in station.operations().iter().enumerate() {
            let inputs = if operation_index == 0 {
                input_schemas.as_slice()
            } else {
                std::slice::from_ref(
                    current_schema
                        .as_ref()
                        .expect("a validated intermediate Operation has an output Schema"),
                )
            };
            let resource = if operation_index == 0 {
                std::mem::take(&mut station_resource)
            } else {
                RuntimeResource::default()
            };
            let prefix = codec::station_operation_prefix(station_index, operation_index);
            let constructed = definition
                .construct(inputs, data, &prefix, resource)
                .map_err(|source| construction_error(station.id(), operation_index, source))?;
            let (operation, output_schema) = constructed.into_parts();
            operations.push(operation);
            current_schema = output_schema;
        }

        let output = match (station.output_capacity_bytes(), current_schema.as_ref()) {
            (Some(capacity), Some(schema)) => Some((
                data.data::<SubscribedLog<Vec<u8>>>(&codec::station_output_name(station_index))
                    .map_err(map_store_error)?,
                capacity,
                schema.clone(),
            )),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output capacity and constructed Schema must agree")
            }
        };
        output_schemas[station_index] = current_schema;
        stations[station_index] = Some(StationParts::new(
            active,
            station.input_count(),
            operations,
            output,
        ));
    }

    Ok(stations
        .into_iter()
        .map(|station| station.expect("every validated Station must be scheduled and constructed"))
        .collect())
}

fn construction_error(
    station_id: &str,
    operation: usize,
    source: OperationSetupError,
) -> FlowError {
    let source = match source {
        OperationSetupError::Bind(source) => {
            return FlowSchemaError::Operation {
                station_id: station_id.to_owned(),
                operation,
                source,
            }
            .into();
        }
        OperationSetupError::Schema { source } => {
            return FlowSchemaError::Operation {
                station_id: station_id.to_owned(),
                operation,
                source: OperationBindError::Rejected { source },
            }
            .into();
        }
        source => source,
    };
    operation_setup_error(station_id, operation, source)
}

fn map_store_error(source: StoreError) -> FlowError {
    match source {
        StoreError::DataNotFound(name) => FlowError::MissingResource { name },
        source => source.into(),
    }
}
