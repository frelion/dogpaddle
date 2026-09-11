use std::sync::Arc;

use arrow_schema::SchemaRef;
use dogpaddle_operation::{InlineBindError, InlineBinding, OperationBindError, OperationBinding};
use thiserror::Error;

use crate::assembly::ResolvedTopology;

use super::FlowDefinition;

/// Failure while binding one Station program to exact logical Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowSchemaError {
    /// The Station's durable core Operation rejected its ordered inputs.
    #[error("station {station_id:?} core has an invalid schema binding: {source}")]
    Core {
        /// Stable ID of the Station whose core binding failed.
        station_id: String,
        /// Operation-level binding failure.
        #[source]
        source: OperationBindError,
    },
    /// One stage on a specific input port rejected the preceding Schema.
    #[error(
        "station {station_id:?} input {port} inline stage {stage} has an invalid schema binding: {source}"
    )]
    InlineInput {
        /// Stable ID of the Station containing the stage.
        station_id: String,
        /// Zero-based input port.
        port: usize,
        /// Zero-based stage ordinal within this port.
        stage: usize,
        /// Inline binding failure.
        #[source]
        source: InlineBindError,
    },
    /// One stage before the durable output rejected the preceding Schema.
    #[error(
        "station {station_id:?} output inline stage {stage} has an invalid schema binding: {source}"
    )]
    InlineOutput {
        /// Stable ID of the Station containing the stage.
        station_id: String,
        /// Zero-based stage ordinal in the output pipeline.
        stage: usize,
        /// Inline binding failure.
        #[source]
        source: InlineBindError,
    },
}

impl FlowSchemaError {
    /// Returns the stable ID of the Station whose Schema binding failed.
    #[must_use]
    pub fn station_id(&self) -> &str {
        match self {
            Self::Core { station_id, .. }
            | Self::InlineInput { station_id, .. }
            | Self::InlineOutput { station_id, .. } => station_id,
        }
    }

    /// Returns the core Operation failure when the durable core was rejected.
    #[must_use]
    pub const fn operation_error(&self) -> Option<&OperationBindError> {
        match self {
            Self::Core { source, .. } => Some(source),
            Self::InlineInput { .. } | Self::InlineOutput { .. } => None,
        }
    }
}

pub(crate) struct StationBinding {
    core: OperationBinding,
    inputs: Vec<Vec<InlineBinding>>,
    output: Vec<InlineBinding>,
}

impl StationBinding {
    pub(crate) const fn core(&self) -> &OperationBinding {
        &self.core
    }

    pub(crate) fn output_schema(&self) -> Option<&SchemaRef> {
        self.output
            .last()
            .map(InlineBinding::output_schema)
            .or_else(|| self.core.output_schema())
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationBinding,
        Vec<Vec<InlineBinding>>,
        Vec<InlineBinding>,
    ) {
        (self.core, self.inputs, self.output)
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
        let mut input_bindings = Vec::with_capacity(station_definition.input_count());
        let mut core_input_schemas = Vec::with_capacity(station_definition.input_count());
        for (port, (&producer, input_definition)) in topology
            .inputs(station)
            .iter()
            .zip(station_definition.input_definitions())
            .enumerate()
        {
            let mut schema = bindings[producer]
                .as_ref()
                .and_then(StationBinding::output_schema)
                .expect("a scheduled, validated input must have a bound output Schema")
                .clone();
            let mut pipeline = Vec::with_capacity(input_definition.inline().len());
            for (stage, inline) in input_definition.inline().iter().enumerate() {
                let binding = inline.bind(Arc::clone(&schema)).map_err(|source| {
                    FlowSchemaError::InlineInput {
                        station_id: station_definition.id().to_owned(),
                        port,
                        stage,
                        source,
                    }
                })?;
                let output_schema = Arc::clone(binding.output_schema());
                pipeline.push(binding);
                schema = output_schema;
            }
            input_bindings.push(pipeline);
            core_input_schemas.push(schema);
        }

        let core = station_definition
            .core()
            .bind(&core_input_schemas)
            .map_err(|source| FlowSchemaError::Core {
                station_id: station_definition.id().to_owned(),
                source,
            })?;
        let mut output_schema = core.output_schema().cloned();
        let mut output_bindings = Vec::with_capacity(station_definition.output_inline().len());
        for (stage, inline) in station_definition.output_inline().iter().enumerate() {
            let schema = output_schema
                .take()
                .expect("validated output inline pipeline belongs to an output core");
            let binding = inline.bind(Arc::clone(&schema)).map_err(|source| {
                FlowSchemaError::InlineOutput {
                    station_id: station_definition.id().to_owned(),
                    stage,
                    source,
                }
            })?;
            output_schema = Some(Arc::clone(binding.output_schema()));
            output_bindings.push(binding);
        }
        bindings[station] = Some(StationBinding {
            core,
            inputs: input_bindings,
            output: output_bindings,
        });
    }

    Ok(bindings
        .into_iter()
        .map(|binding| binding.expect("every validated Station must be scheduled and bound"))
        .collect())
}
