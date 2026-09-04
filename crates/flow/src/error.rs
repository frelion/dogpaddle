use thiserror::Error;

use dogpaddle_operation::MaterializeError;
use dogpaddle_store::StoreError;

use crate::{
    build::{FlowDefinitionError, FlowSchemaError, TopologyError},
    station::StationError,
};

/// Failure while building or opening a persistent Flow.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowError {
    /// Opening uses the durable Definition, never declarations on the factory.
    #[error("opening a flow does not accept station, connection, or capacity declarations")]
    OpenWithDefinition,
    /// A Station received more than one ephemeral resource.
    #[error("station {station_id:?} has more than one runtime resource")]
    DuplicateRuntimeResource {
        /// Stable Station ID supplied by the caller.
        station_id: String,
    },
    /// A resource targets a Station absent from the durable graph.
    #[error("runtime resource targets unknown station {station_id:?}")]
    UnknownRuntimeResource {
        /// Unknown Station ID supplied by the caller.
        station_id: String,
    },
    /// An Operation's ephemeral resource is absent or has the wrong type.
    #[error("station {station_id:?} runtime resource is invalid: {source}")]
    RuntimeResource {
        /// Stable Station ID requiring the resource.
        station_id: String,
        /// Exact resource mismatch without exposing its contents.
        #[source]
        source: MaterializeError,
    },
    /// The declared topology is invalid.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// The durable Flow definition cannot be encoded or decoded.
    #[error(transparent)]
    Definition(#[from] FlowDefinitionError),
    /// One Station rejected the exact Schemas supplied by its upstreams.
    #[error(transparent)]
    Schema(#[from] FlowSchemaError),
    /// Store creation, lookup, transaction, or persistence failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Provided typed data instances do not match an operation definition.
    #[error(transparent)]
    Materialize(#[from] MaterializeError),
    /// A Store exists, but no complete Flow definition was published.
    #[error("flow build is incomplete")]
    IncompleteBuild,
    /// The definition changed between the two phases of opening the Flow.
    #[error("flow definition changed while it was being opened")]
    DefinitionChangedDuringOpen,
    /// A published definition references a required resource that is absent.
    #[error("published flow is missing required resource {name:?}")]
    MissingResource {
        /// Stable Store data object name that could not be opened.
        name: String,
    },
    /// Published runtime frontiers violate the output-retention invariant.
    #[error("station {station_id:?} has invalid runtime state: {reason}")]
    InvalidRuntimeState {
        /// Stable producer Station ID whose output retention is invalid.
        station_id: String,
        /// Concrete invariant violation detected while reopening.
        reason: String,
    },
}

pub(crate) fn retention_open_error(station_id: &str, source: StationError) -> FlowError {
    match source {
        StationError::Store(source) => FlowError::Store(source),
        source => FlowError::InvalidRuntimeState {
            station_id: station_id.to_owned(),
            reason: source.to_string(),
        },
    }
}

/// Failure during one Station turn.
#[derive(Debug, Error)]
#[error("station {station_id:?} failed: {source}")]
pub struct FlowRunError {
    station_id: String,
    #[source]
    source: StationError,
}

impl FlowRunError {
    pub(crate) fn new(station_id: &str, source: StationError) -> Self {
        Self {
            station_id: station_id.to_owned(),
            source,
        }
    }

    /// Returns the stable ID of the Station whose turn failed.
    #[must_use]
    pub fn station_id(&self) -> &str {
        &self.station_id
    }

    /// Returns whether this runtime must be reopened before scheduling can
    /// continue.
    ///
    /// This is true both for the originating post-commit callback failure and
    /// for later calls rejected by the runtime's fail-stop guard. Stations
    /// earlier in the originating scheduling round may already have committed.
    #[must_use]
    pub fn requires_reopen(&self) -> bool {
        self.source.requires_reopen()
    }
}
