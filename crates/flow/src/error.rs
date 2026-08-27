use thiserror::Error;

use dogpaddle_operation::MaterializeError;
use dogpaddle_store::StoreError;

use crate::{
    build::{FlowDefinitionError, TopologyError},
    station::StationError,
};

/// Failure while building or opening a persistent Flow.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowError {
    /// The declared topology is invalid.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// The durable Flow definition cannot be encoded or decoded.
    #[error(transparent)]
    Definition(#[from] FlowDefinitionError),
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
}

/// Failure while advancing one Station in a running Flow.
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
}
