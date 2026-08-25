use thiserror::Error;

use dogpaddle_operation::MaterializeError;
use dogpaddle_store::StoreError;

use crate::build::{FlowDefinitionError, TopologyError};

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
