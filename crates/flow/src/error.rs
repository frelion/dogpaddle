use thiserror::Error;

use dogpaddle_operation::DefinitionCodecError;
use dogpaddle_store::StoreError;

use crate::TopologyError;

/// Failure while encoding or decoding a durable Flow definition.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowDefinitionError {
    /// The encoded definition ends before all declared fields are present.
    #[error("flow definition is truncated")]
    Truncated,
    /// The encoded bytes do not begin with the `DogPaddle` Flow marker.
    #[error("flow definition marker is invalid")]
    InvalidMagic,
    /// The Flow definition format version is unsupported.
    #[error("unsupported flow definition format version {0}")]
    UnsupportedVersion(u16),
    /// A stage or source ID is not valid UTF-8.
    #[error("flow definition contains an invalid UTF-8 stage ID")]
    InvalidUtf8,
    /// A length cannot be represented by the durable format.
    #[error("{0} is too large for the flow definition format")]
    LengthOverflow(&'static str),
    /// A source ID does not identify a declared stage.
    #[error("stage {stage:?} references unknown source {source_id:?}")]
    UnknownSource {
        /// Stage containing the invalid source reference.
        stage: String,
        /// Missing source ID.
        source_id: String,
    },
    /// One operation definition is invalid or unsupported.
    #[error(transparent)]
    Operation(#[from] DefinitionCodecError),
    /// The persisted checksum does not match the definition bytes.
    #[error("flow definition checksum does not match its contents")]
    IntegrityMismatch,
    /// The decoded graph violates topology rules.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// Bytes remain after the complete definition.
    #[error("flow definition contains trailing bytes")]
    TrailingBytes,
}

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
    /// A Store exists, but no complete Flow definition was published.
    #[error("flow build is incomplete")]
    IncompleteBuild,
    /// The definition changed between the two phases of opening the Flow.
    #[error("flow definition changed while it was being opened")]
    DefinitionChangedDuringOpen,
    /// A published definition references a required resource that is absent.
    #[error("published flow is missing required resource {name:?}")]
    MissingResource {
        /// Stable Store namespace that could not be opened.
        name: String,
    },
}
