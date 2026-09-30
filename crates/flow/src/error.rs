use crate::build::{FlowDefinitionError, TopologyError};
use dogpaddle_operation::{OperationBindError, OperationSetupError, operation::OperationError};
use dogpaddle_store::StoreError;
use thiserror::Error;

/// Failure while building or opening a persistent Flow.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowError {
    /// Open obtains the graph exclusively from durable state.
    #[error("opening a flow does not accept operation declarations")]
    OpenWithDefinition,
    /// The durable owner identity differs from the expected identity.
    #[error("persistent flow owner identity does not match")]
    OwnerIdentityMismatch,
    /// One Operation received duplicate ephemeral resources.
    #[error("operation {operation_id:?} has more than one runtime resource")]
    DuplicateRuntimeResource {
        /// Stable Operation ID.
        operation_id: String,
    },
    /// No Operation owns the supplied resource.
    #[error("runtime resource targets unknown operation {operation_id:?}")]
    UnknownRuntimeResource {
        /// Unknown Operation ID.
        operation_id: String,
    },
    /// The resource is missing or has the wrong concrete type.
    #[error("operation {operation_id:?} runtime resource is invalid: {source}")]
    RuntimeResource {
        /// Stable Operation ID.
        operation_id: String,
        /// Concrete resource mismatch.
        #[source]
        source: OperationSetupError,
    },
    /// The graph is invalid.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// The durable definition is invalid.
    #[error(transparent)]
    Definition(#[from] FlowDefinitionError),
    /// Exact input schemas failed binding.
    #[error("operation {operation_id:?} has an invalid schema: {source}")]
    Schema {
        /// Stable Operation ID.
        operation_id: String,
        /// Binding failure.
        #[source]
        source: OperationBindError,
    },
    /// Store access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Output schema could not bind a Change codec.
    #[error("operation {operation_id:?} has an invalid output codec: {source}")]
    OutputCodec {
        /// Stable Operation ID.
        operation_id: String,
        /// Codec error.
        #[source]
        source: dogpaddle_change::CodecError,
    },
    /// Operation construction failed.
    #[error("operation {operation_id:?} setup failed: {source}")]
    OperationSetup {
        /// Stable Operation ID.
        operation_id: String,
        /// Construction error.
        #[source]
        source: OperationSetupError,
    },
    /// No complete definition was published.
    #[error("flow build is incomplete")]
    IncompleteBuild,
    /// A published resource is absent.
    #[error("published flow is missing resource {name:?}")]
    MissingResource {
        /// Exact catalog name.
        name: String,
    },
    /// The durable call stack or boundary state is corrupt.
    #[error("invalid flow runtime state: {reason}")]
    InvalidRuntimeState {
        /// Concrete invariant violation.
        reason: String,
    },
}

pub(crate) fn setup_error(id: &str, source: OperationSetupError) -> FlowError {
    let operation_id = id.to_owned();
    match source {
        OperationSetupError::MissingRuntimeResource
        | OperationSetupError::WrongRuntimeResource
        | OperationSetupError::UnexpectedRuntimeResource => FlowError::RuntimeResource {
            operation_id,
            source,
        },
        OperationSetupError::Store(source) => store_error(source),
        OperationSetupError::Bind(source) => FlowError::Schema {
            operation_id,
            source,
        },
        OperationSetupError::Schema { source } => FlowError::Schema {
            operation_id,
            source: OperationBindError::Rejected { source },
        },
        source => FlowError::OperationSetup {
            operation_id,
            source,
        },
    }
}
pub(crate) fn store_error(source: StoreError) -> FlowError {
    match source {
        StoreError::DataNotFound(name) => FlowError::MissingResource { name },
        source => FlowError::Store(source),
    }
}

/// Failure in a scheduling round, attributed to its logical Operation.
#[derive(Debug, Error)]
#[error("operation {operation_id:?} failed: {source}")]
pub struct FlowRunError {
    operation_id: String,
    #[source]
    source: OperationError,
    requires_reopen: bool,
}
impl FlowRunError {
    pub(crate) fn new(id: &str, source: OperationError, requires_reopen: bool) -> Self {
        Self {
            operation_id: id.to_owned(),
            source,
            requires_reopen,
        }
    }
    /// Returns the responsible logical Operation's stable ID.
    #[must_use]
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    /// Whether commit, durability or external-effect uncertainty requires reopening.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        self.requires_reopen
    }
}
