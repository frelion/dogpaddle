use thiserror::Error;

/// An error returned by an [`Operation`](crate::Operation).
pub type OperationError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A durable flow construction or execution failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlowError {
    /// The backing store failed.
    #[error(transparent)]
    Store(#[from] dogpaddle_store::StoreError),

    /// A stage name is empty or already present.
    #[error("invalid or duplicate stage name {0:?}")]
    StageName(String),

    /// Operation data may not occupy Flow's internal namespace.
    #[error("data name {0:?} is reserved by Flow")]
    ReservedDataName(String),

    /// A stage declares an invalid or duplicate input port.
    #[error("stage {stage:?} declares invalid or duplicate input port {port:?}")]
    InputPort { stage: String, port: String },

    /// A stage handle belongs to another flow.
    #[error("stage belongs to another flow")]
    WrongFlow,

    /// The named input does not exist or is already connected.
    #[error("input {input:?} on stage {stage:?} is missing or already connected")]
    InputConnection { stage: String, input: String },

    /// The graph contains a directed cycle.
    #[error("flow graph contains a cycle")]
    Cycle,

    /// At least one declared input has no upstream stage.
    #[error("input {input:?} on stage {stage:?} is not connected")]
    UnconnectedInput { stage: String, input: String },

    /// Provisioned metadata does not match this declaration.
    #[error("persisted flow declaration does not match the current declaration")]
    DeclarationMismatch,

    /// Provisioning-only data was requested after execution began.
    #[error("flow provisioning is already finished")]
    AlreadyRunning,

    /// An operation failed and the stage was durably stopped.
    #[error("stage {stage:?} failed: {message}")]
    StageFailed { stage: String, message: String },

    /// An opaque persisted control value is corrupt.
    #[error("stage {stage:?} contains corrupt control state")]
    CorruptStage { stage: String },
}
