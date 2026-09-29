use dogpaddle_change::CodecError as ChangeCodecError;
use dogpaddle_operation::operation::{OperationError, PostCommitError};
use dogpaddle_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum StationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("operation {operation} failed: {source}")]
    Operation {
        operation: usize,
        #[source]
        source: OperationError,
    },
    #[error("Store commit failed; station must be reopened: {source}")]
    Commit {
        #[source]
        source: StoreError,
    },
    #[error("Store durability barrier failed; affected stations must be reopened: {source}")]
    DurabilityBarrier {
        #[source]
        source: StoreError,
    },
    #[error("operation failed after its Store transaction committed: {source}")]
    AfterCommit {
        #[source]
        source: PostCommitError,
    },
    #[error("station must be reopened after an uncertain commit or post-commit failure")]
    NeedsReopen,
    #[error("station input {input} contains an invalid Change: {source}")]
    InvalidInputChange {
        input: usize,
        #[source]
        source: ChangeCodecError,
    },
    #[error("station has inputs but no durable active input")]
    MissingActiveInput,
    #[error("station durable active input {input} is outside input count {input_count}")]
    ActiveInputOutOfRange { input: usize, input_count: usize },
    #[error("an Operation returned Complete without an offered input")]
    OperationCompletedWithoutInput,
    #[error("operation produced output for a Station without an output stream")]
    UnexpectedOutput,
    #[error("operation produced a Change that cannot be encoded: {source}")]
    InvalidOutputChange {
        #[source]
        source: ChangeCodecError,
    },
}

impl StationError {
    pub(crate) const fn requires_reopen(&self) -> bool {
        matches!(
            self,
            Self::Commit { .. }
                | Self::DurabilityBarrier { .. }
                | Self::AfterCommit { .. }
                | Self::NeedsReopen
        )
    }
}
