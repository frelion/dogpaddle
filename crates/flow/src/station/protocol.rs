use arrow_schema::SchemaRef;
use dogpaddle_change::CodecError as ChangeCodecError;
use dogpaddle_operation::operation::{OperationError, PostCommitError};
use dogpaddle_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum StationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Operation(#[from] OperationError),
    #[error("station input {input} inline stage {stage} failed: {source}")]
    InlineInput {
        input: usize,
        stage: usize,
        #[source]
        source: OperationError,
    },
    #[error("station output inline stage {stage} failed: {source}")]
    InlineOutput {
        stage: usize,
        #[source]
        source: OperationError,
    },
    #[error("Store commit failed; station must be reopened: {source}")]
    Commit {
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
    #[error(
        "station input {input} Schema does not match its binding: expected {expected:?}, actual {actual:?}"
    )]
    InputSchemaMismatch {
        input: usize,
        expected: SchemaRef,
        actual: SchemaRef,
    },
    #[error("station has inputs but no durable active input")]
    MissingActiveInput,
    #[error("station durable active input {input} is outside input count {input_count}")]
    ActiveInputOutOfRange { input: usize, input_count: usize },
    #[error("an input-free Operation returned Complete")]
    OperationCompletedWithoutInput,
    #[error("operation produced output for a Station without an output stream")]
    UnexpectedOutput,
    #[error("operation produced a Change that cannot be encoded: {source}")]
    InvalidOutputChange {
        #[source]
        source: ChangeCodecError,
    },
    #[error(
        "operation output Schema does not match its binding: expected {expected:?}, actual {actual:?}"
    )]
    OutputSchemaMismatch {
        expected: SchemaRef,
        actual: SchemaRef,
    },
}

impl StationError {
    pub(crate) const fn requires_reopen(&self) -> bool {
        matches!(
            self,
            Self::Commit { .. } | Self::AfterCommit { .. } | Self::NeedsReopen
        )
    }
}
