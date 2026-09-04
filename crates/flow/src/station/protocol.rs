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
    #[error("operation failed after its Store transaction committed: {source}")]
    AfterCommit {
        #[source]
        source: PostCommitError,
    },
    #[error("station must be reopened after a post-commit failure")]
    NeedsReopen,
    #[error("station input {input} contains an invalid Change: {source}")]
    InvalidInputChange {
        input: usize,
        #[source]
        source: ChangeCodecError,
    },
    #[error(
        "station input {input} Schema does not match its bound output: expected {expected:?}, actual {actual:?}"
    )]
    InputSchemaMismatch {
        input: usize,
        expected: SchemaRef,
        actual: SchemaRef,
    },
    #[error("station has inputs but no durable active input")]
    MissingActiveInput,
    #[error("station durable active input is malformed")]
    MalformedActiveInput,
    #[error("station durable active input {input} is outside input count {input_count}")]
    ActiveInputOutOfRange { input: usize, input_count: usize },
    #[error("station input {input} has no durable cursor")]
    MissingCursor { input: usize },
    #[error("station input {input} has a malformed durable cursor")]
    MalformedCursor { input: usize },
    #[error("claimed input offset {claimed} does not match durable consumer cursor {durable}")]
    ClaimCursorMismatch { claimed: u64, durable: u64 },
    #[error("claimed input port {claimed} does not match durable active input port {durable}")]
    ClaimActiveInputMismatch { claimed: usize, durable: usize },
    #[error("claimed input offset {offset} is at output tail {tail}")]
    ClaimAtTail { offset: u64, tail: u64 },
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
    #[error("output consumer {consumer} has no durable cursor")]
    MissingConsumerCursor { consumer: usize },
    #[error("output consumer {consumer} has a malformed durable cursor")]
    MalformedConsumerCursor { consumer: usize },
    #[error(
        "output consumer {consumer} cursor {offset} is outside retained range [{head}, {tail}]"
    )]
    ConsumerCursorOutOfRange {
        consumer: usize,
        offset: u64,
        head: u64,
        tail: u64,
    },
    #[error("output retention head {head} does not equal minimum consumer cursor {minimum}")]
    RetentionHeadMismatch { head: u64, minimum: u64 },
    #[error("output retention truncated to {actual} instead of target {target}")]
    RetentionTruncateMismatch { target: u64, actual: u64 },
}

impl StationError {
    pub(crate) const fn requires_reopen(&self) -> bool {
        matches!(self, Self::AfterCommit { .. } | Self::NeedsReopen)
    }
}
