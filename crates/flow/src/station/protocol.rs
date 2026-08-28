use dogpaddle_change::CodecError as ChangeCodecError;
use dogpaddle_operation::operation::OperationError;
use dogpaddle_store::StoreError;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessOutcome {
    Idle,
    Progressed,
}

#[derive(Debug, Error)]
pub(crate) enum StationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Operation(#[from] OperationError),
    #[error("station input {input} contains an invalid Change: {source}")]
    InvalidInputChange {
        input: usize,
        #[source]
        source: ChangeCodecError,
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
    #[error("cached station input {input} offset {cached} does not match durable cursor {durable}")]
    CachedCursorMismatch {
        input: usize,
        cached: u64,
        durable: u64,
    },
    #[error(
        "cached station input port {cached} does not match durable active input port {durable}"
    )]
    CachedActiveInputMismatch { cached: usize, durable: usize },
    #[error(
        "operation input progress shape does not match its invocation: offered input {offered_input}, returned progress {returned_input}"
    )]
    OperationInputProgressMismatch {
        offered_input: bool,
        returned_input: bool,
    },
    #[error("operation produced output for a Station without an output stream")]
    UnexpectedOutput,
    #[error("operation produced a Change that cannot be encoded: {source}")]
    InvalidOutputChange {
        #[source]
        source: ChangeCodecError,
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
}
