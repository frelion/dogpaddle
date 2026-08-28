use dogpaddle_change::CodecError as ChangeCodecError;
use dogpaddle_store::StoreError;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessOutcome {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Station readiness awaits the processing protocol")
    )]
    Idle,
    Progressed,
}

#[derive(Debug, Error)]
pub(crate) enum StationError {
    #[error(transparent)]
    Store(#[from] StoreError),
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
