use dogpaddle_change::CodecError as ChangeCodecError;
use dogpaddle_store::StoreError;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "station process outcomes are returned by the future scheduling phase"
    )
)]
pub(crate) enum ProcessOutcome {
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
    #[error("station input {input} has no durable cursor")]
    MissingCursor { input: usize },
    #[error("station input {input} has a malformed durable cursor")]
    MalformedCursor { input: usize },
}
