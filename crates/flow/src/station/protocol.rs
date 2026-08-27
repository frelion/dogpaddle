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
    #[error(
        "station input {input} cursor row {row_index} is outside the cached Change with {rows} rows"
    )]
    CursorRowOutOfRange {
        input: usize,
        row_index: u64,
        rows: usize,
    },
    #[error(
        "station input {input} is caught up at offset {offset}, but its cursor row is {row_index}"
    )]
    NonzeroRowAtTail {
        input: usize,
        offset: u64,
        row_index: u64,
    },
}
