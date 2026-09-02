mod input;
mod protocol;
mod runtime;

pub(crate) use input::ConsumerCursor;
pub(crate) use protocol::StationError;
pub(crate) use runtime::{Station, StationParts};

#[cfg(test)]
use input::{
    ACTIVE_INPUT_KEY, CURSOR_ORIGIN, CompletionPlan, Output, cursor_key, decode_active_input,
    decode_cursor, encode_active_input, encode_cursor, plan_complete,
};

#[cfg(test)]
mod tests;
