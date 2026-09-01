mod input;
mod protocol;
mod runtime;

pub(crate) use input::{ConsumerCursor, InputPort, OutputRetention};
pub(crate) use protocol::StationError;
pub(crate) use runtime::{Station, StationParts};

#[cfg(test)]
use input::{
    ACTIVE_INPUT_KEY, CURSOR_ORIGIN, cursor_key, decode_active_input, decode_cursor,
    encode_active_input, encode_cursor,
};

#[cfg(test)]
mod tests;
