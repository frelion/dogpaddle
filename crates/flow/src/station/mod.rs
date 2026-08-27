mod input;
mod protocol;
mod runtime;

pub(crate) use protocol::{ProcessOutcome, StationError};
pub(crate) use runtime::{Station, StationParts};

#[cfg(test)]
use input::{CURSOR_ORIGIN, Input, cursor_key, decode_cursor, encode_cursor};

#[cfg(test)]
mod tests;
