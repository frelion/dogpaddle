#![doc = include_str!("../README.md")]

mod change;
mod codec;
mod schema;

pub use change::{Change, ChangeError};
pub use codec::{CodecError, decode_change, encode_change};
pub use schema::{MAX_NESTING_DEPTH, SchemaError, validate_schema};

#[cfg(test)]
mod tests;
