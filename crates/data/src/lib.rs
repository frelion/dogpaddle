#![doc = include_str!("../README.md")]

mod change;
mod codec;
mod projection;
mod schema;

pub use change::{Change, ChangeError};
pub use codec::{CodecError, decode_change, decode_change_projected, encode_change};
pub use projection::{ChangeProjection, ProjectionError};
pub use schema::{MAX_NESTING_DEPTH, SchemaError, validate_schema};

#[cfg(test)]
mod tests;
