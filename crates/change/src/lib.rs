#![doc = include_str!("../README.md")]

mod change;
mod codec;
mod projection;
mod schema;

pub use change::{Change, ChangeError};
pub use codec::{
    CodecError, decode_change, decode_change_owned, decode_change_projected, encode_change,
    encode_change_bounded,
};
pub use projection::{ChangeProjection, ProjectionError};
pub use schema::{
    MAX_NESTING_DEPTH, MAX_SCHEMA_FIELDS, MAX_SCHEMA_METADATA_ENTRIES, MAX_SCHEMA_TEXT_BYTES,
    SchemaError, validate_schema,
};
