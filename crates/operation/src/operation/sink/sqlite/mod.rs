mod definition;
mod error;
mod row;
mod target;

#[cfg(test)]
mod tests;

const TECHNICAL_ID: &str = "$dogpaddle.id";
const TECHNICAL_HASH: &str = "$dogpaddle.hash";

pub use definition::{SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkSchemaError};
pub use error::SqliteSinkError;

pub(crate) use definition::{TAG, decode_definition};
