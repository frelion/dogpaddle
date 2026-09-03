mod definition;
mod row;
mod runtime;
mod state;

#[cfg(test)]
mod tests;

const TECHNICAL_ID: &str = "$dogpaddle.id";
const TECHNICAL_HASH: &str = "$dogpaddle.hash";

pub use definition::{SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkSchemaError};
pub use runtime::{SqliteSinkError, SqliteSinkOperation};

pub(crate) use definition::{TAG, decode_definition};
