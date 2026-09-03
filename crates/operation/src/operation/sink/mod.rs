//! Sink operations that consume records without producing downstream output.

pub(crate) mod discard;
pub(crate) mod sqlite;

pub use discard::{DiscardDefinition, DiscardError, DiscardOperation};
pub use sqlite::{
    SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkError, SqliteSinkOperation,
    SqliteSinkSchemaError,
};
