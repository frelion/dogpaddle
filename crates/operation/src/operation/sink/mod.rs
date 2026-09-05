//! Sink operations that consume records without producing downstream output.

mod relation;

pub(crate) mod discard;
pub(crate) mod postgres;
pub(crate) mod sqlite;

pub use discard::{DiscardDefinition, DiscardError, DiscardOperation};
pub use postgres::{
    PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkOperation,
    PostgresSinkSchemaError, PostgresTargetSpec,
};
pub use sqlite::{
    SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkError, SqliteSinkOperation,
    SqliteSinkSchemaError,
};
