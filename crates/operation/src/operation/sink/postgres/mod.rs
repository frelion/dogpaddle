//! PostgreSQL-specific target support for the relation sink.

mod config;
mod definition;
mod error;
mod row;
mod schema;
mod target;

pub use config::{PostgresSinkConfig, PostgresTargetSpec};
pub use definition::PostgresSinkDefinition;
pub use error::{PostgresSinkError, PostgresSinkSchemaError};

pub(crate) use definition::{TAG, decode_definition};

#[cfg(test)]
mod tests;
