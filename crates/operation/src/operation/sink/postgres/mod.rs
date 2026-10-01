//! PostgreSQL-specific target support for the relation sink.

mod config;
mod definition;
mod error;
mod row;
mod schema;
mod target;

pub use config::PostgresSinkConfig;
pub use definition::PostgresSinkDefinition;
pub use error::{PostgresSinkError, PostgresSinkSchemaError};

use super::buffered;

#[cfg(test)]
mod tests;
