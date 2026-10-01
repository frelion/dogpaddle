//! Apache Doris-specific target support for the relation sink.

mod config;
mod definition;
mod error;
mod row;
mod schema;
mod target;

pub use config::{DorisSinkConfig, DorisTargetSpec};
pub use definition::DorisSinkDefinition;
pub use error::{DorisSinkError, DorisSinkSchemaError};

use super::buffered;

#[cfg(test)]
mod tests;
