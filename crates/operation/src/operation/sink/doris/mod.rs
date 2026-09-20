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

pub(crate) use definition::{TAG, decode_definition};

use super::{buffered, relation};

#[cfg(test)]
mod tests;
