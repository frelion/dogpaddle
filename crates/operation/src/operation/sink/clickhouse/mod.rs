//! ClickHouse-specific target support for the relation sink.

mod config;
mod definition;
mod error;
mod row;
mod schema;
mod target;

pub use config::{ClickHouseSinkConfig, ClickHouseTargetSpec};
pub use definition::ClickHouseSinkDefinition;
pub use error::{ClickHouseSinkError, ClickHouseSinkSchemaError};

pub(crate) use definition::{TAG, decode_definition};

use super::{buffered, relation};

#[cfg(test)]
mod tests;
