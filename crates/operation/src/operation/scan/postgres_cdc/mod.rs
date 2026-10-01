//! Single-table, fixed-Schema `PostgreSQL` WAL scan backed by Debezium.

mod connection;
mod convert;
mod definition;
mod error;
mod runtime;
mod schema;

pub use connection::PostgresCdcScanConfig;
pub use definition::{PostgresCdcScanDefinition, PostgresCdcScanSpec};
pub use error::PostgresCdcScanError;
use runtime::PostgresCdcScanOperation;

#[cfg(test)]
mod tests;
