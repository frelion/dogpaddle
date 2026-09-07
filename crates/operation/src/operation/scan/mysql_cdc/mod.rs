//! Single-table, fixed-Schema `MySQL` binlog scan backed by Debezium.

mod connection;
mod convert;
mod definition;
mod error;
mod runtime;
mod schema;

pub use connection::MySqlCdcScanConfig;
pub use definition::{MySqlCdcScanDefinition, MySqlCdcScanSpec};
pub use error::MySqlCdcScanError;
pub use runtime::MySqlCdcScanOperation;
pub use schema::{MySqlColumn, MySqlType};

pub(crate) use definition::{TAG, decode_definition};

#[cfg(test)]
mod tests;
