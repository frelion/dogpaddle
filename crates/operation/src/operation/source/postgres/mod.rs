//! Single-table, fixed-Schema `PostgreSQL` WAL source backed by Debezium.

mod connection;
mod convert;
mod definition;
mod error;
mod runtime;
mod schema;

pub use connection::PostgresSourceConfig;
pub use definition::{PostgresSourceDefinition, PostgresSourceSpec};
pub use error::PostgresSourceError;
pub use runtime::PostgresSourceOperation;
pub use schema::{PostgresColumn, PostgresType};

pub(crate) use definition::{TAG, decode_definition};

#[cfg(test)]
mod tests;
