//! Sink operations that consume records without producing output.

mod buffered;
mod relation;

pub(crate) mod clickhouse;
pub(crate) mod discard;
pub(crate) mod doris;
pub(crate) mod postgres;
pub(crate) mod sqlite;

pub use clickhouse::{
    ClickHouseSinkConfig, ClickHouseSinkDefinition, ClickHouseSinkError, ClickHouseSinkSchemaError,
    ClickHouseTargetSpec,
};
pub use discard::{DiscardDefinition, DiscardError, DiscardOperation};
pub use doris::{
    DorisSinkConfig, DorisSinkDefinition, DorisSinkError, DorisSinkSchemaError, DorisTargetSpec,
};
pub use postgres::{
    PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkSchemaError,
    PostgresTargetSpec,
};
pub use sqlite::{
    SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkError, SqliteSinkSchemaError,
};
