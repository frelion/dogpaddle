//! Sink operations that consume records without producing output.

pub(crate) mod buffered;
mod relation;

pub(crate) mod clickhouse;
pub(crate) mod discard;
pub(crate) mod doris;
pub(crate) mod postgres;
pub(crate) mod sqlite;

pub use clickhouse::{
    ClickHouseSinkConfig, ClickHouseSinkDefinition, ClickHouseSinkError, ClickHouseSinkSchemaError,
};
pub use discard::DiscardDefinition;
pub use doris::{DorisSinkConfig, DorisSinkDefinition, DorisSinkError, DorisSinkSchemaError};
pub use postgres::{
    PostgresSinkConfig, PostgresSinkDefinition, PostgresSinkError, PostgresSinkSchemaError,
};
pub use sqlite::{
    SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkError, SqliteSinkSchemaError,
};

fn is_valid_sink_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
