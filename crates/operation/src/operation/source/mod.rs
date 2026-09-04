//! Source operations that produce records without consuming upstream input.

pub mod postgres;
pub(crate) mod sequence;

pub use postgres::{
    PostgresColumn, PostgresSourceConfig, PostgresSourceDefinition, PostgresSourceError,
    PostgresSourceOperation, PostgresSourceSpec, PostgresType,
};
pub use sequence::{SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation};
