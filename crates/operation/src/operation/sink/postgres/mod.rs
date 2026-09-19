//! PostgreSQL-specific target support for the relation sink.

mod config;
mod definition;
mod error;
mod row;
mod schema;
mod target;

pub use config::{PostgresSinkConfig, PostgresTargetSpec};
pub use definition::PostgresSinkDefinition;
pub use error::{PostgresSinkError, PostgresSinkSchemaError};

pub(crate) use definition::{BoundPostgresSink, TAG, decode_definition};

use super::{buffered, relation::RelationSinkTarget};
use crate::{RuntimeResource, operation::Operation, setup::OperationSetupError};
use dogpaddle_store::{Store, StoreSetup};
use target::PostgresTarget;

fn target(
    bound: &BoundPostgresSink,
    config: PostgresSinkConfig,
) -> RelationSinkTarget<PostgresTarget> {
    RelationSinkTarget::new(PostgresTarget::new_bound(
        config,
        bound.target.clone(),
        bound.input_schema.clone(),
    ))
}
pub(crate) fn create(
    bound: BoundPostgresSink,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let config = resource.take()?;
    let target = target(&bound, config);
    buffered::create(bound.input_schema, target, setup, prefix)
}
pub(crate) fn open(
    bound: BoundPostgresSink,
    store: &Store,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let config = resource.take()?;
    let target = target(&bound, config);
    buffered::open(bound.input_schema, target, store, prefix)
}

#[cfg(test)]
mod tests;
