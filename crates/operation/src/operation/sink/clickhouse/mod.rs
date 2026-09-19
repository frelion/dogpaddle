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

pub(crate) use definition::{BoundClickHouseSink, TAG, decode_definition};

use super::{buffered, relation::RelationSinkTarget};
use crate::{RuntimeResource, operation::Operation, setup::OperationSetupError};
use dogpaddle_store::{Store, StoreSetup};
use target::ClickHouseTarget;

fn target(
    bound: &BoundClickHouseSink,
    config: ClickHouseSinkConfig,
) -> RelationSinkTarget<ClickHouseTarget> {
    RelationSinkTarget::new(ClickHouseTarget::new_bound(
        config,
        bound.target.clone(),
        bound.input_schema.clone(),
    ))
}
pub(crate) fn create(
    bound: BoundClickHouseSink,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let config = resource.take()?;
    let target = target(&bound, config);
    buffered::create(bound.input_schema, target, setup, prefix)
}
pub(crate) fn open(
    bound: BoundClickHouseSink,
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
