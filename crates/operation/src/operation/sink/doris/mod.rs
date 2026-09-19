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

pub(crate) use definition::{BoundDorisSink, TAG, decode_definition};

use super::{buffered, relation::RelationSinkTarget};
use crate::{RuntimeResource, operation::Operation, setup::OperationSetupError};
use dogpaddle_store::{Store, StoreSetup};
use target::DorisTarget;

fn target(bound: &BoundDorisSink, config: DorisSinkConfig) -> RelationSinkTarget<DorisTarget> {
    RelationSinkTarget::new(DorisTarget::new_bound(
        config,
        bound.target.clone(),
        bound.input_schema.clone(),
    ))
}
pub(crate) fn create(
    bound: BoundDorisSink,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let config = resource.take()?;
    let target = target(&bound, config);
    buffered::create(bound.input_schema, target, setup, prefix)
}
pub(crate) fn open(
    bound: BoundDorisSink,
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
