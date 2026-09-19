mod definition;
mod error;
mod row;
mod target;

#[cfg(test)]
mod tests;

const TECHNICAL_ID: &str = "$dogpaddle.id";
const TECHNICAL_HASH: &str = "$dogpaddle.hash";

pub use definition::{SqliteSinkDefinition, SqliteSinkDefinitionError, SqliteSinkSchemaError};
pub use error::SqliteSinkError;

pub(crate) use definition::{BoundSqliteSink, TAG, decode_definition};

use super::{buffered, relation::RelationSinkTarget};
use crate::{operation::Operation, setup::OperationSetupError};
use dogpaddle_store::{Store, StoreSetup};

pub(crate) fn create(
    bound: BoundSqliteSink,
    setup: &mut StoreSetup,
    prefix: &str,
) -> Result<Operation, OperationSetupError> {
    buffered::create(
        bound.input_schema,
        RelationSinkTarget::new(bound.target),
        setup,
        prefix,
    )
}
pub(crate) fn open(
    bound: BoundSqliteSink,
    store: &Store,
    prefix: &str,
) -> Result<Operation, OperationSetupError> {
    buffered::open(
        bound.input_schema,
        RelationSinkTarget::new(bound.target),
        store,
        prefix,
    )
}
