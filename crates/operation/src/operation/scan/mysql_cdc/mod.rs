//! Single-table, fixed-Schema `MySQL` binlog scan backed by Debezium.

mod connection;
mod convert;
mod definition;
mod error;
mod runtime;
mod schema;

use crate::{
    RuntimeResource,
    operation::Operation,
    setup::{OperationSetupError, create_data, open_data},
};
use dogpaddle_store::{Cell, Queue, Store, StoreSetup};

pub use connection::{MySqlCdcScanConfig, MySqlCdcScanOptions};
pub use definition::{MySqlCdcScanDefinition, MySqlCdcScanSpec};
pub use error::MySqlCdcScanError;
pub use runtime::MySqlCdcScanOperation;
pub use schema::{MySqlColumn, MySqlType};

pub(crate) use definition::{BoundMySqlCdc, TAG, decode_definition};

fn assemble(
    bound: BoundMySqlCdc,
    phase: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    spool: Queue<Vec<u8>>,
    config: MySqlCdcScanConfig,
) -> Operation {
    Operation::Turn(Box::new(MySqlCdcScanOperation::new_bound(
        bound.spec,
        bound.output,
        phase,
        checkpoint,
        spool,
        bound.bootstrap_spool_bytes,
        config,
    )))
}
pub(crate) fn create(
    bound: BoundMySqlCdc,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let phase = create_data::<Cell<u32>>(setup, prefix, definition::PHASE)?;
    let checkpoint = create_data::<Cell<Vec<u8>>>(setup, prefix, definition::CHECKPOINT)?;
    let spool = create_data::<Queue<Vec<u8>>>(setup, prefix, definition::BOOTSTRAP_SPOOL)?;
    Ok(assemble(bound, phase, checkpoint, spool, resource.take()?))
}
pub(crate) fn open(
    bound: BoundMySqlCdc,
    store: &Store,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    let phase = open_data::<Cell<u32>>(store, prefix, definition::PHASE)?;
    let checkpoint = open_data::<Cell<Vec<u8>>>(store, prefix, definition::CHECKPOINT)?;
    let spool = open_data::<Queue<Vec<u8>>>(store, prefix, definition::BOOTSTRAP_SPOOL)?;
    Ok(assemble(bound, phase, checkpoint, spool, resource.take()?))
}

#[cfg(test)]
mod tests;
