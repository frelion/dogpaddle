use dogpaddle_store::{Store, StoreData, StoreError, StoreSetup};
use thiserror::Error;

use crate::{
    RuntimeResource,
    definition::{BoundBody, OperationBinding},
    operation::Operation,
};

/// Failure while creating or opening one bound operation's typed persistent state.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OperationSetupError {
    /// The operation requires an ephemeral runtime resource.
    #[error("operation runtime resource was not provided")]
    MissingRuntimeResource,
    /// The supplied resource has the wrong concrete Rust type.
    #[error("operation runtime resource has the wrong type")]
    WrongRuntimeResource,
    /// A self-contained operation was supplied an unused resource.
    #[error("operation does not accept a runtime resource")]
    UnexpectedRuntimeResource,
    /// A persistent resource could not be created or opened.
    #[error("operation data {name:?} could not be created or opened: {source}")]
    Store {
        name: String,
        #[source]
        source: StoreError,
    },
    /// Constructed execution capability contradicted the validated operation kind.
    #[error("operation setup execution capability does not match its declared kind")]
    ExecutionKind,
}

pub(crate) fn create_data<D: StoreData>(
    setup: &mut StoreSetup,
    prefix: &str,
    logical: &str,
) -> Result<D, OperationSetupError> {
    let name = format!("{prefix}/{logical}");
    setup
        .create_data::<D>(&name)
        .map_err(|source| OperationSetupError::Store { name, source })
}

pub(crate) fn open_data<D: StoreData>(
    store: &Store,
    prefix: &str,
    logical: &str,
) -> Result<D, OperationSetupError> {
    let name = format!("{prefix}/{logical}");
    store
        .open_data::<D>(&name)
        .map_err(|source| OperationSetupError::Store { name, source })
}

/// Creates typed state and directly constructs one validated operation.
///
/// # Errors
/// Returns [`OperationSetupError`] if state creation, resource consumption, or construction fails.
#[doc(hidden)]
pub fn create(
    binding: OperationBinding,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    binding.validate_resource(&resource)?;
    let kind = binding.kind;
    let operation = match binding.body {
        BoundBody::AtomicReady(operation) => Operation::Atomic(operation),
        BoundBody::TurnReady(operation) => Operation::Turn(operation),
        BoundBody::Sequence(bound) => {
            crate::operation::scan::sequence::create(bound, setup, prefix)?
        }
        BoundBody::PostgresCdc(bound) => {
            crate::operation::scan::postgres_cdc::create(*bound, setup, prefix, resource)?
        }
        BoundBody::MySqlCdc(bound) => {
            crate::operation::scan::mysql_cdc::create(*bound, setup, prefix, resource)?
        }
        BoundBody::RunningEventCount(bound) => {
            crate::operation::transform::running_event_count::create(bound, setup, prefix)?
        }
        BoundBody::Distinct(bound) => {
            crate::operation::transform::distinct::create(bound, setup, prefix)?
        }
        BoundBody::Aggregate(bound) => {
            crate::operation::transform::aggregate::create(*bound, setup, prefix)?
        }
        BoundBody::EquiJoin(bound) => {
            crate::operation::transform::equi_join::create(*bound, setup, prefix)?
        }
        BoundBody::AsOfJoin(bound) => {
            crate::operation::transform::asof_join::create(*bound, setup, prefix)?
        }
        BoundBody::SqliteSink(bound) => {
            crate::operation::sink::sqlite::create(*bound, setup, prefix)?
        }
        BoundBody::PostgresSink(bound) => {
            crate::operation::sink::postgres::create(*bound, setup, prefix, resource)?
        }
        BoundBody::DorisSink(bound) => {
            crate::operation::sink::doris::create(*bound, setup, prefix, resource)?
        }
        BoundBody::ClickHouseSink(bound) => {
            crate::operation::sink::clickhouse::create(*bound, setup, prefix, resource)?
        }
    };
    OperationBinding::normalize(kind, operation)
}

/// Opens typed state and directly constructs one validated operation.
///
/// # Errors
/// Returns [`OperationSetupError`] if state opening, resource consumption, or construction fails.
#[doc(hidden)]
pub fn open(
    binding: OperationBinding,
    store: &Store,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError> {
    binding.validate_resource(&resource)?;
    let kind = binding.kind;
    let operation = match binding.body {
        BoundBody::AtomicReady(operation) => Operation::Atomic(operation),
        BoundBody::TurnReady(operation) => Operation::Turn(operation),
        BoundBody::Sequence(bound) => crate::operation::scan::sequence::open(bound, store, prefix)?,
        BoundBody::PostgresCdc(bound) => {
            crate::operation::scan::postgres_cdc::open(*bound, store, prefix, resource)?
        }
        BoundBody::MySqlCdc(bound) => {
            crate::operation::scan::mysql_cdc::open(*bound, store, prefix, resource)?
        }
        BoundBody::RunningEventCount(bound) => {
            crate::operation::transform::running_event_count::open(bound, store, prefix)?
        }
        BoundBody::Distinct(bound) => {
            crate::operation::transform::distinct::open(bound, store, prefix)?
        }
        BoundBody::Aggregate(bound) => {
            crate::operation::transform::aggregate::open(*bound, store, prefix)?
        }
        BoundBody::EquiJoin(bound) => {
            crate::operation::transform::equi_join::open(*bound, store, prefix)?
        }
        BoundBody::AsOfJoin(bound) => {
            crate::operation::transform::asof_join::open(*bound, store, prefix)?
        }
        BoundBody::SqliteSink(bound) => {
            crate::operation::sink::sqlite::open(*bound, store, prefix)?
        }
        BoundBody::PostgresSink(bound) => {
            crate::operation::sink::postgres::open(*bound, store, prefix, resource)?
        }
        BoundBody::DorisSink(bound) => {
            crate::operation::sink::doris::open(*bound, store, prefix, resource)?
        }
        BoundBody::ClickHouseSink(bound) => {
            crate::operation::sink::clickhouse::open(*bound, store, prefix, resource)?
        }
    };
    OperationBinding::normalize(kind, operation)
}
