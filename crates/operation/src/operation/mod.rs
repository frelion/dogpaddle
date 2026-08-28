use std::error::Error;

use dogpaddle_change::Change;
use dogpaddle_store::TransactionAccess;

use crate::OperationDefinition;

mod private {
    pub trait Sealed {}
}

pub(crate) use private::Sealed;

pub mod sink;
pub mod source;
pub mod transform;

/// One complete input Change borrowed for an Operation turn.
#[derive(Clone, Copy, Debug)]
pub struct OperationInput<'change> {
    /// Zero-based ordinal in the Definition's ordered inputs.
    pub port: usize,
    /// Complete Change offered on `port`.
    pub change: &'change Change,
}

/// Type-erased failure from one concrete Operation turn.
pub type OperationError = Box<dyn Error + Send + Sync + 'static>;

/// Runtime parent trait implemented by every materialized operation.
///
/// The trait is sealed so runtime implementations remain paired with the
/// closed set of built-in [`OperationDefinition`] implementations.
pub trait Operation: private::Sealed + Send + Sync + 'static {
    /// Returns the pure definition that materialized this operation.
    fn definition(&self) -> &dyn OperationDefinition;

    /// Executes one turn in an existing transaction.
    ///
    /// A source receives `None`. An input Operation receives exactly one
    /// complete Change. Success means the offered work is complete; the caller
    /// retains transaction ownership and commits the Operation's state and
    /// optional output atomically.
    ///
    /// # Errors
    ///
    /// Returns an erased concrete Operation failure. The caller must roll back
    /// the transaction on any error.
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError>;
}
