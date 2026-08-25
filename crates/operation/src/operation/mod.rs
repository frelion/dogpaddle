use crate::OperationDefinition;

mod private {
    pub trait Sealed {}
}

pub(crate) use private::Sealed;

pub mod sink;
pub mod source;
pub mod transform;

/// Runtime parent trait implemented by every materialized operation.
///
/// The trait is sealed so runtime implementations remain paired with the
/// closed set of built-in [`OperationDefinition`] implementations.
pub trait Operation: private::Sealed + Send + Sync + 'static {
    /// Returns the pure definition that materialized this operation.
    fn definition(&self) -> &dyn OperationDefinition;
}
