use crate::{CountOperation, OperationDefinition, SequenceSourceOperation};

mod private {
    pub trait Sealed {}
}

/// Runtime parent trait implemented by every materialized operation.
///
/// The trait is sealed so the closed [`OperationDefinition`] union and the set
/// of runtime operation implementations remain exhaustive across engine crates.
pub trait Operation: private::Sealed + Send + Sync + 'static {
    /// Returns the pure definition that materialized this operation.
    fn definition(&self) -> OperationDefinition;
}

impl private::Sealed for CountOperation {}

impl Operation for CountOperation {
    fn definition(&self) -> OperationDefinition {
        OperationDefinition::from(*CountOperation::definition(self))
    }
}

impl private::Sealed for SequenceSourceOperation {}

impl Operation for SequenceSourceOperation {
    fn definition(&self) -> OperationDefinition {
        OperationDefinition::from(*SequenceSourceOperation::definition(self))
    }
}
