use dogpaddle_store::{Cell, CellAccess, Small, StoreError};
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationDefinition,
    definition::{DataName, Sealed as SealedDefinition},
    operation::{Operation, Sealed as SealedOperation},
};

pub(crate) const TAG: u16 = 2;
const COUNT: DataName<Cell<u64, Small>> = DataName::new("count");
const DATA: &[DataDeclaration] = &[COUNT.declaration()];

/// Pure definition of a running count operation.
///
/// The operation accepts one input record at a time, increments its durable
/// count, and returns the updated count as its output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CountDefinition {
    _private: (),
}

/// Materialized running count operation.
///
/// This value owns its pure definition and persistent count, but never
/// begins, commits, or stores a transaction.
pub struct CountOperation {
    definition: CountDefinition,
    count: Cell<u64, Small>,
}

/// Failure while applying one input to a [`CountOperation`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CountError {
    /// Persistent count access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The durable count has reached [`u64::MAX`].
    #[error("count overflow")]
    Overflow,
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl CountDefinition {
    /// Creates a running count definition.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl SealedDefinition for CountDefinition {}

impl OperationDefinition for CountDefinition {
    fn input_count(&self) -> usize {
        1
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn materialize(
        &self,
        data: &mut DataInstances,
    ) -> Result<Box<dyn Operation>, MaterializeError> {
        let count = data.take(&COUNT)?;
        Ok(Box::new(CountOperation::new(*self, count)))
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl CountOperation {
    /// Materializes a Count operation from its pure definition and durable data.
    #[must_use]
    pub const fn new(definition: CountDefinition, count: Cell<u64, Small>) -> Self {
        Self { definition, count }
    }

    /// Returns the pure definition used to materialize this operation.
    #[must_use]
    pub const fn definition(&self) -> &CountDefinition {
        &self.definition
    }

    /// Returns the cell holding the committed count.
    #[must_use]
    pub const fn count(&self) -> &Cell<u64, Small> {
        &self.count
    }

    /// Applies one accepted input and returns the updated running count.
    ///
    /// A missing cell value is interpreted as zero. The caller owns the
    /// transaction that produced `count` and decides whether to commit or roll
    /// back the state change and returned output together.
    ///
    /// # Errors
    ///
    /// Returns [`CountError::Overflow`] rather than wrapping at [`u64::MAX`].
    /// Storage and codec failures are returned as [`CountError::Store`].
    pub fn apply(&self, count: &mut CellAccess<'_, u64>) -> Result<u64, CountError> {
        let next = count
            .get()?
            .unwrap_or_default()
            .checked_add(1)
            .ok_or(CountError::Overflow)?;
        count.set(&next)?;
        Ok(next)
    }
}

impl SealedOperation for CountOperation {}

impl Operation for CountOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    if payload.is_empty() {
        Ok(Box::new(CountDefinition::new()))
    } else {
        Err(DefinitionCodecError::TrailingBytes)
    }
}
