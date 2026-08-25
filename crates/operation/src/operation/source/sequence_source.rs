use dogpaddle_store::{Cell, StoreError, TransactionAccess};
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationDefinition,
    definition::{DataName, Sealed as SealedDefinition},
    operation::{Operation, Sealed as SealedOperation},
};

pub(crate) const TAG: u16 = 1;
const POSITION: DataName<Cell<u64>> = DataName::new("sequence_source.position");
const DATA: &[DataDeclaration] = &[POSITION.declaration()];

/// Pure definition of a monotonically increasing source.
///
/// The source accepts no inputs and emits `u64` values beginning at `start`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceSourceDefinition {
    start: u64,
}

/// Materialized monotonically increasing source operation.
///
/// This value owns its pure definition and persistent position, but never
/// begins, commits, or stores a transaction.
pub struct SequenceSourceOperation {
    definition: SequenceSourceDefinition,
    position: Cell<u64>,
}

/// Failure while producing one value from a [`SequenceSourceOperation`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SequenceSourceError {
    /// Persistent position access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The source has already emitted [`u64::MAX`].
    #[error("sequence exhausted after emitting u64::MAX")]
    Exhausted,
}

impl SequenceSourceDefinition {
    /// Creates a source whose first emitted value is `start`.
    #[must_use]
    pub const fn new(start: u64) -> Self {
        Self { start }
    }

    /// Returns the first value emitted by a new source.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }
}

impl SealedDefinition for SequenceSourceDefinition {}

impl OperationDefinition for SequenceSourceDefinition {
    fn input_count(&self) -> usize {
        0
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn materialize(
        &self,
        data: &mut DataInstances,
    ) -> Result<Box<dyn Operation>, MaterializeError> {
        let position = data.take(&POSITION)?;
        Ok(Box::new(SequenceSourceOperation::new(*self, position)))
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.start.to_be_bytes());
    }
}

impl SequenceSourceOperation {
    /// Materializes a Sequence source from its pure definition and durable data.
    #[must_use]
    pub const fn new(definition: SequenceSourceDefinition, position: Cell<u64>) -> Self {
        Self {
            definition,
            position,
        }
    }

    /// Returns the pure definition used to materialize this source.
    #[must_use]
    pub const fn definition(&self) -> &SequenceSourceDefinition {
        &self.definition
    }

    /// Produces and records the next sequence value.
    ///
    /// A missing position emits the configured start value. Otherwise the last
    /// committed value is incremented. The operation binds its own position
    /// cell through `access`; the caller retains transaction ownership and
    /// decides whether to commit the new position and returned output together.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceSourceError::Exhausted`] after [`u64::MAX`] has been emitted.
    /// Storage and codec failures are returned as [`SequenceSourceError::Store`].
    pub fn step(&self, access: TransactionAccess<'_>) -> Result<u64, SequenceSourceError> {
        let mut position = self.position.access(access)?;
        let next = match position.get()? {
            Some(previous) => previous
                .checked_add(1)
                .ok_or(SequenceSourceError::Exhausted)?,
            None => self.definition.start,
        };
        position.set(&next)?;
        Ok(next)
    }
}

impl SealedOperation for SequenceSourceOperation {}

impl Operation for SequenceSourceOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let start = match <[u8; 8]>::try_from(payload) {
        Ok(bytes) => u64::from_be_bytes(bytes),
        Err(_) if payload.len() < size_of::<u64>() => {
            return Err(DefinitionCodecError::Truncated);
        }
        Err(_) => return Err(DefinitionCodecError::TrailingBytes),
    };
    Ok(Box::new(SequenceSourceDefinition::new(start)))
}
