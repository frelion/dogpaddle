use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationCategory,
    OperationDefinition,
    definition::Sealed as SealedDefinition,
    operation::{
        InputProgress, Operation, OperationError, OperationInput, TurnCommit, TurnDecision,
    },
};

pub(crate) const TAG: u16 = 3;
const DATA: &[DataDeclaration] = &[];

/// Pure definition of a sink that intentionally discards every input Change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscardDefinition {
    _private: (),
}

/// Materialized sink that intentionally discards every input Change.
///
/// Input completion remains durable because the owning Station commits its
/// cursor in the same transaction as this Operation turn.
pub struct DiscardOperation {
    definition: DiscardDefinition,
}

/// Discard-specific failure during one [`DiscardOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DiscardError {
    /// The sink was called without an input Change.
    #[error("discard requires one input Change")]
    MissingInput,
    /// Discard only accepts its definition's first input port.
    #[error("discard does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl DiscardDefinition {
    /// Creates a discard sink definition.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl SealedDefinition for DiscardDefinition {}

impl OperationDefinition for DiscardDefinition {
    fn category(&self) -> OperationCategory {
        OperationCategory::Sink
    }

    fn input_count(&self) -> usize {
        1
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn materialize(
        &self,
        _data: &mut DataInstances,
    ) -> Result<Box<dyn Operation>, MaterializeError> {
        Ok(Box::new(DiscardOperation::new(*self)))
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl DiscardOperation {
    /// Materializes a Discard sink from its pure definition.
    #[must_use]
    pub const fn new(definition: DiscardDefinition) -> Self {
        Self { definition }
    }

    /// Returns the pure definition used to materialize this sink.
    #[must_use]
    pub const fn definition(&self) -> &DiscardDefinition {
        &self.definition
    }
}

impl Operation for DiscardOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError> {
        let input = input.ok_or(DiscardError::MissingInput)?;
        if input.port != 0 {
            return Err(DiscardError::InvalidInputPort { port: input.port }.into());
        }

        Ok(TurnDecision::Commit(TurnCommit {
            input: Some(InputProgress::Complete),
            output: None,
        }))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    if payload.is_empty() {
        Ok(Box::new(DiscardDefinition::new()))
    } else {
        Err(DefinitionCodecError::TrailingBytes)
    }
}
