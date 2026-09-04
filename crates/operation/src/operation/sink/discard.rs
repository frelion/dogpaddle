use std::num::NonZeroU32;

use arrow_schema::SchemaRef;
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::Sealed as SealedDefinition,
    operation::{Action, Operation, OperationError, OperationInput},
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
pub struct DiscardOperation;

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

impl SealedDefinition for DiscardDefinition {
    fn bind_schemas(
        &self,
        _input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        Ok(OperationBinding::without_data(None, DiscardOperation))
    }
}

impl OperationDefinition for DiscardDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Sink(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl Operation for DiscardOperation {
    fn turn(
        &mut self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(DiscardError::MissingInput)?;
        if input.port != 0 {
            return Err(DiscardError::InvalidInputPort { port: input.port }.into());
        }

        Ok(Action::Complete(None))
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
