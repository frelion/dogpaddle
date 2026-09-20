use std::num::NonZeroU32;

use arrow_schema::SchemaRef;
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DefinitionCodecError, OperationDefinition, OperationKind, RuntimeResource,
    definition::{ConstructedOperation, Sealed as SealedDefinition},
    operation::{Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation},
};

pub(crate) const TAG: u16 = 3;

/// Pure definition of a sink that intentionally discards every input Change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscardDefinition {
    _private: (),
}

/// Materialized sink that intentionally discards every input Change.
///
/// Input completion remains durable because the owning Station acknowledges
/// its Subscription in the same transaction as this Operation turn.
pub struct DiscardOperation;

/// Discard-specific failure during one [`DiscardOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DiscardError {
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
    fn output_schema_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        _input_schemas: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        Ok(None)
    }

    fn construct_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        _input_schemas: &[SchemaRef],
        _data: &mut dogpaddle_store::DataScope<'_>,
        _prefix: &str,
        _resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        Ok(ConstructedOperation::turn(None, DiscardOperation))
    }
}

impl OperationDefinition for DiscardDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Sink(NonZeroU32::MIN)
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl TurnOperation for DiscardOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let Some(input) = input else {
            return Ok(Turn::Idle);
        };
        if input.port != 0 {
            return Err(DiscardError::InvalidInputPort { port: input.port }.into());
        }
        Ok(Turn::ready(|_access: TransactionAccess<'_>| {
            Ok((Action::Complete(None), AfterCommit::none()))
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
