use dogpaddle_store::TransactionAccess;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    DefinitionCodecError,
    codec::decode_json_payload,
    definition::ConstructedOperation,
    operation::{Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation},
};

pub(crate) const TAG: u16 = 3;

/// Pure definition of a sink that intentionally discards every input Change.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DiscardDefinition {}

/// Materialized sink that intentionally discards every input Change.
///
/// Input completion remains durable because the owning Station acknowledges
/// its Subscription in the same transaction as this Operation turn.
pub(crate) struct DiscardOperation;

/// Discard-specific failure during one `DiscardOperation` turn.
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
        Self {}
    }
}

impl DiscardDefinition {
    pub(crate) fn construct_unchecked() -> ConstructedOperation {
        ConstructedOperation::turn(None, DiscardOperation)
    }
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
) -> Result<Box<DiscardDefinition>, DefinitionCodecError> {
    let definition: DiscardDefinition = decode_json_payload(payload, "invalid Discard payload")?;
    Ok(Box::new(definition))
}
