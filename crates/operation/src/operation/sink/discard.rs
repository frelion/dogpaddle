use dogpaddle_store::{ReadTransactionAccess, TransactionAccess};
use serde::{Deserialize, Serialize};

use crate::{
    DefinitionCodecError,
    codec::decode_json_payload,
    definition::ConstructedOperation,
    operation::{OperationError, SinkOperation, SinkPending, SinkPrepared},
};

pub(crate) const TAG: u16 = 3;

/// Pure definition of a sink that intentionally discards every input Change.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DiscardDefinition {}

/// Materialized sink that intentionally discards every input Change.
///
/// Input completion remains durable because Flow advances the parent frame
/// in the same transaction as this sink accepts the page.
pub(crate) struct DiscardOperation;

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
        ConstructedOperation::sink(DiscardOperation)
    }
}

impl SinkOperation for DiscardOperation {
    fn try_enqueue(
        &mut self,
        _access: TransactionAccess<'_>,
        _page: &dogpaddle_change::Change,
    ) -> Result<bool, OperationError> {
        Ok(true)
    }
    fn load(
        &mut self,
        _access: ReadTransactionAccess<'_>,
    ) -> Result<Option<SinkPending>, OperationError> {
        Ok(None)
    }
    fn prepare(&mut self, _pending: SinkPending) -> Result<SinkPrepared, OperationError> {
        unreachable!("discard has no pending delivery")
    }
    fn persist_prepared(
        &self,
        _access: TransactionAccess<'_>,
        _prepared: &SinkPrepared,
    ) -> Result<(), OperationError> {
        unreachable!("discard has no prepared delivery")
    }
    fn deliver(&mut self, _prepared: &SinkPrepared) -> Result<(), OperationError> {
        unreachable!("discard has no prepared delivery")
    }
    fn settle(
        &mut self,
        _access: TransactionAccess<'_>,
        _prepared: &SinkPrepared,
    ) -> Result<(), OperationError> {
        unreachable!("discard has no prepared delivery")
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<DiscardDefinition>, DefinitionCodecError> {
    let definition: DiscardDefinition = decode_json_payload(payload, "invalid Discard payload")?;
    Ok(Box::new(definition))
}
