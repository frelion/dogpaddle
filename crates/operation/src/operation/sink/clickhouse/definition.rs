use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::SchemaRef;

use super::{
    config::ClickHouseTargetSpec,
    error::{ClickHouseSinkError, invalid_spec},
    schema::ClickHouseLayout,
};
use crate::{
    DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::{BoundBody, Sealed},
};

pub(crate) const TAG: u16 = 19;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

pub(crate) struct BoundClickHouseSink {
    pub(super) target: ClickHouseTargetSpec,
    pub(super) input_schema: SchemaRef,
}

/// Pure definition of a sink-owned `ClickHouse` relation target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClickHouseSinkDefinition {
    target: ClickHouseTargetSpec,
}

impl ClickHouseSinkDefinition {
    /// Freezes a discovered non-sensitive target identity.
    ///
    /// # Errors
    ///
    /// Rejects invalid or oversized specifications.
    pub fn try_new(target: ClickHouseTargetSpec) -> Result<Self, ClickHouseSinkError> {
        target.validate()?;
        if encoded_target(&target).len() > MAX_DEFINITION_BYTES {
            return Err(invalid_spec(
                "target specification exceeds the 1 MiB definition limit",
            ));
        }
        Ok(Self { target })
    }

    /// Returns the persistent target identity.
    #[must_use]
    pub const fn target(&self) -> &ClickHouseTargetSpec {
        &self.target
    }
}

impl Sealed for ClickHouseSinkDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces ClickHouse sink input arity");
        let _layout = ClickHouseLayout::try_new(Arc::clone(input_schema))
            .map_err(|source| -> OperationSchemaError { Box::new(source) })?;
        let target = self.target.clone();
        let input_schema = Arc::clone(input_schema);
        Ok(OperationBinding::bound(
            None,
            BoundBody::ClickHouseSink(Box::new(BoundClickHouseSink {
                target,
                input_schema,
            })),
        ))
    }
}

impl OperationDefinition for ClickHouseSinkDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Sink(NonZeroU32::MIN)
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.extend(encoded_target(&self.target));
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let invalid =
        || DefinitionCodecError::InvalidPayload("invalid ClickHouse sink target specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let target = serde_json::from_slice(payload).map_err(|_| invalid())?;
    let definition = ClickHouseSinkDefinition::try_new(target).map_err(|_| invalid())?;
    if encoded_target(definition.target()) != payload {
        return Err(invalid());
    }
    Ok(Box::new(definition))
}

fn encoded_target(target: &ClickHouseTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("ClickHouse target specification is JSON-serializable")
}
