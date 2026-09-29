use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::Serialize;

use super::{
    buffered,
    config::{ClickHouseSinkConfig, ClickHouseTargetSpec},
    error::{ClickHouseSinkError, invalid_spec},
    schema::ClickHouseLayout,
    target::ClickHouseTarget,
};
use crate::{
    ConstructedOperation, DefinitionCodecError, RuntimeResource,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::schema_error,
};

pub(crate) const TAG: u16 = 19;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

/// Pure definition of a sink-owned `ClickHouse` relation target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
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

impl ClickHouseSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        ClickHouseLayout::try_new(Arc::clone(&inputs[0]))?;
        Ok(())
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces ClickHouse sink input arity");
        let layout = ClickHouseLayout::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<ClickHouseSinkConfig>()?;
        let target = ClickHouseTarget::new_bound(config, self.target.clone(), layout);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<ClickHouseSinkConfig>()
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<ClickHouseSinkDefinition>, DefinitionCodecError> {
    let invalid =
        || DefinitionCodecError::InvalidPayload("invalid ClickHouse sink target specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let target = parse_json_payload(payload)?;
    let definition = ClickHouseSinkDefinition::try_new(target).map_err(|_| invalid())?;
    require_canonical_json_payload(
        &definition,
        payload,
        "invalid ClickHouse sink target specification",
    )?;
    Ok(Box::new(definition))
}

fn encoded_target(target: &ClickHouseTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("ClickHouse target specification is JSON-serializable")
}
