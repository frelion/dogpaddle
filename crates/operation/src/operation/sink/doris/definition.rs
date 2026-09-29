use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::Serialize;

use super::{
    buffered,
    config::{DorisSinkConfig, DorisTargetSpec},
    error::{DorisSinkError, invalid_spec},
    schema::DorisLayout,
    target::DorisTarget,
};
use crate::{
    ConstructedOperation, DefinitionCodecError, RuntimeResource,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::schema_error,
};

pub(crate) const TAG: u16 = 18;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

/// Pure definition of a sink-owned Apache Doris relation target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct DorisSinkDefinition {
    target: DorisTargetSpec,
}

impl DorisSinkDefinition {
    /// Freezes a discovered non-sensitive target identity.
    ///
    /// # Errors
    ///
    /// Rejects invalid or oversized specifications.
    pub fn try_new(target: DorisTargetSpec) -> Result<Self, DorisSinkError> {
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
    pub const fn target(&self) -> &DorisTargetSpec {
        &self.target
    }
}

impl DorisSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        DorisLayout::try_new(Arc::clone(&inputs[0]))?;
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
            .expect("the final binding entrypoint enforces Doris sink input arity");
        let layout = DorisLayout::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<DorisSinkConfig>()?;
        let target = DorisTarget::new_bound(config, self.target.clone(), layout);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<DorisSinkConfig>()
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<DorisSinkDefinition>, DefinitionCodecError> {
    let invalid =
        || DefinitionCodecError::InvalidPayload("invalid Doris sink target specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let target = parse_json_payload(payload)?;
    let definition = DorisSinkDefinition::try_new(target).map_err(|_| invalid())?;
    require_canonical_json_payload(
        &definition,
        payload,
        "invalid Doris sink target specification",
    )?;
    Ok(Box::new(definition))
}

fn encoded_target(target: &DorisTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("Doris sink target specification is JSON-serializable")
}
