use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use super::{
    buffered,
    config::{ClickHouseSinkConfig, ClickHouseTargetSpec},
    error::{ClickHouseSinkError, invalid_spec},
    row::ClickHouseRowCodec,
    schema,
    target::ClickHouseTarget,
};
use crate::{ConstructedOperation, RuntimeResource, definition::schema_error};

const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

/// Pure definition of a sink-owned `ClickHouse` relation target.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
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
        let definition = Self { target };
        definition.validate()?;
        Ok(definition)
    }

    fn validate(&self) -> Result<(), ClickHouseSinkError> {
        self.target.validate()?;
        if encoded_target(&self.target).len() > MAX_DEFINITION_BYTES {
            return Err(invalid_spec(
                "target specification exceeds the 1 MiB definition limit",
            ));
        }
        Ok(())
    }

    /// Returns the persistent target identity.
    #[must_use]
    pub const fn target(&self) -> &ClickHouseTargetSpec {
        &self.target
    }
}

impl ClickHouseSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        self.validate()?;
        schema::validate(&inputs[0])?;
        Ok(())
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        self.validate().map_err(schema_error)?;
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces ClickHouse sink input arity");
        let codec = ClickHouseRowCodec::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<ClickHouseSinkConfig>()?;
        let target = ClickHouseTarget::new_bound(config, self.target.clone(), codec);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<ClickHouseSinkConfig>()
    }
}

fn encoded_target(target: &ClickHouseTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("ClickHouse target specification is JSON-serializable")
}
