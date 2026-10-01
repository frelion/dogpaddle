use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use super::{
    buffered,
    config::{PostgresSinkConfig, PostgresTargetSpec},
    error::{PostgresSinkError, invalid_spec},
    schema::PostgresLayout,
    target::PostgresTarget,
};
use crate::{ConstructedOperation, RuntimeResource, definition::schema_error};

const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

/// Pure definition of a sink that materializes its input relation in `PostgreSQL`.
///
/// The definition persists only the non-sensitive target identity discovered
/// before Flow construction. Credentials and endpoint configuration are
/// supplied separately through [`super::PostgresSinkConfig`] whenever the Flow is
/// built or reopened. Construction and Schema binding perform no network I/O.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct PostgresSinkDefinition {
    target: PostgresTargetSpec,
}

impl PostgresSinkDefinition {
    /// Freezes one discovered, sink-owned target as a persistent definition.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresSinkError`] when the target identity is invalid or its
    /// canonical representation exceeds the persistent definition limit.
    pub fn try_new(target: PostgresTargetSpec) -> Result<Self, PostgresSinkError> {
        let definition = Self { target };
        definition.validate()?;
        Ok(definition)
    }

    fn validate(&self) -> Result<(), PostgresSinkError> {
        self.target.validate()?;
        if encoded_target(&self.target).len() > MAX_DEFINITION_BYTES {
            return Err(invalid_spec(
                "target specification exceeds the 1 MiB definition limit",
            ));
        }
        Ok(())
    }

    /// Returns the frozen, non-sensitive target identity.
    #[must_use]
    pub const fn target(&self) -> &PostgresTargetSpec {
        &self.target
    }
}

impl PostgresSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        self.validate()?;
        PostgresLayout::try_new(Arc::clone(&inputs[0]))?;
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
            .expect("the final binding entrypoint enforces PostgreSQL sink input arity");
        let layout = PostgresLayout::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<PostgresSinkConfig>()?;
        let target = PostgresTarget::new_bound(config, self.target.clone(), layout);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<PostgresSinkConfig>()
    }
}

fn encoded_target(target: &PostgresTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("PostgreSQL sink target specification is JSON-serializable")
}
