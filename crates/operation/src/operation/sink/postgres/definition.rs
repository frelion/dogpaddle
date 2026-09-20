use std::{any::TypeId, num::NonZeroU32, sync::Arc};

use arrow_schema::SchemaRef;

use super::{
    buffered,
    config::{PostgresSinkConfig, PostgresTargetSpec},
    error::{PostgresSinkError, invalid_spec},
    relation::RelationSinkTarget,
    schema::PostgresLayout,
    target::PostgresTarget,
};
use crate::{
    ConstructedOperation, DefinitionCodecError, OperationDefinition, OperationKind,
    RuntimeResource,
    definition::{Sealed, schema_error},
};

pub(crate) const TAG: u16 = 12;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

/// Pure definition of a sink that materializes its input relation in `PostgreSQL`.
///
/// The definition persists only the non-sensitive target identity discovered
/// before Flow construction. Credentials and endpoint configuration are
/// supplied separately through [`super::PostgresSinkConfig`] whenever the Flow is
/// built or reopened. Construction and Schema binding perform no network I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
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
        target.validate()?;
        if encoded_target(&target).len() > MAX_DEFINITION_BYTES {
            return Err(invalid_spec(
                "target specification exceeds the 1 MiB definition limit",
            ));
        }
        Ok(Self { target })
    }

    /// Returns the frozen, non-sensitive target identity.
    #[must_use]
    pub const fn target(&self) -> &PostgresTargetSpec {
        &self.target
    }
}

impl Sealed for PostgresSinkDefinition {
    fn output_schema_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        PostgresLayout::try_new(Arc::clone(&inputs[0]))?;
        Ok(None)
    }

    fn construct_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces PostgreSQL sink input arity");
        let layout = PostgresLayout::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<PostgresSinkConfig>()?;
        let target = RelationSinkTarget::new(PostgresTarget::new_bound(
            config,
            self.target.clone(),
            layout,
        ));
        buffered::construct(input_schema, target, data)
    }

    fn resource_type(&self) -> Option<TypeId> {
        Some(TypeId::of::<PostgresSinkConfig>())
    }
}

impl OperationDefinition for PostgresSinkDefinition {
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
        || DefinitionCodecError::InvalidPayload("invalid PostgreSQL sink target specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }

    let target = serde_json::from_slice(payload).map_err(|_| invalid())?;
    let definition = PostgresSinkDefinition::try_new(target).map_err(|_| invalid())?;
    if encoded_target(definition.target()) != payload {
        return Err(invalid());
    }
    Ok(Box::new(definition))
}

fn encoded_target(target: &PostgresTargetSpec) -> Vec<u8> {
    serde_json::to_vec(target).expect("PostgreSQL sink target specification is JSON-serializable")
}
