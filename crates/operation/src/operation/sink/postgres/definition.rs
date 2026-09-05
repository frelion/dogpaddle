use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_store::Cell;

use super::{
    config::{PostgresSinkConfig, PostgresTargetSpec},
    error::{PostgresSinkError, invalid_spec},
    runtime::PostgresSinkOperation,
    schema::PostgresLayout,
};
use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    definition::{DataName, Sealed},
    operation::Operation,
};

pub(crate) const TAG: u16 = 12;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const STATE: DataName<Cell<Vec<u8>>> = DataName::new("postgres_sink.state");
const DATA: &[DataDeclaration] = &[STATE.declaration()];

/// Pure definition of a sink that materializes its input relation in `PostgreSQL`.
///
/// The definition persists only the non-sensitive target identity discovered
/// before Flow construction. Credentials and endpoint configuration are
/// supplied separately through [`PostgresSinkConfig`] whenever the Flow is
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
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces PostgreSQL sink input arity");
        let _layout = PostgresLayout::try_new(Arc::clone(input_schema))
            .map_err(|source| -> OperationSchemaError { Box::new(source) })?;

        let target = self.target.clone();
        let input_schema = Arc::clone(input_schema);
        Ok(OperationBinding::with_resource::<PostgresSinkConfig, _>(
            None,
            move |data: &mut DataInstances,
                  config|
                  -> Result<Box<dyn Operation>, MaterializeError> {
                let state = data.take(&STATE)?;
                Ok(Box::new(PostgresSinkOperation::new_bound(
                    target,
                    input_schema,
                    state,
                    config,
                )))
            },
        ))
    }
}

impl OperationDefinition for PostgresSinkDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Sink(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
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
