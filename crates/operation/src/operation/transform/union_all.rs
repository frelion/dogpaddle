use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::Sealed as SealedDefinition,
    operation::{Action, Operation, OperationError, OperationInput},
};

pub(crate) const TAG: u16 = 8;
const DATA: &[DataDeclaration] = &[];

/// Pure definition of an order-preserving `UNION ALL` operation.
///
/// Every input must have the same exact logical Schema. Each complete input
/// Change is forwarded unchanged, preserving row order, differences, and Arrow
/// buffers. `UnionAll` defines no semantic order across ports: their physical
/// interleaving follows the owning Station's input schedule and may change when
/// inputs are rebatched. Each port's event order and the final relation remain
/// unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnionAllDefinition {
    input_count: NonZeroU32,
}

/// Materialized stateless `UNION ALL` operation.
pub struct UnionAllOperation {
    input_count: usize,
}

/// `UnionAll`-specific failure while binding exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UnionAllSchemaError {
    /// One input Schema differs from the first input Schema.
    #[error("union-all input {input} Schema differs from input 0")]
    InputSchemaMismatch {
        /// Zero-based index of the mismatched input.
        input: usize,
        /// Exact Schema required by input 0.
        expected: SchemaRef,
        /// Exact Schema supplied for this input.
        actual: SchemaRef,
    },
}

/// `UnionAll`-specific failure during one [`UnionAllOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UnionAllError {
    /// The operation was called without a Change.
    #[error("union-all requires one input Change")]
    MissingInput,
    /// The supplied port is outside the Definition's ordered inputs.
    #[error("union-all does not accept input port {port}; it has {input_count} inputs")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
        /// Number of input ports accepted by this operation.
        input_count: usize,
    },
}

impl UnionAllDefinition {
    /// Creates a definition with the exact non-zero number of ordered inputs.
    #[must_use]
    pub const fn new(input_count: NonZeroU32) -> Self {
        Self { input_count }
    }

    /// Returns the exact number of ordered inputs.
    #[must_use]
    pub const fn input_count(&self) -> NonZeroU32 {
        self.input_count
    }
}

impl SealedDefinition for UnionAllDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let output_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces UnionAll input arity");
        for (input, schema) in input_schemas.iter().enumerate().skip(1) {
            if schema != output_schema {
                return Err(Box::new(UnionAllSchemaError::InputSchemaMismatch {
                    input,
                    expected: Arc::clone(output_schema),
                    actual: Arc::clone(schema),
                }));
            }
        }

        let input_count = usize::try_from(self.input_count.get())
            .expect("a UnionAll u32 input count fits supported Arrow targets");
        Ok(OperationBinding::new(
            Some(Arc::clone(output_schema)),
            move |_data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                Ok(Box::new(UnionAllOperation { input_count }))
            },
        ))
    }
}

impl OperationDefinition for UnionAllDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Transform(self.input_count)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.input_count.get().to_be_bytes());
    }
}

impl Operation for UnionAllOperation {
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(UnionAllError::MissingInput)?;
        if input.port >= self.input_count {
            return Err(UnionAllError::InvalidInputPort {
                port: input.port,
                input_count: self.input_count,
            }
            .into());
        }

        Ok(Action::Complete(Some(input.change.clone())))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let input_count = NonZeroU32::new(cursor.read_u32()?).ok_or(
        DefinitionCodecError::InvalidPayload("union-all input count must be non-zero"),
    )?;
    cursor.finish()?;
    Ok(Box::new(UnionAllDefinition::new(input_count)))
}
