use std::num::NonZeroU32;

use arrow_schema::SchemaRef;
use dogpaddle_change::{ChangeProjection, ProjectionError};
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::Sealed as SealedDefinition,
    operation::{Action, Operation, OperationError, OperationInput},
};

pub(crate) const TAG: u16 = 4;
const DATA: &[DataDeclaration] = &[];

/// Pure definition of an order-preserving top-level field projection.
///
/// Field indices address the exact logical input Schema supplied during
/// binding. They must be strictly increasing and may be empty. Selecting a
/// nested field retains its complete subtree; the implicit diff column is
/// always preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectDefinition {
    field_indices: Box<[u32]>,
}

/// Materialized zero-copy top-level projection.
///
/// The operation retains only the exact Schema-bound [`ChangeProjection`]
/// compiled by its Definition. It owns no persistent Store data.
pub struct ProjectOperation {
    projection: ChangeProjection,
}

/// Project-specific failure while binding exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProjectSchemaError {
    /// The requested fields do not form a valid projection of the bound input.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
}

/// Project-specific failure during one [`ProjectOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProjectError {
    /// The input Operation was called without a Change.
    #[error("project requires one input Change")]
    MissingInput,
    /// Project only accepts its Definition's first input port.
    #[error("project does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// The runtime Change does not satisfy the projection compiled at binding.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
}

impl ProjectDefinition {
    /// Creates a top-level projection from stable zero-based field indices.
    ///
    /// Ordering, uniqueness, and bounds depend on the eventual input Schema
    /// and are therefore checked by the Definition's Schema binding.
    ///
    /// # Panics
    ///
    /// Panics when more than [`u32::MAX`] indices are supplied. The stable v1
    /// Definition format stores the index count as a big-endian `u32`.
    #[must_use]
    pub fn new(field_indices: impl IntoIterator<Item = u32>) -> Self {
        let field_indices = field_indices.into_iter().collect::<Box<[_]>>();
        assert!(
            u32::try_from(field_indices.len()).is_ok(),
            "Project field index count must fit the stable v1 format"
        );
        Self { field_indices }
    }

    /// Returns the requested stable zero-based logical field indices.
    #[must_use]
    pub const fn field_indices(&self) -> &[u32] {
        &self.field_indices
    }
}

impl SealedDefinition for ProjectDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Project input arity");
        let field_indices = self.field_indices.iter().map(|&index| {
            usize::try_from(index).expect("a Project u32 field index fits supported Arrow targets")
        });
        let projection = ChangeProjection::try_new(input_schema.clone(), field_indices).map_err(
            |source| -> OperationSchemaError { Box::new(ProjectSchemaError::Projection(source)) },
        )?;
        let output_schema = projection.output_schema();
        Ok(OperationBinding::without_data(
            Some(output_schema),
            ProjectOperation::new(projection),
        ))
    }
}

impl OperationDefinition for ProjectDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Transform(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        let count = u32::try_from(self.field_indices.len())
            .expect("ProjectDefinition::new validated the stable field count");
        output.extend_from_slice(&count.to_be_bytes());
        for index in &self.field_indices {
            output.extend_from_slice(&index.to_be_bytes());
        }
    }
}

impl ProjectOperation {
    /// Creates a Project operation from an exact Schema-bound projection.
    #[must_use]
    pub const fn new(projection: ChangeProjection) -> Self {
        Self { projection }
    }
}

impl Operation for ProjectOperation {
    fn turn(
        &mut self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(ProjectError::MissingInput)?;
        if input.port != 0 {
            return Err(ProjectError::InvalidInputPort { port: input.port }.into());
        }

        let output = input
            .change
            .try_project(&self.projection)
            .map_err(ProjectError::Projection)?;
        Ok(Action::Complete(Some(output)))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let (encoded_count, remaining) = payload
        .split_first_chunk::<4>()
        .ok_or(DefinitionCodecError::Truncated)?;
    let count = usize::try_from(u32::from_be_bytes(*encoded_count))
        .map_err(|_| DefinitionCodecError::Truncated)?;
    let encoded_length = count
        .checked_mul(size_of::<u32>())
        .ok_or(DefinitionCodecError::Truncated)?;
    let (encoded_indices, trailing) = remaining
        .split_at_checked(encoded_length)
        .ok_or(DefinitionCodecError::Truncated)?;
    if !trailing.is_empty() {
        return Err(DefinitionCodecError::TrailingBytes);
    }
    let field_indices = encoded_indices.chunks_exact(size_of::<u32>()).map(|bytes| {
        u32::from_be_bytes(
            bytes
                .try_into()
                .expect("Project index chunks have a fixed encoded width"),
        )
    });
    Ok(Box::new(ProjectDefinition::new(field_indices)))
}
