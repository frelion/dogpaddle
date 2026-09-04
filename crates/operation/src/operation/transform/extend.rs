use std::{num::NonZeroU32, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DefinitionCodecError, Expr, ExpressionBindError, ExpressionDefinitionError,
    ExpressionError, OperationBinding, OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::Sealed as SealedDefinition,
    expression::{BoundExpression, StoredExpression},
    operation::{Action, OperationError, OperationInput, TransactionalOperation},
};

pub(crate) const TAG: u16 = 6;
const DATA: &[DataDeclaration] = &[];

/// Pure definition of a transform that appends one computed top-level field.
///
/// The field type and nullability are derived uniquely by binding `expression`
/// to the exact input Schema. Input fields and Schema metadata are preserved;
/// the appended field starts with empty Field metadata. Multiple fields are
/// expressed by chaining multiple Extend operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtendDefinition {
    field_name: String,
    expression: StoredExpression,
}

/// Materialized exact-Schema-bound single-field extension.
///
/// This value owns only its compiled expression and exact output Schema. It
/// owns no persistent Store data and retains no Definition.
pub struct ExtendOperation {
    expression: BoundExpression,
    output_schema: SchemaRef,
}

/// Extend-specific failure while binding exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtendSchemaError {
    /// The persistent expression cannot bind to the input Schema.
    #[error(transparent)]
    Expression(#[from] ExpressionBindError),
}

/// Failure while constructing an [`ExtendDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtendDefinitionError {
    /// The appended field name cannot fit the stable definition format.
    #[error("Extend field name is too long for the stable format")]
    FieldNameTooLong,
    /// The `DataFusion` expression cannot be persisted exactly and canonically.
    #[error(transparent)]
    Expression(#[from] ExpressionDefinitionError),
}

/// Extend-specific failure during one [`ExtendOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtendError {
    /// The input Operation was called without a Change.
    #[error("extend requires one input Change")]
    MissingInput,
    /// Extend only accepts its Definition's first input port.
    #[error("extend does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// Expression evaluation failed.
    #[error(transparent)]
    Expression(#[from] ExpressionError),
    /// Arrow could not construct the extended record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// The extended output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

impl ExtendDefinition {
    /// Admits a `DataFusion` expression that appends one computed field.
    ///
    /// Field-name uniqueness and the reserved protocol namespace depend on the
    /// eventual input Schema and are checked by the final Schema binding.
    ///
    /// # Errors
    ///
    /// Returns [`ExtendDefinitionError`] when the field name cannot fit the
    /// stable format or `DataFusion` cannot round-trip the expression.
    pub fn try_new(
        field_name: impl Into<String>,
        expression: Expr,
    ) -> Result<Self, ExtendDefinitionError> {
        let field_name = field_name.into();
        if u32::try_from(field_name.len()).is_err() {
            return Err(ExtendDefinitionError::FieldNameTooLong);
        }
        Ok(Self {
            field_name,
            expression: StoredExpression::try_new(expression)?,
        })
    }

    /// Returns the name of the appended top-level field.
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Returns the canonical `DataFusion` expression admitted by this definition.
    #[must_use]
    pub fn expression(&self) -> &Expr {
        self.expression.expression()
    }
}

impl SealedDefinition for ExtendDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Extend input arity");
        let expression = self.expression.bind(Arc::clone(input_schema)).map_err(
            |source| -> OperationSchemaError { Box::new(ExtendSchemaError::Expression(source)) },
        )?;

        let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            &self.field_name,
            expression.output_type().clone(),
            expression.output_nullable(),
        )));
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let materialized_schema = Arc::clone(&output_schema);
        Ok(OperationBinding::without_data(
            Some(output_schema),
            ExtendOperation {
                expression,
                output_schema: materialized_schema,
            },
        ))
    }
}

impl OperationDefinition for ExtendDefinition {
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
        let name_length = u32::try_from(self.field_name.len())
            .expect("ExtendDefinition::try_new validated the stable field-name length");
        output.extend_from_slice(&name_length.to_be_bytes());
        output.extend_from_slice(self.field_name.as_bytes());
        self.expression.encode(output);
    }
}

impl TransactionalOperation for ExtendOperation {
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(ExtendError::MissingInput)?;
        if input.port != 0 {
            return Err(ExtendError::InvalidInputPort { port: input.port }.into());
        }

        let computed = self
            .expression
            .evaluate(input.change.records())
            .map_err(ExtendError::Expression)?;
        let mut columns = input.change.records().columns().to_vec();
        columns.push(computed);
        let records = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(ExtendError::Arrow)?;
        let output =
            Change::try_new(records, input.change.diffs().clone()).map_err(ExtendError::Change)?;
        Ok(Action::Complete(Some(output)))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let name_length = usize::try_from(cursor.read_u32()?)
        .map_err(|_| DefinitionCodecError::InvalidPayload("Extend field-name length is invalid"))?;
    let name = cursor.read_bytes(name_length)?;
    let field_name = std::str::from_utf8(name)
        .map_err(|_| DefinitionCodecError::InvalidPayload("Extend field name is invalid UTF-8"))?
        .to_owned();
    let expression = StoredExpression::decode(&mut cursor)?;
    cursor.finish()?;
    Ok(Box::new(ExtendDefinition {
        field_name,
        expression,
    }))
}
