use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::DFSchema;
use thiserror::Error;

use crate::{
    ConstructedOperation, DefinitionCodecError, Expr, ExpressionBindError,
    ExpressionDefinitionError,
    codec::decode_json_payload,
    definition::schema_error,
    expression::{BoundProjection, StoredExpression},
};

pub(crate) const TAG: u16 = 7;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SelectField {
    name: String,
    expression: StoredExpression,
}

/// Pure definition of an ordered, expression-based projection.
///
/// Every expression is bound independently to the same exact input Schema.
/// The output contains exactly the declared fields in declaration order and
/// may contain no fields. Output types and nullability come from `DataFusion`;
/// input Schema metadata is preserved. Direct column references retain Field
/// metadata; computed fields start with empty metadata.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectDefinition {
    fields: Box<[SelectField]>,
}

/// Failure while constructing a [`SelectDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SelectDefinitionError {
    /// The number of selected fields cannot fit the stable definition format.
    #[error("Select field count is too large for the stable format")]
    FieldCountTooLarge,
    /// A selected field name cannot fit the stable definition format.
    #[error("Select field {field} name is too long for the stable format")]
    FieldNameTooLong {
        /// Zero-based index of the rejected field.
        field: usize,
    },
    /// A `DataFusion` expression cannot be persisted exactly and canonically.
    #[error("Select field {field} expression cannot be persisted")]
    Expression {
        /// Zero-based index of the rejected field.
        field: usize,
        /// Expression persistence failure.
        #[source]
        source: ExpressionDefinitionError,
    },
}

/// Select-specific failure while binding an exact input Schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SelectSchemaError {
    /// One persistent expression cannot bind to the input Schema.
    #[error("Select field {field} expression cannot bind to the input Schema")]
    Expression {
        /// Zero-based index of the rejected field.
        field: usize,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
}

impl SelectDefinition {
    /// Admits an ordered collection of named `DataFusion` expressions.
    ///
    /// An empty collection is valid. Field-name uniqueness and the reserved
    /// protocol namespace depend on the complete output Schema and are checked
    /// by the final Schema binding.
    ///
    /// # Errors
    ///
    /// Non-replayable expressions (not immutable or not row-local) are rejected.
    ///
    /// Returns [`SelectDefinitionError`] when the field count or a field name
    /// cannot fit the stable format, or when `DataFusion` cannot round-trip an
    /// expression exactly and canonically.
    pub fn try_new<I, N>(fields: I) -> Result<Self, SelectDefinitionError>
    where
        I: IntoIterator<Item = (N, Expr)>,
        N: Into<String>,
    {
        let mut stored = Vec::new();
        for (field, (name, expression)) in fields.into_iter().enumerate() {
            let count = field
                .checked_add(1)
                .ok_or(SelectDefinitionError::FieldCountTooLarge)?;
            if u32::try_from(count).is_err() {
                return Err(SelectDefinitionError::FieldCountTooLarge);
            }
            let name = name.into();
            if u32::try_from(name.len()).is_err() {
                return Err(SelectDefinitionError::FieldNameTooLong { field });
            }
            let expression = StoredExpression::try_new(expression)
                .map_err(|source| SelectDefinitionError::Expression { field, source })?;
            stored.push(SelectField { name, expression });
        }
        Ok(Self {
            fields: stored.into_boxed_slice(),
        })
    }

    /// Appends named expressions to all fields of an existing input Schema.
    ///
    /// Every expression reads the original input, including when several fields
    /// are appended. No output Schema or field type is required from the caller.
    ///
    /// # Errors
    /// Returns the same persistence errors as [`Self::try_new`]. Duplicate output
    /// names and invalid expressions are rejected during construction or binding.
    pub fn try_extend<I, N>(input: &SchemaRef, fields: I) -> Result<Self, SelectDefinitionError>
    where
        I: IntoIterator<Item = (N, Expr)>,
        N: Into<String>,
    {
        Self::try_new(
            input
                .fields()
                .iter()
                .map(|field| (field.name().clone(), crate::ident(field.name().clone())))
                .chain(
                    fields
                        .into_iter()
                        .map(|(name, expression)| (name.into(), expression)),
                ),
        )
    }

    /// Returns the selected field names and canonical expressions in order.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &Expr)> {
        self.fields
            .iter()
            .map(|field| (field.name.as_str(), field.expression.expression()))
    }

    fn bind_operation(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<(SchemaRef, BoundProjection), SelectSchemaError> {
        let datafusion_schema = DFSchema::try_from(Arc::clone(input_schema))
            .map_err(ExpressionBindError::from)
            .map_err(|source| SelectSchemaError::Expression { field: 0, source })?;
        let mut expressions = Vec::with_capacity(self.fields.len());
        let mut output_fields = Vec::with_capacity(self.fields.len());
        for (field, selected) in self.fields.iter().enumerate() {
            let expression = selected
                .expression
                .bind_with_dfschema(&datafusion_schema)
                .map_err(|source| SelectSchemaError::Expression { field, source })?;
            let mut output_field = Field::new(
                &selected.name,
                expression.output_type().clone(),
                expression.output_nullable(),
            );
            if matches!(selected.expression.expression(), Expr::Column(_)) {
                output_field = output_field.with_metadata(expression.output_metadata().clone());
            }
            output_fields.push(Arc::new(output_field));
            expressions.push(expression);
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            input_schema.metadata().clone(),
        ));
        let operation = BoundProjection::new(
            Arc::clone(input_schema),
            expressions,
            Arc::clone(&output_schema),
        );
        Ok((output_schema, operation))
    }
}

impl SelectDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        self.bind_operation(&inputs[0])
            .map(|(schema, _)| Some(schema))
            .map_err(Into::into)
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Select input arity");
        let (output_schema, operation) = self.bind_operation(input_schema).map_err(schema_error)?;
        Ok(ConstructedOperation::atomic(output_schema, operation))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<SelectDefinition>, DefinitionCodecError> {
    let definition: SelectDefinition = decode_json_payload(payload, "invalid Select payload")?;
    Ok(Box::new(definition))
}
