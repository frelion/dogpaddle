use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow_schema::{Field, Metadata, Schema, SchemaRef};
use datafusion_common::DFSchema;
use thiserror::Error;

use crate::{
    ConstructedOperation, Expr, ExpressionBindError, ExpressionDefinitionError,
    definition::schema_error,
    expression::{BoundProjection, StoredExpression},
};

/// One ordered projection field, optionally overriding its derived Schema.
///
/// Types come from the expression, including explicit `cast` or `try_cast`.
/// `None` preserves derived nullability and metadata; `Some` supplies an exact
/// override. Nullability may only widen, and empty metadata explicitly clears it.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectField<E = Expr> {
    /// Output field name.
    pub name: String,
    /// Expression bound against the original input Schema.
    pub expression: E,
    /// Optional target nullability; binding rejects nullable-to-non-null narrowing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// Optional exact Field metadata, including an explicit empty map.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

impl<N: Into<String>, E> From<(N, E)> for SelectField<E> {
    fn from((name, expression): (N, E)) -> Self {
        Self {
            name: name.into(),
            expression,
            nullable: None,
            metadata: None,
        }
    }
}

/// Pure definition of an ordered, expression-based projection.
///
/// Every expression is bound independently to the same exact input Schema.
/// The output contains exactly the declared fields in declaration order and
/// may contain no fields. Output types come from `DataFusion`; nullability and
/// metadata follow the expression unless overridden by a [`SelectField`]. Input
/// Schema metadata is preserved unless [`Self::with_metadata`] overrides it.
/// Direct column references retain Field metadata; computed fields start empty.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectDefinition {
    fields: Box<[SelectField<StoredExpression>]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
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
    /// The target would claim a nullable expression is non-null.
    #[error("Select field {field} cannot narrow a nullable expression to non-null")]
    NullabilityNarrowing {
        /// Zero-based index of the rejected output field.
        field: usize,
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
    pub fn try_new<I, F>(fields: I) -> Result<Self, SelectDefinitionError>
    where
        I: IntoIterator<Item = F>,
        F: Into<SelectField>,
    {
        let mut stored = Vec::new();
        for (field, selected) in fields.into_iter().enumerate() {
            let count = field
                .checked_add(1)
                .ok_or(SelectDefinitionError::FieldCountTooLarge)?;
            if u32::try_from(count).is_err() {
                return Err(SelectDefinitionError::FieldCountTooLarge);
            }
            let SelectField {
                name,
                expression,
                nullable,
                metadata,
            } = selected.into();
            if u32::try_from(name.len()).is_err() {
                return Err(SelectDefinitionError::FieldNameTooLong { field });
            }
            let expression = StoredExpression::try_new(expression)
                .map_err(|source| SelectDefinitionError::Expression { field, source })?;
            stored.push(SelectField {
                name,
                expression,
                nullable,
                metadata,
            });
        }
        Ok(Self {
            fields: stored.into_boxed_slice(),
            metadata: None,
        })
    }

    /// Overrides output Schema metadata, including with an explicit empty map.
    ///
    /// The complete output Schema validates metadata when this plan is bound.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<Metadata>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Appends named expressions to all fields of an existing input Schema.
    ///
    /// Every expression reads the original input, including when several fields
    /// are appended. No output Schema or field type is required from the caller.
    ///
    /// # Errors
    /// Returns the same persistence errors as [`Self::try_new`]. Duplicate output
    /// names and invalid expressions are rejected during construction or binding.
    pub fn try_extend<I, F>(input: &SchemaRef, fields: I) -> Result<Self, SelectDefinitionError>
    where
        I: IntoIterator<Item = F>,
        F: Into<SelectField>,
    {
        Self::try_new(
            input
                .fields()
                .iter()
                .map(|field| {
                    SelectField::from((field.name().clone(), crate::ident(field.name().clone())))
                })
                .chain(fields.into_iter().map(Into::into)),
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
            let nullable = selected.nullable.unwrap_or(expression.output_nullable());
            if expression.output_nullable() && !nullable {
                return Err(SelectSchemaError::NullabilityNarrowing { field });
            }
            let mut output_field =
                Field::new(&selected.name, expression.output_type().clone(), nullable);
            if let Some(metadata) = &selected.metadata {
                output_field = output_field.with_metadata(metadata.clone());
            } else if matches!(selected.expression.expression(), Expr::Column(_)) {
                output_field = output_field.with_metadata(expression.output_metadata().clone());
            }
            output_fields.push(Arc::new(output_field));
            expressions.push(expression);
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            self.metadata
                .as_ref()
                .unwrap_or(input_schema.metadata())
                .clone(),
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
