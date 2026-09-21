use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::DFSchema;
use thiserror::Error;

use crate::{
    ConstructedOperation, DefinitionCodecError, Expr, ExpressionBindError,
    ExpressionDefinitionError, OperationDefinition, OperationKind, RuntimeResource,
    codec::PayloadCursor,
    definition::{Sealed as SealedDefinition, schema_error},
    expression::{BoundProjection, StoredExpression},
};

pub(crate) const TAG: u16 = 7;

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectField {
    name: String,
    expression: StoredExpression,
}

/// Pure definition of an ordered, expression-based projection.
///
/// Every expression is bound independently to the same exact input Schema.
/// The output contains exactly the declared fields in declaration order and
/// may contain no fields. Output types and nullability come from `DataFusion`;
/// input Schema metadata is preserved and output Field metadata starts empty.
#[derive(Clone, Debug, Eq, PartialEq)]
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

    /// Returns the selected field names and canonical expressions in order.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &Expr)> {
        self.fields
            .iter()
            .map(|field| (field.name.as_str(), field.expression.expression()))
    }

    fn is_atomic(&self) -> bool {
        self.fields.iter().all(|field| field.expression.is_atomic())
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
            output_fields.push(Arc::new(Field::new(
                &selected.name,
                expression.output_type().clone(),
                expression.output_nullable(),
            )));
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

impl SealedDefinition for SelectDefinition {
    fn output_schema_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        self.bind_operation(&inputs[0])
            .map(|(schema, _)| Some(schema))
            .map_err(Into::into)
    }

    fn construct_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        input_schemas: &[SchemaRef],
        _data: &mut dogpaddle_store::DataScope<'_>,
        _resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Select input arity");
        let (output_schema, operation) = self.bind_operation(input_schema).map_err(schema_error)?;
        Ok(ConstructedOperation::atomic(output_schema, operation))
    }
}

impl OperationDefinition for SelectDefinition {
    fn kind(&self) -> OperationKind {
        if self.is_atomic() {
            OperationKind::AtomicTransform(NonZeroU32::MIN)
        } else {
            OperationKind::ExclusiveTransform(NonZeroU32::MIN)
        }
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        let count = u32::try_from(self.fields.len())
            .expect("SelectDefinition::try_new validated the stable field count");
        output.extend_from_slice(&count.to_be_bytes());
        for field in &self.fields {
            let name_length = u32::try_from(field.name.len())
                .expect("SelectDefinition::try_new validated the stable field-name length");
            output.extend_from_slice(&name_length.to_be_bytes());
            output.extend_from_slice(field.name.as_bytes());
            field.expression.encode(output);
        }
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    decode_select(payload).map(|definition| Box::new(definition) as Box<dyn OperationDefinition>)
}

fn decode_select(payload: &[u8]) -> Result<SelectDefinition, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let count = cursor.read_u32()?;
    let mut fields = Vec::new();
    for _ in 0..count {
        let name_length = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("Select field-name length is invalid")
        })?;
        let name = cursor.read_bytes(name_length)?;
        let name = std::str::from_utf8(name).map_err(|_| {
            DefinitionCodecError::InvalidPayload("Select field name is invalid UTF-8")
        })?;
        let expression = StoredExpression::decode(&mut cursor)?;
        fields.push(SelectField {
            name: name.to_owned(),
            expression,
        });
    }
    cursor.finish()?;
    Ok(SelectDefinition {
        fields: fields.into_boxed_slice(),
    })
}
