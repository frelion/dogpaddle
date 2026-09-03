use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, Expr, ExpressionBindError,
    ExpressionDefinitionError, ExpressionError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::Sealed as SealedDefinition,
    expression::{BoundExpression, StoredExpression},
    operation::{Action, Operation, OperationError, OperationInput},
};

pub(crate) const TAG: u16 = 7;
const DATA: &[DataDeclaration] = &[];

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

/// Materialized exact-Schema-bound expression projection.
///
/// This value owns only its compiled expressions and exact output Schema. It
/// owns no persistent Store data and retains no Definition.
pub struct SelectOperation {
    expressions: Box<[BoundExpression]>,
    output_schema: SchemaRef,
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

/// Select-specific failure during one [`SelectOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SelectError {
    /// The input Operation was called without a Change.
    #[error("select requires one input Change")]
    MissingInput,
    /// Select only accepts its Definition's first input port.
    #[error("select does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// One bound expression could not evaluate against the input batch.
    #[error("Select field {field} expression evaluation failed")]
    Expression {
        /// Zero-based index of the failed field.
        field: usize,
        /// Expression evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// Arrow could not construct the selected record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// The selected output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
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
}

impl SealedDefinition for SelectDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Select input arity");
        let mut expressions = Vec::with_capacity(self.fields.len());
        let mut output_fields = Vec::with_capacity(self.fields.len());
        for (field, selected) in self.fields.iter().enumerate() {
            let expression = selected.expression.bind(Arc::clone(input_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(SelectSchemaError::Expression { field, source })
                },
            )?;
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
        let materialized_schema = Arc::clone(&output_schema);
        Ok(OperationBinding::new(
            Some(output_schema),
            move |_data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                Ok(Box::new(SelectOperation {
                    expressions: expressions.into_boxed_slice(),
                    output_schema: materialized_schema,
                }))
            },
        ))
    }
}

impl OperationDefinition for SelectDefinition {
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

impl Operation for SelectOperation {
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(SelectError::MissingInput)?;
        if input.port != 0 {
            return Err(SelectError::InvalidInputPort { port: input.port }.into());
        }

        let columns = self
            .expressions
            .iter()
            .enumerate()
            .map(|(field, expression)| {
                expression
                    .evaluate(input.change.records())
                    .map_err(|source| SelectError::Expression { field, source })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(input.change.num_rows()));
        let records =
            RecordBatch::try_new_with_options(Arc::clone(&self.output_schema), columns, &options)
                .map_err(SelectError::Arrow)?;
        let output =
            Change::try_new(records, input.change.diffs().clone()).map_err(SelectError::Change)?;
        Ok(Action::Complete(Some(output)))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
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
    Ok(Box::new(SelectDefinition {
        fields: fields.into_boxed_slice(),
    }))
}
