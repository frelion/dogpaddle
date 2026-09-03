use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};

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

pub(crate) const TAG: u16 = 9;
const DATA: &[DataDeclaration] = &[];

/// One ordered output field of a [`SchemaAlignDefinition`].
///
/// The field's data type is derived from `expression` after binding it to the
/// exact input Schema. A type conversion is therefore represented explicitly
/// by a `DataFusion` `cast` or `try_cast` expression rather than by a second
/// conversion description. `nullable` may equal the expression's derived
/// nullability or widen non-null to nullable; binding rejects narrowing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaAlignField {
    name: String,
    expression: StoredExpression,
    nullable: bool,
    metadata: BTreeMap<String, String>,
}

/// Pure definition of an explicit, expression-based Schema alignment.
///
/// The output contains exactly the declared fields in declaration order.
/// Names, target nullability, Field metadata, and Schema metadata are explicit
/// persistent inputs. Field types are derived from exact-input-bound
/// expressions, including any caller-declared `cast` or `try_cast`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaAlignDefinition {
    fields: Box<[SchemaAlignField]>,
    metadata: BTreeMap<String, String>,
}

/// Materialized exact-Schema-bound alignment.
///
/// This value owns only its exact input Schema, compiled expressions, and
/// exact output Schema. It owns no persistent Store data and retains no
/// Definition.
pub struct SchemaAlignOperation {
    input_schema: SchemaRef,
    expressions: Box<[BoundExpression]>,
    output_schema: SchemaRef,
}

/// Failure while constructing one [`SchemaAlignField`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaAlignFieldError {
    /// The target field name cannot fit the stable definition format.
    #[error("SchemaAlign field name is too long for the stable format")]
    NameTooLong,
    /// The Field metadata entry count cannot fit the stable definition format.
    #[error("SchemaAlign Field metadata has too many entries for the stable format")]
    MetadataCountTooLarge,
    /// One Field metadata key cannot fit the stable definition format.
    #[error("SchemaAlign Field metadata key is too long for the stable format")]
    MetadataKeyTooLong,
    /// One Field metadata value cannot fit the stable definition format.
    #[error("SchemaAlign Field metadata value is too long for the stable format")]
    MetadataValueTooLong,
    /// One Field metadata key was supplied more than once.
    #[error("SchemaAlign Field metadata key {key:?} is duplicated")]
    DuplicateMetadataKey {
        /// The duplicated key.
        key: String,
    },
    /// The `DataFusion` expression cannot be persisted exactly and canonically.
    #[error(transparent)]
    Expression(#[from] ExpressionDefinitionError),
}

/// Failure while constructing a [`SchemaAlignDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaAlignDefinitionError {
    /// The output field count cannot fit the stable definition format.
    #[error("SchemaAlign field count is too large for the stable format")]
    FieldCountTooLarge,
    /// The Schema metadata entry count cannot fit the stable definition format.
    #[error("SchemaAlign Schema metadata has too many entries for the stable format")]
    MetadataCountTooLarge,
    /// One Schema metadata key cannot fit the stable definition format.
    #[error("SchemaAlign Schema metadata key is too long for the stable format")]
    MetadataKeyTooLong,
    /// One Schema metadata value cannot fit the stable definition format.
    #[error("SchemaAlign Schema metadata value is too long for the stable format")]
    MetadataValueTooLong,
    /// One Schema metadata key was supplied more than once.
    #[error("SchemaAlign Schema metadata key {key:?} is duplicated")]
    DuplicateMetadataKey {
        /// The duplicated key.
        key: String,
    },
}

/// SchemaAlign-specific failure while binding an exact input Schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaAlignSchemaError {
    /// One persistent expression cannot bind to the input Schema.
    #[error("SchemaAlign field {field} expression cannot bind to the input Schema")]
    Expression {
        /// Zero-based index of the rejected output field.
        field: usize,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// The target would claim a nullable expression is non-null.
    #[error("SchemaAlign field {field} cannot narrow a nullable expression to non-null")]
    NullabilityNarrowing {
        /// Zero-based index of the rejected output field.
        field: usize,
    },
}

/// SchemaAlign-specific failure during one [`SchemaAlignOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaAlignError {
    /// The Operation was called without an input Change.
    #[error("schema align requires one input Change")]
    MissingInput,
    /// `SchemaAlign` only accepts its Definition's first input port.
    #[error("schema align does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// Runtime input differs from the exact Schema used during binding.
    #[error("schema align input schema differs from its bound schema")]
    InputSchemaMismatch,
    /// One bound expression could not evaluate against the input batch.
    #[error("SchemaAlign field {field} expression evaluation failed")]
    Expression {
        /// Zero-based index of the failed output field.
        field: usize,
        /// Expression evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// Arrow could not construct the aligned record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// The aligned output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

impl SchemaAlignField {
    /// Creates a target field with empty Field metadata.
    ///
    /// The output data type is derived by binding `expression`. Callers express
    /// a type conversion explicitly with a `DataFusion` `cast` or `try_cast`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaAlignFieldError`] when the name cannot fit the stable
    /// format or the expression cannot round-trip exactly and canonically.
    pub fn try_new(
        name: impl Into<String>,
        expression: Expr,
        nullable: bool,
    ) -> Result<Self, SchemaAlignFieldError> {
        Self::try_new_with_metadata(name, expression, nullable, BTreeMap::new())
    }

    /// Creates a target field with explicit Field metadata.
    ///
    /// Metadata entry order does not affect persistence: unique entries are
    /// sorted by key in the canonical Definition payload. Duplicate keys are
    /// rejected rather than silently overwritten.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaAlignFieldError`] when the name or metadata cannot fit
    /// the stable format, a metadata key is duplicated, or the expression
    /// cannot round-trip exactly and canonically.
    pub fn try_new_with_metadata(
        name: impl Into<String>,
        expression: Expr,
        nullable: bool,
        metadata: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, SchemaAlignFieldError> {
        let name = name.into();
        if u32::try_from(name.len()).is_err() {
            return Err(SchemaAlignFieldError::NameTooLong);
        }
        let metadata = collect_metadata(metadata).map_err(|error| match error {
            MetadataConstructionError::Count => SchemaAlignFieldError::MetadataCountTooLarge,
            MetadataConstructionError::Key => SchemaAlignFieldError::MetadataKeyTooLong,
            MetadataConstructionError::Value => SchemaAlignFieldError::MetadataValueTooLong,
            MetadataConstructionError::Duplicate(key) => {
                SchemaAlignFieldError::DuplicateMetadataKey { key }
            }
        })?;
        Ok(Self {
            name,
            expression: StoredExpression::try_new(expression)?,
            nullable,
            metadata,
        })
    }

    /// Returns the explicit target field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the canonical expression that supplies this field.
    #[must_use]
    pub fn expression(&self) -> &Expr {
        self.expression.expression()
    }

    /// Returns the explicit target nullability.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the explicit Field metadata in canonical key order.
    #[must_use]
    pub fn metadata(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl SchemaAlignDefinition {
    /// Creates an alignment with empty output Schema metadata.
    ///
    /// An empty output field collection is valid and preserves the input row
    /// count and diffs.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaAlignDefinitionError::FieldCountTooLarge`] when the field
    /// count cannot fit the stable format.
    pub fn try_new(
        fields: impl IntoIterator<Item = SchemaAlignField>,
    ) -> Result<Self, SchemaAlignDefinitionError> {
        Self::try_new_with_metadata(fields, BTreeMap::new())
    }

    /// Creates an alignment with explicit output Schema metadata.
    ///
    /// Metadata entry order does not affect persistence: unique entries are
    /// sorted by key in the canonical Definition payload. Duplicate keys are
    /// rejected rather than silently overwritten.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaAlignDefinitionError`] when the field count or metadata
    /// cannot fit the stable format, or a metadata key is duplicated.
    pub fn try_new_with_metadata(
        fields: impl IntoIterator<Item = SchemaAlignField>,
        metadata: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, SchemaAlignDefinitionError> {
        let fields = fields.into_iter().collect::<Box<[_]>>();
        if u32::try_from(fields.len()).is_err() {
            return Err(SchemaAlignDefinitionError::FieldCountTooLarge);
        }
        let metadata = collect_metadata(metadata).map_err(|error| match error {
            MetadataConstructionError::Count => SchemaAlignDefinitionError::MetadataCountTooLarge,
            MetadataConstructionError::Key => SchemaAlignDefinitionError::MetadataKeyTooLong,
            MetadataConstructionError::Value => SchemaAlignDefinitionError::MetadataValueTooLong,
            MetadataConstructionError::Duplicate(key) => {
                SchemaAlignDefinitionError::DuplicateMetadataKey { key }
            }
        })?;
        Ok(Self { fields, metadata })
    }

    /// Returns the ordered explicit target fields.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &SchemaAlignField> {
        self.fields.iter()
    }

    /// Returns the explicit output Schema metadata in canonical key order.
    #[must_use]
    pub fn metadata(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl SealedDefinition for SchemaAlignDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces SchemaAlign input arity");
        let mut expressions = Vec::with_capacity(self.fields.len());
        let mut output_fields = Vec::with_capacity(self.fields.len());
        for (field, target) in self.fields.iter().enumerate() {
            let expression = target.expression.bind(Arc::clone(input_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(SchemaAlignSchemaError::Expression { field, source })
                },
            )?;
            if expression.output_nullable() && !target.nullable {
                return Err(Box::new(SchemaAlignSchemaError::NullabilityNarrowing {
                    field,
                }));
            }
            output_fields.push(Arc::new(
                Field::new(
                    &target.name,
                    expression.output_type().clone(),
                    target.nullable,
                )
                .with_metadata(
                    target
                        .metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                ),
            ));
            expressions.push(expression);
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            self.metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ));
        let materialized_input_schema = Arc::clone(input_schema);
        let materialized_schema = Arc::clone(&output_schema);
        Ok(OperationBinding::new(
            Some(output_schema),
            move |_data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                Ok(Box::new(SchemaAlignOperation {
                    input_schema: materialized_input_schema,
                    expressions: expressions.into_boxed_slice(),
                    output_schema: materialized_schema,
                }))
            },
        ))
    }
}

impl OperationDefinition for SchemaAlignDefinition {
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
        let field_count = u32::try_from(self.fields.len())
            .expect("SchemaAlignDefinition::try_new validated the stable field count");
        output.extend_from_slice(&field_count.to_be_bytes());
        for field in &self.fields {
            encode_string(&field.name, output);
            field.expression.encode(output);
            output.push(u8::from(field.nullable));
            encode_metadata(&field.metadata, output);
        }
        encode_metadata(&self.metadata, output);
    }
}

impl Operation for SchemaAlignOperation {
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(SchemaAlignError::MissingInput)?;
        if input.port != 0 {
            return Err(SchemaAlignError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(SchemaAlignError::InputSchemaMismatch.into());
        }

        let columns = self
            .expressions
            .iter()
            .enumerate()
            .map(|(field, expression)| {
                expression
                    .evaluate(input.change.records())
                    .map_err(|source| SchemaAlignError::Expression { field, source })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(input.change.num_rows()));
        let records =
            RecordBatch::try_new_with_options(Arc::clone(&self.output_schema), columns, &options)
                .map_err(SchemaAlignError::Arrow)?;
        let output = Change::try_new(records, input.change.diffs().clone())
            .map_err(SchemaAlignError::Change)?;
        Ok(Action::Complete(Some(output)))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let field_count = cursor.read_u32()?;
    let mut fields = Vec::new();
    for _ in 0..field_count {
        let name = decode_string(&mut cursor, "SchemaAlign field name is invalid UTF-8")?;
        let expression = StoredExpression::decode(&mut cursor)?;
        let nullable = match cursor.read_bytes(1)?[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(DefinitionCodecError::InvalidPayload(
                    "SchemaAlign field nullability is invalid",
                ));
            }
        };
        let metadata = decode_metadata(&mut cursor, "SchemaAlign Field metadata is invalid")?;
        fields.push(SchemaAlignField {
            name,
            expression,
            nullable,
            metadata,
        });
    }
    let metadata = decode_metadata(&mut cursor, "SchemaAlign Schema metadata is invalid")?;
    cursor.finish()?;
    Ok(Box::new(SchemaAlignDefinition {
        fields: fields.into_boxed_slice(),
        metadata,
    }))
}

#[derive(Clone)]
enum MetadataConstructionError {
    Count,
    Key,
    Value,
    Duplicate(String),
}

fn collect_metadata(
    metadata: impl IntoIterator<Item = (String, String)>,
) -> Result<BTreeMap<String, String>, MetadataConstructionError> {
    let mut canonical = BTreeMap::new();
    for (key, value) in metadata {
        if u32::try_from(key.len()).is_err() {
            return Err(MetadataConstructionError::Key);
        }
        if u32::try_from(value.len()).is_err() {
            return Err(MetadataConstructionError::Value);
        }
        match canonical.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(MetadataConstructionError::Duplicate(entry.key().clone()));
            }
        }
    }
    if u32::try_from(canonical.len()).is_err() {
        return Err(MetadataConstructionError::Count);
    }
    Ok(canonical)
}

fn encode_string(value: &str, output: &mut Vec<u8>) {
    let length = u32::try_from(value.len())
        .expect("SchemaAlign construction validated the stable string length");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn encode_metadata(metadata: &BTreeMap<String, String>, output: &mut Vec<u8>) {
    let count = u32::try_from(metadata.len())
        .expect("SchemaAlign construction validated the stable metadata count");
    output.extend_from_slice(&count.to_be_bytes());
    for (key, value) in metadata {
        encode_string(key, output);
        encode_string(value, output);
    }
}

fn decode_string(
    cursor: &mut PayloadCursor<'_>,
    invalid_utf8: &'static str,
) -> Result<String, DefinitionCodecError> {
    let length = usize::try_from(cursor.read_u32()?).map_err(|_| {
        DefinitionCodecError::InvalidPayload("SchemaAlign string length is invalid")
    })?;
    let value = cursor.read_bytes(length)?;
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| DefinitionCodecError::InvalidPayload(invalid_utf8))
}

fn decode_metadata(
    cursor: &mut PayloadCursor<'_>,
    invalid: &'static str,
) -> Result<BTreeMap<String, String>, DefinitionCodecError> {
    let count = cursor.read_u32()?;
    let mut metadata = BTreeMap::new();
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let key = decode_string(cursor, invalid)?;
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(DefinitionCodecError::InvalidPayload(invalid));
        }
        let value = decode_string(cursor, invalid)?;
        previous = Some(key.clone());
        metadata.insert(key, value);
    }
    Ok(metadata)
}
