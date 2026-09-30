use std::{collections::BTreeMap, sync::Arc};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, MapAccess, Visitor},
};

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

pub(crate) const TAG: u16 = 9;

/// One ordered output field of a [`SchemaAlignDefinition`].
///
/// The field's data type is derived from `expression` after binding it to the
/// exact input Schema. A type conversion is therefore represented explicitly
/// by a `DataFusion` `cast` or `try_cast` expression rather than by a second
/// conversion description. `nullable` may equal the expression's derived
/// nullability or widen non-null to nullable; binding rejects narrowing.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAlignField {
    name: String,
    expression: StoredExpression,
    nullable: bool,
    #[serde(deserialize_with = "deserialize_unique_metadata")]
    metadata: BTreeMap<String, String>,
}

/// Pure definition of an explicit, expression-based Schema alignment.
///
/// The output contains exactly the declared fields in declaration order.
/// Names, target nullability, Field metadata, and Schema metadata are explicit
/// persistent inputs. Field types are derived from exact-input-bound
/// expressions, including any caller-declared `cast` or `try_cast`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAlignDefinition {
    fields: Box<[SchemaAlignField]>,
    #[serde(deserialize_with = "deserialize_unique_metadata")]
    metadata: BTreeMap<String, String>,
}

fn deserialize_unique_metadata<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    struct UniqueMetadata;

    impl<'de> Visitor<'de> for UniqueMetadata {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("metadata with unique keys")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
            let mut metadata = BTreeMap::new();
            while let Some((key, value)) = input.next_entry::<String, String>()? {
                if metadata.insert(key, value).is_some() {
                    return Err(A::Error::custom("duplicate SchemaAlign metadata key"));
                }
            }
            Ok(metadata)
        }
    }

    deserializer.deserialize_map(UniqueMetadata)
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

impl SchemaAlignField {
    /// Creates a target field with empty Field metadata.
    ///
    /// The output data type is derived by binding `expression`. Callers express
    /// a type conversion explicitly with a `DataFusion` `cast` or `try_cast`.
    ///
    /// # Errors
    ///
    /// Non-replayable expressions (not immutable or not row-local) are rejected.
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
    /// Non-replayable expressions (not immutable or not row-local) are rejected.
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

    fn bind_operation(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<(SchemaRef, BoundProjection), SchemaAlignSchemaError> {
        let datafusion_schema = DFSchema::try_from(Arc::clone(input_schema))
            .map_err(ExpressionBindError::from)
            .map_err(|source| SchemaAlignSchemaError::Expression { field: 0, source })?;
        let mut expressions = Vec::with_capacity(self.fields.len());
        let mut output_fields = Vec::with_capacity(self.fields.len());
        for (field, target) in self.fields.iter().enumerate() {
            let expression = target
                .expression
                .bind_with_dfschema(&datafusion_schema)
                .map_err(|source| SchemaAlignSchemaError::Expression { field, source })?;
            if expression.output_nullable() && !target.nullable {
                return Err(SchemaAlignSchemaError::NullabilityNarrowing { field });
            }
            output_fields.push(Arc::new(
                Field::new(
                    &target.name,
                    expression.output_type().clone(),
                    target.nullable,
                )
                .with_metadata(target.metadata.clone()),
            ));
            expressions.push(expression);
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            self.metadata.clone(),
        ));
        let operation = BoundProjection::new(
            Arc::clone(input_schema),
            expressions,
            Arc::clone(&output_schema),
        );
        Ok((output_schema, operation))
    }
}

impl SchemaAlignDefinition {
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
            .expect("the final binding entrypoint enforces SchemaAlign input arity");
        let (output_schema, operation) = self.bind_operation(input_schema).map_err(schema_error)?;
        Ok(ConstructedOperation::atomic(output_schema, operation))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<SchemaAlignDefinition>, DefinitionCodecError> {
    let definition = decode_json_payload(payload, "invalid SchemaAlign payload")?;
    Ok(Box::new(definition))
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
