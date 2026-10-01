use std::sync::Arc;

use serde::{Deserialize, Serialize};

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::{DFSchema, TableReference};

use crate::{
    ConstructedOperation, Expr, OperationSchemaError,
    definition::schema_error,
    expression::{BoundExpression, StoredExpression},
};

use super::{
    EquiJoinDefinitionError, EquiJoinKind, EquiJoinSchemaError, key_type_supported,
    runtime::{BoundKey, BoundKeyPair},
};

pub(super) const LEFT_ROWS: &str = "equi_join.left_rows";
pub(super) const RIGHT_ROWS: &str = "equi_join.right_rows";
pub(super) const MATCH_COUNTS: &str = "equi_join.match_counts";

pub(crate) struct EquiJoinLayout {
    pub(super) kind: EquiJoinKind,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) candidate_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) keys: Box<[BoundKeyPair]>,
    pub(super) residual: Option<BoundExpression>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredKeyPair {
    left: StoredExpression,
    right: StoredExpression,
}

/// Pure definition of a two-input equality join.
///
/// Port `0` is permanently the left relation and port `1` is the right
/// relation. Keys are evaluated in declaration order. The output always
/// contains left fields only for Semi/Anti, and every left field followed by
/// every right field for Inner/Outer; `output_names`
/// supplies the unique physical names required by a `DogPaddle` Schema.
/// A residual is evaluated on each exact candidate pair before any Outer Join
/// NULL extension; its fields use `left` and `right` qualifiers for ports `0`
/// and `1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EquiJoinDefinition {
    kind: EquiJoinKind,
    keys: Box<[StoredKeyPair]>,
    output_names: Box<[String]>,
    residual: Option<StoredExpression>,
}

impl EquiJoinDefinition {
    /// Reports whether an exact expression type can be used as an equality key.
    ///
    /// Higher-level compilers can use this capability check to retain unsupported
    /// equality expressions as residual predicates instead of constructing a
    /// definition that cannot bind.
    #[must_use]
    pub const fn supports_key_type(data_type: &DataType) -> bool {
        key_type_supported(data_type)
    }

    /// Creates an equality join with immutable ordered key pairs and an optional residual.
    ///
    /// Output-name cardinality and uniqueness depend on the eventual exact
    /// input Schemas and are validated by the [`crate::OperationDefinition`] binding entrypoint.
    /// A residual must produce Boolean when bound to the exact candidate-pair
    /// Schema; only a non-null `true` constitutes a match.
    ///
    /// # Errors
    ///
    /// Returns [`EquiJoinDefinitionError`] when there are no keys, a
    /// stable count or name length overflows, or a key or residual is not an
    /// immutable canonical `DataFusion` expression.
    pub fn try_new<K, N, S>(
        kind: EquiJoinKind,
        keys: K,
        output_names: N,
        residual: Option<Expr>,
    ) -> Result<Self, EquiJoinDefinitionError>
    where
        K: IntoIterator<Item = (Expr, Expr)>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut stored_keys = Vec::new();
        for (key, (left, right)) in keys.into_iter().enumerate() {
            let left = store_key(left, key, "left")?;
            let right = store_key(right, key, "right")?;
            stored_keys.push(StoredKeyPair { left, right });
        }
        let names = output_names.into_iter().map(Into::into).collect::<Vec<_>>();
        let residual = residual.map(store_residual).transpose()?;
        let definition = Self {
            kind,
            keys: stored_keys.into_boxed_slice(),
            output_names: names.into_boxed_slice(),
            residual,
        };
        definition.validate()?;
        Ok(definition)
    }

    /// Returns the relational output semantics.
    #[must_use]
    pub const fn join_kind(&self) -> EquiJoinKind {
        self.kind
    }

    /// Returns ordered left/right key expressions.
    #[must_use]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = (&Expr, &Expr)> {
        self.keys
            .iter()
            .map(|key| (key.left.expression(), key.right.expression()))
    }

    /// Returns physical output names, left-only for Semi/Anti and left-then-right otherwise.
    #[must_use]
    pub fn output_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.output_names.iter().map(String::as_str)
    }

    /// Returns the optional predicate evaluated for equality-key candidate pairs.
    ///
    /// Candidate fields use the stable `left` and `right` qualifiers for input
    /// ports `0` and `1`, respectively.
    #[must_use]
    pub fn residual(&self) -> Option<&Expr> {
        self.residual.as_ref().map(StoredExpression::expression)
    }
}

impl EquiJoinDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        let [left, right] = inputs else {
            unreachable!()
        };
        self.compile_layout(left, right)
            .map(|layout| Some(layout.output_schema))
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let [left_schema, right_schema] = input_schemas else {
            unreachable!("the final binding entrypoint enforces Join input arity")
        };
        let layout = self
            .compile_layout(left_schema, right_schema)
            .map_err(schema_error)?;
        let output_schema = Arc::clone(&layout.output_schema);
        let operation = super::construct(layout, data)?;
        Ok(ConstructedOperation::new(operation, Some(output_schema)))
    }
}

impl EquiJoinDefinition {
    fn validate(&self) -> Result<(), EquiJoinDefinitionError> {
        if self.keys.is_empty() {
            return Err(EquiJoinDefinitionError::EmptyKeys);
        }
        if let Some(index) = self.keys.len().checked_sub(1) {
            ensure_count(index, "key pairs")?;
        }
        for (output, name) in self.output_names.iter().enumerate() {
            ensure_count(output, "output names")?;
            if u32::try_from(name.len()).is_err() {
                return Err(EquiJoinDefinitionError::OutputNameTooLong { output });
            }
        }
        Ok(())
    }

    fn compile_layout(
        &self,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> Result<EquiJoinLayout, OperationSchemaError> {
        self.validate()?;
        let expected_names = left_schema.fields().len()
            + if self.kind.left_only() {
                0
            } else {
                right_schema.fields().len()
            };
        if self.output_names.len() != expected_names {
            return Err(Box::new(EquiJoinSchemaError::OutputNameCount {
                expected: expected_names,
                actual: self.output_names.len(),
            }));
        }

        let keys = bind_keys(&self.keys, left_schema, right_schema)?;
        let candidate_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let residual = self
            .residual
            .as_ref()
            .map(|stored| bind_residual(stored, &candidate_schema, left_schema.fields().len()))
            .transpose()?;

        let input_schemas = [Arc::clone(left_schema), Arc::clone(right_schema)];
        let mut output_fields = Vec::with_capacity(expected_names);
        for (port, schema) in input_schemas.iter().enumerate() {
            if port == 1 && self.kind.left_only() {
                break;
            }
            let pad = self.kind.preserves(1 - port);
            for field in schema.fields() {
                let mut output = field
                    .as_ref()
                    .clone()
                    .with_name(&self.output_names[output_fields.len()]);
                if pad {
                    output = output.with_nullable(true);
                }
                output_fields.push(Arc::new(output));
            }
        }
        let output_schema = Arc::new(Schema::new(output_fields));
        dogpaddle_change::validate_schema(&output_schema)?;
        Ok(EquiJoinLayout {
            kind: self.kind,
            input_schemas,
            candidate_schema,
            output_schema,
            keys: keys.into_boxed_slice(),
            residual,
        })
    }
}

fn store_key(
    expression: Expr,
    key: usize,
    side: &'static str,
) -> Result<StoredExpression, EquiJoinDefinitionError> {
    StoredExpression::try_new(expression).map_err(|source| EquiJoinDefinitionError::KeyExpression {
        key,
        side,
        source,
    })
}

fn store_residual(expression: Expr) -> Result<StoredExpression, EquiJoinDefinitionError> {
    StoredExpression::try_new(expression)
        .map_err(|source| EquiJoinDefinitionError::ResidualExpression { source })
}

fn bind_keys(
    keys: &[StoredKeyPair],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Vec<BoundKeyPair>, OperationSchemaError> {
    keys.iter()
        .enumerate()
        .map(|(key, stored)| {
            let left = bind_key(&stored.left, Arc::clone(left_schema), key, "left")?;
            let right = bind_key(&stored.right, Arc::clone(right_schema), key, "right")?;
            if left.output_type() != right.output_type() {
                return Err(Box::new(EquiJoinSchemaError::KeyTypeMismatch {
                    key,
                    left: left.output_type().clone(),
                    right: right.output_type().clone(),
                }) as OperationSchemaError);
            }
            if !key_type_supported(left.output_type()) {
                return Err(Box::new(EquiJoinSchemaError::UnsupportedKeyType {
                    key,
                    data_type: left.output_type().clone(),
                }) as OperationSchemaError);
            }
            Ok(BoundKeyPair {
                left: BoundKey::new(left),
                right: BoundKey::new(right),
            })
        })
        .collect()
}

fn bind_key(
    stored: &StoredExpression,
    schema: SchemaRef,
    key: usize,
    side: &'static str,
) -> Result<BoundExpression, OperationSchemaError> {
    stored.bind(schema).map_err(|source| {
        Box::new(EquiJoinSchemaError::KeyExpression { key, side, source }) as OperationSchemaError
    })
}

fn bind_residual(
    residual: &StoredExpression,
    candidate_schema: &SchemaRef,
    left_field_count: usize,
) -> Result<BoundExpression, EquiJoinSchemaError> {
    let mut qualifiers = vec![Some(TableReference::bare("left")); left_field_count];
    qualifiers.extend(vec![
        Some(TableReference::bare("right"));
        candidate_schema.fields().len() - left_field_count
    ]);
    let datafusion_schema =
        DFSchema::from_field_specific_qualified_schema(qualifiers, candidate_schema).map_err(
            |source| EquiJoinSchemaError::ResidualExpression {
                source: source.into(),
            },
        )?;
    let residual = residual
        .bind_with_dfschema(&datafusion_schema)
        .map_err(|source| EquiJoinSchemaError::ResidualExpression { source })?;
    if residual.output_type() != &DataType::Boolean {
        return Err(EquiJoinSchemaError::ResidualType {
            actual: residual.output_type().clone(),
        });
    }
    Ok(residual)
}

fn ensure_count(index: usize, kind: &'static str) -> Result<(), EquiJoinDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(EquiJoinDefinitionError::TooMany { kind })
}
