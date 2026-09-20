use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::{DFSchema, ScalarValue, TableReference};

use crate::{
    ConstructedOperation, DefinitionCodecError, Expr, OperationDefinition, OperationKind,
    OperationSchemaError, RuntimeResource,
    codec::PayloadCursor,
    definition::{Sealed as SealedDefinition, schema_error},
    expression::{BoundExpression, StoredExpression},
};

use super::{
    EquiJoinDefinitionError, EquiJoinKind, EquiJoinSchemaError, key_type_supported,
    runtime::{BoundKey, BoundKeyPair},
};

pub(crate) const TAG: u16 = 16;

pub(super) const LEFT_ROWS: &str = "equi_join.left_rows";
pub(super) const RIGHT_ROWS: &str = "equi_join.right_rows";
pub(super) const CONTINUATION: &str = "equi_join.continuation";
pub(super) const KEY_COUNTS: &str = "equi_join.key_counts";
pub(super) const MATCH_COUNTS: &str = "equi_join.match_counts";

pub(crate) struct EquiJoinLayout {
    pub(super) kind: EquiJoinKind,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) candidate_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) keys: Box<[BoundKeyPair]>,
    pub(super) residual: Option<BoundExpression>,
    pub(super) nulls: [Vec<ScalarValue>; 2],
    pub(super) has_residual: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// input Schemas and are validated by the [`OperationDefinition`] binding entrypoint.
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
            ensure_count(key, "key pairs")?;
            let left = store_key(left, key, "left")?;
            let right = store_key(right, key, "right")?;
            stored_keys.push(StoredKeyPair { left, right });
        }
        if stored_keys.is_empty() {
            return Err(EquiJoinDefinitionError::EmptyKeys);
        }

        let mut names = Vec::new();
        for (output, name) in output_names.into_iter().enumerate() {
            ensure_count(output, "output names")?;
            let name = name.into();
            if u32::try_from(name.len()).is_err() {
                return Err(EquiJoinDefinitionError::OutputNameTooLong { output });
            }
            names.push(name);
        }
        let residual = residual.map(store_residual).transpose()?;
        Ok(Self {
            kind,
            keys: stored_keys.into_boxed_slice(),
            output_names: names.into_boxed_slice(),
            residual,
        })
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

impl SealedDefinition for EquiJoinDefinition {
    fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        let [left, right] = inputs else {
            unreachable!()
        };
        self.compile_layout(left, right)
            .map(|layout| Some(layout.output_schema))
    }

    fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
        prefix: &str,
        _resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let [left_schema, right_schema] = input_schemas else {
            unreachable!("the final binding entrypoint enforces Join input arity")
        };
        let layout = self
            .compile_layout(left_schema, right_schema)
            .map_err(schema_error)?;
        let output_schema = Arc::clone(&layout.output_schema);
        let operation = super::construct(layout, data, prefix)?;
        Ok(ConstructedOperation::new(operation, Some(output_schema)))
    }
}

impl EquiJoinDefinition {
    fn compile_layout(
        &self,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> Result<EquiJoinLayout, OperationSchemaError> {
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
        let mut nulls = [Vec::new(), Vec::new()];
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
                    nulls[port].push(
                        ScalarValue::try_from(field.data_type())
                            .map_err(EquiJoinSchemaError::NullPadding)?,
                    );
                }
                output_fields.push(Arc::new(output));
            }
        }
        let output_schema = Arc::new(Schema::new(output_fields));
        let has_residual = residual.is_some();
        Ok(EquiJoinLayout {
            kind: self.kind,
            input_schemas,
            candidate_schema,
            output_schema,
            keys: keys.into_boxed_slice(),
            residual,
            nulls,
            has_residual,
        })
    }
}

impl OperationDefinition for EquiJoinDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::TurnTransform(NonZeroU32::new(2).expect("equi-join has two inputs"))
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.push(self.kind.code());
        put_count(output, self.keys.len());
        for key in &self.keys {
            key.left.encode(output);
            key.right.encode(output);
        }
        put_count(output, self.output_names.len());
        for name in &self.output_names {
            put_count(output, name.len());
            output.extend_from_slice(name.as_bytes());
        }
        match &self.residual {
            None => output.push(0),
            Some(residual) => {
                output.push(1);
                residual.encode(output);
            }
        }
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let kind = EquiJoinKind::from_code(cursor.read_bytes(1)?[0]).ok_or(
        DefinitionCodecError::InvalidPayload("equi-join kind is invalid"),
    )?;
    let key_count = cursor.read_u32()?;
    if key_count == 0 {
        return Err(DefinitionCodecError::InvalidPayload(
            "equi-join key list is empty",
        ));
    }
    let mut keys = Vec::new();
    for _ in 0..key_count {
        let left = decode_key(&mut cursor)?;
        let right = decode_key(&mut cursor)?;
        keys.push(StoredKeyPair { left, right });
    }
    let output_count = cursor.read_u32()?;
    let mut output_names = Vec::new();
    for _ in 0..output_count {
        let length = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("equi-join output name length is invalid")
        })?;
        let name = cursor.read_bytes(length)?;
        let name = std::str::from_utf8(name).map_err(|_| {
            DefinitionCodecError::InvalidPayload("equi-join output name is invalid UTF-8")
        })?;
        output_names.push(name.to_owned());
    }
    let residual = match cursor.read_bytes(1)?[0] {
        0 => None,
        1 => Some(decode_residual(&mut cursor)?),
        _ => {
            return Err(DefinitionCodecError::InvalidPayload(
                "equi-join residual marker is invalid",
            ));
        }
    };
    cursor.finish()?;
    Ok(Box::new(EquiJoinDefinition {
        kind,
        keys: keys.into_boxed_slice(),
        output_names: output_names.into_boxed_slice(),
        residual,
    }))
}

fn store_key(
    expression: Expr,
    key: usize,
    side: &'static str,
) -> Result<StoredExpression, EquiJoinDefinitionError> {
    let expression = StoredExpression::try_new(expression)
        .map_err(|source| EquiJoinDefinitionError::KeyExpression { key, side, source })?;
    if !expression.is_atomic() {
        return Err(EquiJoinDefinitionError::NonImmutableKey { key, side });
    }
    Ok(expression)
}

fn store_residual(expression: Expr) -> Result<StoredExpression, EquiJoinDefinitionError> {
    let expression = StoredExpression::try_new(expression)
        .map_err(|source| EquiJoinDefinitionError::ResidualExpression { source })?;
    if !expression.is_atomic() {
        return Err(EquiJoinDefinitionError::NonImmutableResidual);
    }
    Ok(expression)
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

fn decode_key(cursor: &mut PayloadCursor<'_>) -> Result<StoredExpression, DefinitionCodecError> {
    let expression = StoredExpression::decode(cursor)?;
    if !expression.is_atomic() {
        return Err(DefinitionCodecError::InvalidPayload(
            "equi-join key expression is not immutable",
        ));
    }
    Ok(expression)
}

fn decode_residual(
    cursor: &mut PayloadCursor<'_>,
) -> Result<StoredExpression, DefinitionCodecError> {
    let expression = StoredExpression::decode(cursor)?;
    if !expression.is_atomic() {
        return Err(DefinitionCodecError::InvalidPayload(
            "equi-join residual expression is not immutable",
        ));
    }
    Ok(expression)
}

fn ensure_count(index: usize, kind: &'static str) -> Result<(), EquiJoinDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(EquiJoinDefinitionError::TooMany { kind })
}

fn put_count(output: &mut Vec<u8>, count: usize) {
    output.extend_from_slice(
        &u32::try_from(count)
            .expect("EquiJoinDefinition construction bounds persistent counts")
            .to_be_bytes(),
    );
}
