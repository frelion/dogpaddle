use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DFSchema, ScalarValue, TableReference};

use crate::{
    DefinitionCodecError, Expr, OperationSchemaError,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::{ConstructedOperation, schema_error},
    expression::{BoundExpression, StoredExpression},
    operation::relation::indexable,
};

use super::{
    AsOfDirection, AsOfEqualityMode, AsOfJoinDefinitionError, AsOfJoinKind, AsOfJoinSchemaError,
    AsOfTieFallback,
    runtime::{BoundEqualityPair, BoundOrderPair, BoundScalar, BoundTieBreak},
};

pub(crate) const TAG: u16 = 17;

pub(super) const LEFT_ROWS: &str = "asof_join.left_rows";
pub(super) const RIGHT_ROWS: &str = "asof_join.right_rows";
pub(super) const CONTINUATION: &str = "asof_join.continuation";

pub(crate) struct AsOfJoinLayout {
    pub(super) kind: AsOfJoinKind,
    pub(super) direction: AsOfDirection,
    pub(super) tie_fallback: AsOfTieFallback,
    pub(super) tolerance: Option<u128>,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) candidate_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) equalities: Box<[BoundEqualityPair]>,
    pub(super) orders: Box<[BoundOrderPair]>,
    pub(super) ties: Box<[BoundTieBreak]>,
    pub(super) right_nulls: Vec<ScalarValue>,
    pub(super) residual: Option<BoundExpression>,
}

/// One left/right equality expression pair and its NULL comparison semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsOfEqualityKey {
    mode: AsOfEqualityMode,
    left: Expr,
    right: Expr,
}

impl AsOfEqualityKey {
    /// Creates one equality-partition expression pair.
    #[must_use]
    pub fn new(mode: AsOfEqualityMode, left: Expr, right: Expr) -> Self {
        Self { mode, left, right }
    }

    /// Returns this key's NULL comparison semantics.
    #[must_use]
    pub const fn mode(&self) -> AsOfEqualityMode {
        self.mode
    }

    /// Returns the expression evaluated against the left input.
    #[must_use]
    pub const fn left(&self) -> &Expr {
        &self.left
    }

    /// Returns the expression evaluated against the right input.
    #[must_use]
    pub const fn right(&self) -> &Expr {
        &self.right
    }
}

/// One left/right lexicographic ASOF order expression pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsOfOrderKey {
    left: Expr,
    right: Expr,
}

impl AsOfOrderKey {
    /// Creates one order expression pair.
    #[must_use]
    pub fn new(left: Expr, right: Expr) -> Self {
        Self { left, right }
    }

    /// Returns the expression evaluated against the left input.
    #[must_use]
    pub const fn left(&self) -> &Expr {
        &self.left
    }

    /// Returns the expression evaluated against the right input.
    #[must_use]
    pub const fn right(&self) -> &Expr {
        &self.right
    }
}

/// One right-only expression in the deterministic candidate rank.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsOfTieBreak {
    value: Expr,
    descending: bool,
    nulls_first: bool,
}

impl AsOfTieBreak {
    /// Creates one right-only candidate ranking expression.
    #[must_use]
    pub fn new(value: Expr, descending: bool, nulls_first: bool) -> Self {
        Self {
            value,
            descending,
            nulls_first,
        }
    }

    /// Returns the right expression.
    #[must_use]
    pub const fn expression(&self) -> &Expr {
        &self.value
    }

    /// Returns whether greater values sort before lesser values.
    #[must_use]
    pub const fn descending(&self) -> bool {
        self.descending
    }

    /// Returns whether NULL sorts before every non-NULL value.
    #[must_use]
    pub const fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredEqualityKey {
    mode: AsOfEqualityMode,
    left: StoredExpression,
    right: StoredExpression,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredOrderKey {
    left: StoredExpression,
    right: StoredExpression,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTieBreak {
    value: StoredExpression,
    descending: bool,
    nulls_first: bool,
}

/// Pure definition of a dynamic two-input ASOF join.
///
/// Port `0` is permanently the left relation and port `1` is the right
/// relation. Equality keys form a partition, order keys select the nearest
/// eligible order value according to [`AsOfDirection`], and right-only tie
/// breaks select a deterministic exact right row at that value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AsOfJoinDefinition {
    kind: AsOfJoinKind,
    direction: AsOfDirection,
    equalities: Box<[StoredEqualityKey]>,
    orders: Box<[StoredOrderKey]>,
    ties: Box<[StoredTieBreak]>,
    tie_fallback: AsOfTieFallback,
    tolerance: Option<u128>,
    output_names: Box<[String]>,
    residual: Option<StoredExpression>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    kind: AsOfJoinKind,
    direction: AsOfDirection,
    equalities: Box<[StoredEqualityKey]>,
    orders: Box<[StoredOrderKey]>,
    ties: Box<[StoredTieBreak]>,
    tie_fallback: AsOfTieFallback,
    tolerance: Option<u128>,
    output_names: Box<[String]>,
    residual: Option<StoredExpression>,
}

impl Payload {
    fn into_definition(self) -> Result<AsOfJoinDefinition, &'static str> {
        if self.orders.is_empty() {
            return Err("ASOF join order key list is empty");
        }
        Ok(AsOfJoinDefinition {
            kind: self.kind,
            direction: self.direction,
            equalities: self.equalities,
            orders: self.orders,
            ties: self.ties,
            tie_fallback: self.tie_fallback,
            tolerance: self.tolerance,
            output_names: self.output_names,
            residual: self.residual,
        })
    }
}

impl<'de> Deserialize<'de> for AsOfJoinDefinition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Payload::deserialize(deserializer)?
            .into_definition()
            .map_err(D::Error::custom)
    }
}

impl AsOfJoinDefinition {
    /// Reports whether a scalar type has a stable ASOF index encoding.
    #[must_use]
    pub fn supports_index_type(data_type: &DataType) -> bool {
        indexable(data_type)
    }

    /// Reports whether a scalar type supports absolute ASOF distance.
    #[must_use]
    pub const fn supports_distance_type(data_type: &DataType) -> bool {
        distance_capable(data_type)
    }

    /// Creates a persistent ASOF definition from immutable expressions.
    ///
    /// Equality keys and tie breaks may be empty. At least one order pair is
    /// required. Output-name cardinality and exact expression types depend on
    /// the eventual input Schemas and are checked by the final
    /// [`crate::OperationDefinition`] binding entrypoint. The optional residual is a
    /// candidate-eligibility predicate evaluated before nearest selection;
    /// only a non-NULL `true` candidate remains eligible. A tolerance requires
    /// exactly one distance-capable order pair and is measured in that type's
    /// physical units (timestamp unit, date days, or decimal least-significant
    /// scaled unit).
    ///
    /// # Errors
    ///
    /// Returns [`AsOfJoinDefinitionError`] when the order list is empty, a
    /// stable count or name length overflows, or any expression is not an
    /// immutable canonical `DataFusion` expression.
    #[expect(
        clippy::too_many_arguments,
        reason = "the persistent ASOF policy is intentionally explicit at construction"
    )]
    pub fn try_new<E, O, T, N, S>(
        kind: AsOfJoinKind,
        direction: AsOfDirection,
        equalities: E,
        orders: O,
        ties: T,
        tie_fallback: AsOfTieFallback,
        tolerance: Option<u128>,
        output_names: N,
        residual: Option<Expr>,
    ) -> Result<Self, AsOfJoinDefinitionError>
    where
        E: IntoIterator<Item = AsOfEqualityKey>,
        O: IntoIterator<Item = AsOfOrderKey>,
        T: IntoIterator<Item = AsOfTieBreak>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut stored_equalities = Vec::new();
        for (index, key) in equalities.into_iter().enumerate() {
            ensure_count(index, "equality keys")?;
            stored_equalities.push(StoredEqualityKey {
                mode: key.mode,
                left: store_expression(key.left, "left equality", index)?,
                right: store_expression(key.right, "right equality", index)?,
            });
        }

        let mut stored_orders = Vec::new();
        for (index, key) in orders.into_iter().enumerate() {
            ensure_count(index, "order keys")?;
            stored_orders.push(StoredOrderKey {
                left: store_expression(key.left, "left order", index)?,
                right: store_expression(key.right, "right order", index)?,
            });
        }
        if stored_orders.is_empty() {
            return Err(AsOfJoinDefinitionError::EmptyOrderKeys);
        }

        let mut stored_ties = Vec::new();
        for (index, tie) in ties.into_iter().enumerate() {
            ensure_count(index, "tie breaks")?;
            stored_ties.push(StoredTieBreak {
                value: store_expression(tie.value, "tie break", index)?,
                descending: tie.descending,
                nulls_first: tie.nulls_first,
            });
        }

        let mut names = Vec::new();
        for (output, name) in output_names.into_iter().enumerate() {
            ensure_count(output, "output names")?;
            let name = name.into();
            if u32::try_from(name.len()).is_err() {
                return Err(AsOfJoinDefinitionError::OutputNameTooLong { output });
            }
            names.push(name);
        }
        let residual = residual.map(store_residual).transpose()?;

        Ok(Self {
            kind,
            direction,
            equalities: stored_equalities.into_boxed_slice(),
            orders: stored_orders.into_boxed_slice(),
            ties: stored_ties.into_boxed_slice(),
            tie_fallback,
            tolerance,
            output_names: names.into_boxed_slice(),
            residual,
        })
    }

    /// Returns the relational output semantics.
    #[must_use]
    pub const fn join_kind(&self) -> AsOfJoinKind {
        self.kind
    }

    /// Returns the ordered candidate-search direction.
    #[must_use]
    pub const fn direction(&self) -> AsOfDirection {
        self.direction
    }

    /// Returns ordered equality partition keys.
    #[must_use]
    pub fn equality_keys(&self) -> impl ExactSizeIterator<Item = (AsOfEqualityMode, &Expr, &Expr)> {
        self.equalities
            .iter()
            .map(|key| (key.mode, key.left.expression(), key.right.expression()))
    }

    /// Returns the non-empty lexicographic order keys.
    #[must_use]
    pub fn order_keys(&self) -> impl ExactSizeIterator<Item = (&Expr, &Expr)> {
        self.orders
            .iter()
            .map(|key| (key.left.expression(), key.right.expression()))
    }

    /// Returns ordered right-only tie-break expressions and sort options.
    #[must_use]
    pub fn tie_breaks(&self) -> impl ExactSizeIterator<Item = (&Expr, bool, bool)> {
        self.ties
            .iter()
            .map(|tie| (tie.value.expression(), tie.descending, tie.nulls_first))
    }

    /// Returns the fallback used after all explicit tie breaks compare equal.
    #[must_use]
    pub const fn tie_fallback(&self) -> AsOfTieFallback {
        self.tie_fallback
    }

    /// Returns the inclusive maximum order distance in the bound order type's physical units.
    #[must_use]
    pub const fn tolerance(&self) -> Option<u128> {
        self.tolerance
    }

    /// Returns physical output names, left-only for Semi/Anti and left-then-right otherwise.
    #[must_use]
    pub fn output_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.output_names.iter().map(String::as_str)
    }

    /// Returns the optional candidate-eligibility predicate.
    ///
    /// Candidate fields use the stable `left` and `right` qualifiers for input
    /// ports `0` and `1`, respectively.
    #[must_use]
    pub fn residual(&self) -> Option<&Expr> {
        self.residual.as_ref().map(StoredExpression::expression)
    }
}

impl AsOfJoinDefinition {
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
            unreachable!("the final binding entrypoint enforces ASOF join input arity")
        };
        let layout = self
            .compile_layout(left_schema, right_schema)
            .map_err(schema_error)?;
        let output_schema = Arc::clone(&layout.output_schema);
        let operation = super::construct(layout, data)?;
        Ok(ConstructedOperation::new(operation, Some(output_schema)))
    }
}

impl AsOfJoinDefinition {
    fn compile_layout(
        &self,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> Result<AsOfJoinLayout, OperationSchemaError> {
        let expected_names = left_schema.fields().len()
            + if self.kind.left_only() {
                0
            } else {
                right_schema.fields().len()
            };
        if self.output_names.len() != expected_names {
            return Err(Box::new(AsOfJoinSchemaError::OutputNameCount {
                expected: expected_names,
                actual: self.output_names.len(),
            }));
        }

        let equalities = bind_equalities(&self.equalities, left_schema, right_schema)?;
        let orders = bind_orders(&self.orders, left_schema, right_schema)?;
        if self.direction.nearest()
            && (orders.len() != 1 || !distance_capable(orders[0].left.field.data_type()))
        {
            return Err(Box::new(AsOfJoinSchemaError::DistanceOrder {
                feature: "nearest direction",
            }));
        }
        if self.tolerance.is_some()
            && (orders.len() != 1 || !distance_capable(orders[0].left.field.data_type()))
        {
            return Err(Box::new(AsOfJoinSchemaError::DistanceOrder {
                feature: "tolerance",
            }));
        }
        let ties = bind_ties(&self.ties, right_schema)?;
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
        for field in left_schema.fields() {
            output_fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(&self.output_names[output_fields.len()]),
            ));
        }
        let mut right_nulls = Vec::new();
        if !self.kind.left_only() {
            for field in right_schema.fields() {
                let mut output = field
                    .as_ref()
                    .clone()
                    .with_name(&self.output_names[output_fields.len()]);
                if self.kind == AsOfJoinKind::LeftOuter {
                    output = output.with_nullable(true);
                    right_nulls.push(
                        ScalarValue::try_from(field.data_type())
                            .map_err(AsOfJoinSchemaError::NullPadding)?,
                    );
                }
                output_fields.push(Arc::new(output));
            }
        }
        let output_schema = Arc::new(Schema::new(output_fields));
        Ok(AsOfJoinLayout {
            kind: self.kind,
            direction: self.direction,
            tie_fallback: self.tie_fallback,
            tolerance: self.tolerance,
            input_schemas,
            candidate_schema,
            output_schema,
            equalities: equalities.into_boxed_slice(),
            orders: orders.into_boxed_slice(),
            ties: ties.into_boxed_slice(),
            right_nulls,
            residual,
        })
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<AsOfJoinDefinition>, DefinitionCodecError> {
    let definition = parse_json_payload::<Payload>(payload)?
        .into_definition()
        .map_err(DefinitionCodecError::InvalidPayload)?;
    require_canonical_json_payload(&definition, payload, "invalid ASOF join payload")?;
    Ok(Box::new(definition))
}

fn store_expression(
    expression: Expr,
    role: &'static str,
    index: usize,
) -> Result<StoredExpression, AsOfJoinDefinitionError> {
    StoredExpression::try_new(expression).map_err(|source| AsOfJoinDefinitionError::Expression {
        role,
        index,
        source,
    })
}

fn store_residual(expression: Expr) -> Result<StoredExpression, AsOfJoinDefinitionError> {
    StoredExpression::try_new(expression)
        .map_err(|source| AsOfJoinDefinitionError::ResidualExpression { source })
}

fn bind_equalities(
    keys: &[StoredEqualityKey],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Vec<BoundEqualityPair>, OperationSchemaError> {
    keys.iter()
        .enumerate()
        .map(|(index, key)| {
            let left = bind_scalar(&key.left, left_schema, "equality", index, "left")?;
            let right = bind_scalar(&key.right, right_schema, "equality", index, "right")?;
            ensure_pair_type(&left, &right, "equality", index)?;
            ensure_indexable(&left, "equality", index)?;
            Ok(BoundEqualityPair {
                mode: key.mode,
                left,
                right,
            })
        })
        .collect()
}

fn bind_orders(
    keys: &[StoredOrderKey],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Vec<BoundOrderPair>, OperationSchemaError> {
    keys.iter()
        .enumerate()
        .map(|(index, key)| {
            let left = bind_scalar(&key.left, left_schema, "order", index, "left")?;
            let right = bind_scalar(&key.right, right_schema, "order", index, "right")?;
            ensure_pair_type(&left, &right, "order", index)?;
            ensure_indexable(&left, "order", index)?;
            Ok(BoundOrderPair { left, right })
        })
        .collect()
}

fn bind_ties(
    ties: &[StoredTieBreak],
    right_schema: &SchemaRef,
) -> Result<Vec<BoundTieBreak>, OperationSchemaError> {
    ties.iter()
        .enumerate()
        .map(|(index, tie)| {
            let value = bind_scalar(&tie.value, right_schema, "tie break", index, "right")?;
            ensure_indexable(&value, "tie break", index)?;
            Ok(BoundTieBreak {
                value,
                descending: tie.descending,
                nulls_first: tie.nulls_first,
            })
        })
        .collect()
}

fn bind_residual(
    residual: &StoredExpression,
    candidate_schema: &SchemaRef,
    left_field_count: usize,
) -> Result<BoundExpression, AsOfJoinSchemaError> {
    let mut qualifiers = vec![Some(TableReference::bare("left")); left_field_count];
    qualifiers.extend(vec![
        Some(TableReference::bare("right"));
        candidate_schema.fields().len() - left_field_count
    ]);
    let datafusion_schema =
        DFSchema::from_field_specific_qualified_schema(qualifiers, candidate_schema).map_err(
            |source| AsOfJoinSchemaError::ResidualExpression {
                source: source.into(),
            },
        )?;
    let residual = residual
        .bind_with_dfschema(&datafusion_schema)
        .map_err(|source| AsOfJoinSchemaError::ResidualExpression { source })?;
    if residual.output_type() != &DataType::Boolean {
        return Err(AsOfJoinSchemaError::ResidualType {
            actual: residual.output_type().clone(),
        });
    }
    Ok(residual)
}

fn bind_scalar(
    stored: &StoredExpression,
    schema: &SchemaRef,
    role: &'static str,
    index: usize,
    side: &'static str,
) -> Result<BoundScalar, OperationSchemaError> {
    let expression = stored.bind(Arc::clone(schema)).map_err(|source| {
        Box::new(AsOfJoinSchemaError::Expression {
            role,
            index,
            side,
            source,
        }) as OperationSchemaError
    })?;
    let field = bound_field(&expression);
    Ok(BoundScalar { expression, field })
}

fn bound_field(expression: &BoundExpression) -> Arc<Field> {
    Arc::new(
        Field::new(
            "value",
            expression.output_type().clone(),
            expression.output_nullable(),
        )
        .with_metadata(expression.output_metadata().clone()),
    )
}

fn ensure_pair_type(
    left: &BoundScalar,
    right: &BoundScalar,
    role: &'static str,
    index: usize,
) -> Result<(), OperationSchemaError> {
    if left.field.data_type() == right.field.data_type() {
        Ok(())
    } else {
        Err(Box::new(AsOfJoinSchemaError::TypeMismatch {
            role,
            index,
            left: left.field.data_type().clone(),
            right: right.field.data_type().clone(),
        }))
    }
}

fn ensure_indexable(
    scalar: &BoundScalar,
    role: &'static str,
    index: usize,
) -> Result<(), OperationSchemaError> {
    if indexable(scalar.field.data_type()) {
        Ok(())
    } else {
        Err(Box::new(AsOfJoinSchemaError::UnsupportedType {
            role,
            index,
            data_type: scalar.field.data_type().clone(),
        }))
    }
}

const fn distance_capable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

fn ensure_count(index: usize, kind: &'static str) -> Result<(), AsOfJoinDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(AsOfJoinDefinitionError::TooMany { kind })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use crate::{OperationSetupError, RuntimeResource, col, decode_definition, encode_definition};
    use dogpaddle_store::StoreSetup;

    use super::*;
    use crate::operation::transform::asof_join::AsOfEquidistantPreference;

    fn schema(fields: impl IntoIterator<Item = Field>) -> SchemaRef {
        Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()))
    }

    fn construct(
        definition: &AsOfJoinDefinition,
        inputs: &[SchemaRef],
    ) -> Result<crate::ConstructedOperation, OperationSetupError> {
        let mut setup = StoreSetup::new();
        crate::OperationDefinition::from(definition.clone()).construct(
            inputs,
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
    }

    fn definition(direction: AsOfDirection, tolerance: Option<u128>) -> AsOfJoinDefinition {
        AsOfJoinDefinition::try_new(
            AsOfJoinKind::LeftOuter,
            direction,
            [AsOfEqualityKey::new(
                AsOfEqualityMode::NotDistinct,
                col("symbol"),
                col("symbol"),
            )],
            [AsOfOrderKey::new(col("at"), col("at"))],
            [AsOfTieBreak::new(col("sequence"), true, false)],
            AsOfTieFallback::CanonicalAscending,
            tolerance,
            [
                "left_symbol",
                "left_at",
                "right_symbol",
                "right_at",
                "sequence",
            ],
            Some(col("left.at").gt_eq(col("right.at"))),
        )
        .unwrap()
    }

    #[test]
    fn tag_payload_round_trips_every_persistent_option() {
        let definition_value = definition(
            AsOfDirection::Nearest {
                allow_exact: false,
                equidistant: AsOfEquidistantPreference::Forward,
            },
            Some(u128::MAX),
        );
        let encoded = encode_definition(&definition_value.clone().into());
        let header = b"dogpaddle.operation\0".len() + 2;
        assert_eq!(&encoded[header..][..2], &TAG.to_be_bytes());
        let payload = &encoded[header + 2..];
        assert_eq!(
            serde_json::from_slice::<AsOfJoinDefinition>(payload).unwrap(),
            definition_value
        );
        let payload = std::str::from_utf8(payload).unwrap();
        assert!(payload.contains("\"kind\":\"LeftOuter\""));
        assert!(payload.contains(
            "\"direction\":{\"Nearest\":{\"allow_exact\":false,\"equidistant\":\"Forward\"}}"
        ));
        assert!(payload.contains(&u128::MAX.to_string()));
        let decoded = decode_definition(&encoded).unwrap();
        assert_eq!(encode_definition(&decoded), encoded);
        for length in 0..encoded.len() {
            assert!(
                decode_definition(&encoded[..length]).is_err(),
                "ASOF definition prefix {length} was accepted"
            );
        }

        let mut invalid_exact = encoded[..header + 2].to_vec();
        invalid_exact.extend_from_slice(
            payload
                .replacen("\"allow_exact\":false", "\"allow_exact\":2", 1)
                .as_bytes(),
        );
        assert!(matches!(
            decode_definition(&invalid_exact),
            Err(DefinitionCodecError::InvalidJsonPayload { .. })
        ));

        let mut without_residual = definition(AsOfDirection::Backward { allow_exact: true }, None);
        without_residual.residual = None;
        let encoded = encode_definition(&without_residual.into());
        let mut invalid_residual = encoded[..header + 2].to_vec();
        let payload = std::str::from_utf8(&encoded[header + 2..]).unwrap();
        invalid_residual.extend_from_slice(
            payload
                .replacen("\"residual\":null", "\"residual\":2", 1)
                .as_bytes(),
        );
        assert!(matches!(
            decode_definition(&invalid_residual),
            Err(DefinitionCodecError::InvalidJsonPayload { .. })
        ));
    }

    #[test]
    fn every_persistent_policy_name_is_fixed() {
        assert_eq!(
            [
                AsOfJoinKind::Inner,
                AsOfJoinKind::LeftOuter,
                AsOfJoinKind::LeftSemi,
                AsOfJoinKind::LeftAnti,
            ]
            .map(|kind| serde_json::to_string(&kind).unwrap()),
            ["\"Inner\"", "\"LeftOuter\"", "\"LeftSemi\"", "\"LeftAnti\""].map(str::to_owned)
        );
        assert_eq!(
            serde_json::to_string(&AsOfDirection::Nearest {
                allow_exact: false,
                equidistant: AsOfEquidistantPreference::Forward,
            })
            .unwrap(),
            "{\"Nearest\":{\"allow_exact\":false,\"equidistant\":\"Forward\"}}"
        );
        assert_eq!(
            serde_json::to_string(&AsOfEqualityMode::NotDistinct).unwrap(),
            "\"NotDistinct\""
        );
        assert_eq!(
            serde_json::to_string(&AsOfTieFallback::CanonicalAscending).unwrap(),
            "\"CanonicalAscending\""
        );
    }

    #[test]
    fn bind_preserves_exact_fields_and_outer_nullability() {
        let left = schema([
            Field::new("symbol", DataType::Utf8, false),
            Field::new("at", DataType::Int64, false),
        ]);
        let right = schema([
            Field::new("symbol", DataType::Utf8, false),
            Field::new("at", DataType::Int64, false),
            Field::new("sequence", DataType::UInt64, false),
        ]);
        let definition = definition(AsOfDirection::Backward { allow_exact: true }, Some(7));
        let binding = construct(&definition, &[left, right]).unwrap();
        let output = binding.output_schema().unwrap();
        assert_eq!(output.fields().len(), 5);
        assert!(!output.field(0).is_nullable());
        assert!(!output.field(1).is_nullable());
        assert!(output.field(2).is_nullable());
        assert!(output.field(3).is_nullable());
        assert!(output.field(4).is_nullable());
    }

    #[test]
    fn bind_rejects_pair_type_drift_and_non_distance_tolerance() {
        let strings = schema([
            Field::new("symbol", DataType::Utf8, false),
            Field::new("at", DataType::Utf8, false),
            Field::new("sequence", DataType::UInt64, false),
        ]);
        let mismatched = schema([
            Field::new("symbol", DataType::Binary, false),
            Field::new("at", DataType::Utf8, false),
            Field::new("sequence", DataType::UInt64, false),
        ]);
        let unbounded = definition(AsOfDirection::Backward { allow_exact: true }, None);
        assert!(construct(&unbounded, &[Arc::clone(&strings), mismatched]).is_err());

        let bounded = definition(AsOfDirection::Backward { allow_exact: true }, Some(1));
        assert!(construct(&bounded, &[Arc::clone(&strings), strings]).is_err());
    }

    #[test]
    fn bind_requires_a_boolean_candidate_residual() {
        let left = schema([
            Field::new("symbol", DataType::Utf8, false),
            Field::new("at", DataType::Int64, false),
        ]);
        let right = schema([
            Field::new("symbol", DataType::Utf8, false),
            Field::new("at", DataType::Int64, false),
            Field::new("sequence", DataType::UInt64, false),
        ]);
        let mut non_boolean = definition(AsOfDirection::Backward { allow_exact: true }, None);
        non_boolean.residual = Some(store_residual(col("left.at")).unwrap());
        let Err(OperationSetupError::Schema { source }) = construct(&non_boolean, &[left, right])
        else {
            panic!("non-Boolean ASOF residual unexpectedly bound");
        };
        assert!(matches!(
            source.downcast_ref::<AsOfJoinSchemaError>(),
            Some(AsOfJoinSchemaError::ResidualType {
                actual: DataType::Int64
            })
        ));
    }

    #[test]
    fn construction_rejects_an_empty_order_list() {
        assert!(matches!(
            AsOfJoinDefinition::try_new(
                AsOfJoinKind::Inner,
                AsOfDirection::Backward { allow_exact: true },
                std::iter::empty::<AsOfEqualityKey>(),
                std::iter::empty::<AsOfOrderKey>(),
                std::iter::empty::<AsOfTieBreak>(),
                AsOfTieFallback::Reject,
                None,
                ["left", "right"],
                None,
            ),
            Err(AsOfJoinDefinitionError::EmptyOrderKeys)
        ));
    }
}
