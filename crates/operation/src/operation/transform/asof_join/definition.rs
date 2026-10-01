use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::ScalarValue;
use serde::{Deserialize, Serialize};

use super::{
    AsOfDirection, AsOfJoinDefinitionError, AsOfJoinSchemaError,
    runtime::{BoundPair, BoundScalar},
};
use crate::{
    Expr, OperationSchemaError,
    definition::{ConstructedOperation, schema_error},
    expression::StoredExpression,
    operation::relation::indexable,
};

pub(super) const LEFT_ROWS: &str = "asof_join.left_index";
pub(super) const RIGHT_ROWS: &str = "asof_join.right_index";

pub(crate) struct AsOfJoinLayout {
    pub(super) direction: AsOfDirection,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) output_schema: SchemaRef,
    pub(super) equalities: Box<[BoundPair]>,
    pub(super) order: BoundPair,
    pub(super) right_nulls: Vec<ScalarValue>,
}

/// One SQL equality expression pair. NULL keys never match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsOfEqualityKey {
    left: Expr,
    right: Expr,
}
impl AsOfEqualityKey {
    /// Creates a left/right equality pair.
    #[must_use]
    pub fn new(left: Expr, right: Expr) -> Self {
        Self { left, right }
    }
    /// Returns the left expression.
    #[must_use]
    pub const fn left(&self) -> &Expr {
        &self.left
    }
    /// Returns the right expression.
    #[must_use]
    pub const fn right(&self) -> &Expr {
        &self.right
    }
}

/// The single ordered expression pair used by SQL ASOF.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsOfOrderKey {
    left: Expr,
    right: Expr,
}
impl AsOfOrderKey {
    /// Creates the left/right order pair.
    #[must_use]
    pub fn new(left: Expr, right: Expr) -> Self {
        Self { left, right }
    }
    /// Returns the left expression.
    #[must_use]
    pub const fn left(&self) -> &Expr {
        &self.left
    }
    /// Returns the right expression.
    #[must_use]
    pub const fn right(&self) -> &Expr {
        &self.right
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredPair {
    left: StoredExpression,
    right: StoredExpression,
}

/// SQL ASOF left outer join with one order key and rejected ambiguous winners.
///
/// Equality uses SQL NULL semantics. Repeated copies of one exact right row
/// remain one candidate; distinct rows at the selected time are rejected.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AsOfJoinDefinition {
    direction: AsOfDirection,
    equalities: Box<[StoredPair]>,
    order: StoredPair,
    output_names: Box<[String]>,
}
impl AsOfJoinDefinition {
    /// Reports whether this type has a stable ordered index encoding.
    #[must_use]
    pub fn supports_index_type(data_type: &DataType) -> bool {
        indexable(data_type)
    }
    /// Creates a persistent SQL ASOF left outer join.
    /// # Errors
    /// Returns an error for oversized definitions or non-replayable expressions.
    pub fn try_new<E, N, S>(
        direction: AsOfDirection,
        equalities: E,
        order: AsOfOrderKey,
        output_names: N,
    ) -> Result<Self, AsOfJoinDefinitionError>
    where
        E: IntoIterator<Item = AsOfEqualityKey>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let equalities = equalities
            .into_iter()
            .enumerate()
            .map(|(index, pair)| store_pair(pair.left, pair.right, "equality", index))
            .collect::<Result<Vec<_>, _>>()?;
        let order = store_pair(order.left, order.right, "order", 0)?;
        let output_names = output_names.into_iter().map(Into::into).collect::<Vec<_>>();
        let definition = Self {
            direction,
            equalities: equalities.into_boxed_slice(),
            order,
            output_names: output_names.into_boxed_slice(),
        };
        definition.validate()?;
        Ok(definition)
    }

    fn validate(&self) -> Result<(), AsOfJoinDefinitionError> {
        if self.equalities.len() > 1024
            || self.output_names.len() > 4096
            || self.output_names.iter().any(|name| name.len() > 65536)
        {
            return Err(AsOfJoinDefinitionError::TooMany {
                kind: "definition fields",
            });
        }
        Ok(())
    }

    /// Returns the ordered search direction and exactness.
    #[must_use]
    pub const fn direction(&self) -> AsOfDirection {
        self.direction
    }
    /// Returns SQL equality expression pairs.
    #[must_use]
    pub fn equality_keys(&self) -> impl ExactSizeIterator<Item = (&Expr, &Expr)> {
        self.equalities
            .iter()
            .map(|pair| (pair.left.expression(), pair.right.expression()))
    }
    /// Returns the single ordered expression pair.
    #[must_use]
    pub fn order_key(&self) -> (&Expr, &Expr) {
        (self.order.left.expression(), self.order.right.expression())
    }
    /// Returns left-then-right physical output names.
    pub fn output_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.output_names.iter().map(String::as_str)
    }
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, OperationSchemaError> {
        self.compile(&inputs[0], &inputs[1])
            .map(|layout| Some(layout.output_schema))
    }
    pub(crate) fn construct_unchecked(
        &self,
        inputs: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let layout = self.compile(&inputs[0], &inputs[1]).map_err(schema_error)?;
        let schema = Arc::clone(&layout.output_schema);
        Ok(ConstructedOperation::new(
            super::construct(layout, data)?,
            Some(schema),
        ))
    }
    fn compile(
        &self,
        left: &SchemaRef,
        right: &SchemaRef,
    ) -> Result<AsOfJoinLayout, OperationSchemaError> {
        self.validate()?;
        let expected = left.fields().len() + right.fields().len();
        if expected != self.output_names.len() {
            return Err(Box::new(AsOfJoinSchemaError::OutputNameCount {
                expected,
                actual: self.output_names.len(),
            }));
        }
        let equalities = self
            .equalities
            .iter()
            .enumerate()
            .map(|(index, pair)| bind_pair(pair, left, right, "equality", index))
            .collect::<Result<Vec<_>, _>>()?;
        let order = bind_pair(&self.order, left, right, "order", 0)?;
        let mut fields = Vec::with_capacity(expected);
        for field in left.fields() {
            fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(&self.output_names[fields.len()]),
            ));
        }
        let mut right_nulls = Vec::with_capacity(right.fields().len());
        for field in right.fields() {
            right_nulls.push(
                ScalarValue::try_from(field.data_type())
                    .map_err(AsOfJoinSchemaError::NullPadding)?,
            );
            fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(&self.output_names[fields.len()])
                    .with_nullable(true),
            ));
        }
        let output_schema = Arc::new(Schema::new(fields));
        dogpaddle_change::validate_schema(&output_schema)?;
        Ok(AsOfJoinLayout {
            direction: self.direction,
            input_schemas: [Arc::clone(left), Arc::clone(right)],
            output_schema,
            equalities: equalities.into_boxed_slice(),
            order,
            right_nulls,
        })
    }
}
fn store_pair(
    left: Expr,
    right: Expr,
    role: &'static str,
    index: usize,
) -> Result<StoredPair, AsOfJoinDefinitionError> {
    let encode = |value| {
        StoredExpression::try_new(value).map_err(|source| AsOfJoinDefinitionError::Expression {
            role,
            index,
            source,
        })
    };
    Ok(StoredPair {
        left: encode(left)?,
        right: encode(right)?,
    })
}
fn bind_pair(
    pair: &StoredPair,
    left: &SchemaRef,
    right: &SchemaRef,
    role: &'static str,
    index: usize,
) -> Result<BoundPair, OperationSchemaError> {
    let bind = |stored: &StoredExpression,
                schema: &SchemaRef,
                side|
     -> Result<BoundScalar, OperationSchemaError> {
        let expression = stored.bind(Arc::clone(schema)).map_err(|source| {
            Box::new(AsOfJoinSchemaError::Expression {
                role,
                index,
                side,
                source,
            }) as OperationSchemaError
        })?;
        let data_type = expression.output_type();
        if !indexable(data_type) {
            return Err(Box::new(AsOfJoinSchemaError::UnsupportedType {
                role,
                index,
                data_type: data_type.clone(),
            }));
        }
        let field = Arc::new(Field::new(
            role,
            data_type.clone(),
            expression.output_nullable(),
        ));
        Ok(BoundScalar { expression, field })
    };
    let left = bind(&pair.left, left, "left")?;
    let right = bind(&pair.right, right, "right")?;
    if left.field.data_type() != right.field.data_type() {
        return Err(Box::new(AsOfJoinSchemaError::TypeMismatch {
            role,
            index,
            left: left.field.data_type().clone(),
            right: right.field.data_type().clone(),
        }));
    }
    Ok(BoundPair { left, right })
}
