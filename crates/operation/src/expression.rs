//! `DataFusion` scalar expressions persisted with `DataFusion`'s protobuf codec.
//!
//! `DogPaddle` owns only the outer Operation Definition version and the exact
//! Schema binding boundary. Expression syntax, protobuf conversion, physical
//! planning, type derivation, nullability, and evaluation belong to
//! `DataFusion`.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{
    DFSchema, DataFusionError,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::{
    ExprSchemable, Volatility, execution_props::ExecutionProps,
    physical_planning_context::PhysicalPlanningContext,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_proto::bytes::Serializeable;
use thiserror::Error;

use crate::{DefinitionCodecError, codec::PayloadCursor};

pub use datafusion_common::ScalarValue;
pub use datafusion_expr::{Expr, Operator, cast, col, ident, lit, try_cast};

/// Failure while making a `DataFusion` [`Expr`] persistable.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExpressionDefinitionError {
    /// `DataFusion` could not serialize or deserialize the expression.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    /// `DataFusion`'s protobuf conversion did not preserve the logical expression.
    #[error("DataFusion protobuf does not round-trip this expression exactly")]
    NonRoundTrip,
    /// `DataFusion` did not produce one canonical protobuf representation.
    #[error("DataFusion protobuf encoding is not canonical for this expression")]
    NonCanonical,
    /// The protobuf cannot fit the Operation Definition length field.
    #[error("DataFusion expression protobuf is too large for an Operation Definition")]
    TooLarge,
}

/// Failure while binding a persisted expression to one exact input Schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExpressionBindError {
    /// `DataFusion` could not plan the expression against the supplied Schema.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

/// Failure while evaluating an exact-Schema-bound expression.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExpressionError {
    /// Runtime input differs from the exact Schema used during binding.
    #[error("expression input schema differs from its bound schema")]
    SchemaMismatch,
    /// `DataFusion` could not evaluate or materialize the expression result.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredExpression {
    expression: Arc<Expr>,
    protobuf: Arc<[u8]>,
}

pub(crate) struct BoundExpression {
    input_schema: SchemaRef,
    physical: Arc<dyn PhysicalExpr>,
    output_type: DataType,
    output_nullable: bool,
    output_metadata: HashMap<String, String>,
}

impl StoredExpression {
    pub(crate) fn try_new(expression: Expr) -> Result<Self, ExpressionDefinitionError> {
        if has_nondeterministic_protobuf_map(&expression) {
            return Err(ExpressionDefinitionError::NonCanonical);
        }
        let protobuf = expression.to_bytes()?;
        if u32::try_from(protobuf.len()).is_err() {
            return Err(ExpressionDefinitionError::TooLarge);
        }

        let decoded = Expr::from_bytes(protobuf.as_ref())?;
        if decoded != expression {
            return Err(ExpressionDefinitionError::NonRoundTrip);
        }
        let canonical = decoded.to_bytes()?;
        if canonical != protobuf {
            return Err(ExpressionDefinitionError::NonCanonical);
        }

        Ok(Self {
            expression: Arc::new(expression),
            protobuf: Arc::from(canonical.as_ref()),
        })
    }

    pub(crate) fn expression(&self) -> &Expr {
        self.expression.as_ref()
    }

    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        let length = u32::try_from(self.protobuf.len())
            .expect("expression construction enforces the protobuf length");
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&self.protobuf);
    }

    pub(crate) fn decode(cursor: &mut PayloadCursor<'_>) -> Result<Self, DefinitionCodecError> {
        let length = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("DataFusion expression protobuf length is invalid")
        })?;
        let protobuf = cursor.read_bytes(length)?;
        let expression = Expr::from_bytes(protobuf).map_err(|_| {
            DefinitionCodecError::InvalidPayload("DataFusion expression protobuf is invalid")
        })?;
        if has_nondeterministic_protobuf_map(&expression) {
            return Err(DefinitionCodecError::InvalidPayload(
                "DataFusion expression protobuf contains non-canonical map metadata",
            ));
        }
        let canonical = expression.to_bytes().map_err(|_| {
            DefinitionCodecError::InvalidPayload("DataFusion expression cannot be re-encoded")
        })?;
        if canonical.as_ref() != protobuf {
            return Err(DefinitionCodecError::InvalidPayload(
                "DataFusion expression protobuf is not canonical",
            ));
        }

        Ok(Self {
            expression: Arc::new(expression),
            protobuf: Arc::from(protobuf),
        })
    }

    pub(crate) fn bind(
        &self,
        input_schema: SchemaRef,
    ) -> Result<BoundExpression, ExpressionBindError> {
        let datafusion_schema = DFSchema::try_from(Arc::clone(&input_schema))?;
        let output_metadata = self
            .expression()
            .to_field(&datafusion_schema)?
            .1
            .metadata()
            .clone();
        let physical = create_physical_expr(
            self.expression(),
            &datafusion_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )?;
        let output_type = physical.data_type(input_schema.as_ref())?;
        let output_nullable = physical.nullable(input_schema.as_ref())?;

        Ok(BoundExpression {
            input_schema,
            physical,
            output_type,
            output_nullable,
            output_metadata,
        })
    }

    pub(crate) fn is_atomic(&self) -> bool {
        let mut eligible = true;
        let _ = self.expression().apply(|expression| {
            eligible = atomic_expression_supported(expression);
            Ok::<_, DataFusionError>(if eligible {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            })
        });
        eligible
    }
}

fn atomic_expression_supported(expression: &Expr) -> bool {
    if let Expr::ScalarFunction(function) = expression {
        return function.func.signature().volatility == Volatility::Immutable;
    }
    !unsupported_atomic_expression(expression)
}

#[expect(deprecated)]
fn unsupported_atomic_expression(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::ScalarVariable(_, _)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Unnest(_)
            | Expr::HigherOrderFunction(_)
            | Expr::Lambda(_)
            | Expr::LambdaVariable(_)
    )
}

// prost encodes `HashMap` fields in per-process hash iteration order. DataFusion
// uses those fields for expression Field metadata and for nested Arrow Fields.
// Keeping map-bearing expressions out of v1 preserves byte-stable Definition
// encoding without adding a second expression format or partially reimplementing
// DataFusion's protobuf schema.
fn has_nondeterministic_protobuf_map(expression: &Expr) -> bool {
    let mut found = false;
    let _ = expression.apply(|expression| {
        found = match expression {
            Expr::Alias(alias) => alias
                .metadata
                .as_ref()
                .is_some_and(|metadata| !metadata.is_empty()),
            Expr::ScalarVariable(field, _) | Expr::OuterReferenceColumn(field, _) => {
                field_has_metadata(field)
            }
            Expr::Literal(value, metadata) => {
                metadata
                    .as_ref()
                    .is_some_and(|metadata| !metadata.is_empty())
                    || data_type_has_metadata(&value.data_type())
            }
            Expr::Cast(cast) => field_has_metadata(&cast.field),
            Expr::TryCast(cast) => field_has_metadata(&cast.field),
            Expr::Placeholder(placeholder) => {
                placeholder.field.as_ref().is_some_and(field_has_metadata)
            }
            Expr::LambdaVariable(variable) => {
                variable.field.as_ref().is_some_and(field_has_metadata)
            }
            // These embed a LogicalPlan protobuf. Its schemas and provider
            // options contain additional protobuf map fields outside the
            // scalar-expression tree that DogPaddle binds.
            Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::ScalarSubquery(_) => true,
            _ => false,
        };
        Ok::<_, DataFusionError>(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

fn field_has_metadata(field: &arrow_schema::FieldRef) -> bool {
    !field.metadata().is_empty() || data_type_has_metadata(field.data_type())
}

fn data_type_has_metadata(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => field_has_metadata(field),
        DataType::Struct(fields) => fields.iter().any(field_has_metadata),
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| field_has_metadata(field)),
        DataType::Dictionary(key, value) => {
            data_type_has_metadata(key) || data_type_has_metadata(value)
        }
        DataType::RunEndEncoded(run_ends, values) => {
            field_has_metadata(run_ends) || field_has_metadata(values)
        }
        _ => false,
    }
}

impl BoundExpression {
    pub(crate) const fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub(crate) const fn output_nullable(&self) -> bool {
        self.output_nullable
    }

    pub(crate) const fn output_metadata(&self) -> &HashMap<String, String> {
        &self.output_metadata
    }

    pub(crate) fn evaluate(&self, records: &RecordBatch) -> Result<ArrayRef, ExpressionError> {
        if records.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ExpressionError::SchemaMismatch);
        }
        self.physical
            .evaluate(records)?
            .into_array_of_size(records.num_rows())
            .map_err(ExpressionError::DataFusion)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::DataType;
    use datafusion_expr::{Volatility, create_udf};

    use super::{atomic_expression_supported, col};

    fn function_expression(name: &str, volatility: Volatility) -> super::Expr {
        create_udf(
            name,
            vec![DataType::UInt64],
            DataType::UInt64,
            volatility,
            Arc::new(|arguments| Ok(arguments[0].clone())),
        )
        .call(vec![col("value")])
    }

    #[test]
    fn atomic_expression_requires_immutable_scalar_functions() {
        assert!(!atomic_expression_supported(&function_expression(
            "stable_identity",
            Volatility::Stable
        )));
        assert!(!atomic_expression_supported(&function_expression(
            "volatile_identity",
            Volatility::Volatile
        )));
        assert!(atomic_expression_supported(&function_expression(
            "immutable_identity",
            Volatility::Immutable
        )));
    }
}
