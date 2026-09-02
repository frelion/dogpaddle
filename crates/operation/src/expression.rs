//! `DataFusion` scalar expressions persisted with `DataFusion`'s protobuf codec.
//!
//! `DogPaddle` owns only the outer Operation Definition version and the exact
//! Schema binding boundary. Expression syntax, protobuf conversion, physical
//! planning, type derivation, nullability, and evaluation belong to
//! `DataFusion`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::{
    execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext,
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
}

impl StoredExpression {
    pub(crate) fn try_new(expression: Expr) -> Result<Self, ExpressionDefinitionError> {
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
        })
    }
}

impl BoundExpression {
    pub(crate) const fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub(crate) const fn output_nullable(&self) -> bool {
        self.output_nullable
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
