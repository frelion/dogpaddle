use std::{fmt, sync::Arc};

use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray, UInt64Array};
use arrow_schema::{DataType, SchemaRef};
use thiserror::Error;

use crate::{DefinitionCodecError, codec::PayloadCursor};

const COLUMN_TAG: u8 = 0;
const BOOLEAN_LITERAL_TAG: u8 = 1;
const INT64_LITERAL_TAG: u8 = 2;
const UINT64_LITERAL_TAG: u8 = 3;
const UTF8_LITERAL_TAG: u8 = 4;
const UNARY_TAG: u8 = 5;
const BINARY_TAG: u8 = 6;

const NOT_TAG: u8 = 0;
const IS_NULL_TAG: u8 = 1;

const EQUAL_TAG: u8 = 0;
const NOT_EQUAL_TAG: u8 = 1;
const AND_TAG: u8 = 2;
const OR_TAG: u8 = 3;

/// Maximum number of expression values that may be live simultaneously.
///
/// The limit bounds vectorized evaluation memory without imposing an
/// arbitrary limit on total nodes or unary-chain depth.
pub const MAX_EXPRESSION_STACK_DEPTH: usize = 64;

/// One typed scalar literal in a persistent [`Expression`].
///
/// A `None` value is a null of the variant's explicit type. Expressions do not
/// have an untyped null literal or implicit casts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    /// A nullable Boolean literal.
    Boolean(Option<bool>),
    /// A nullable signed 64-bit integer literal.
    Int64(Option<i64>),
    /// A nullable unsigned 64-bit integer literal.
    UInt64(Option<u64>),
    /// A nullable UTF-8 literal.
    Utf8(Option<String>),
}

/// One unary operator in the stable expression language.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UnaryOperator {
    /// Boolean negation with null propagation.
    Not,
    /// Tests whether its operand is null and always returns a non-null Boolean.
    IsNull,
}

/// One binary operator in the stable expression language.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BinaryOperator {
    /// Equality with null propagation.
    Equal,
    /// Inequality with null propagation.
    NotEqual,
    /// SQL/Kleene three-valued Boolean conjunction.
    And,
    /// SQL/Kleene three-valued Boolean disjunction.
    Or,
}

/// A stable scalar expression over top-level logical record fields.
///
/// The public value is deliberately opaque. It stores a linear postfix
/// program rather than a recursive syntax tree, so persistent decoding,
/// binding, evaluation, and destruction do not recurse with expression depth.
/// Column references are stable zero-based top-level field indices and are
/// bound to one exact input Schema before an Operation is materialized.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Expression {
    nodes: Vec<Node>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Node {
    Column(u32),
    Literal(Literal),
    Unary(UnaryOperator),
    Binary(BinaryOperator),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValueType {
    data_type: DataType,
    nullable: bool,
}

#[derive(Clone, Copy)]
enum RuntimeScalar<'a> {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    UInt64(Option<u64>),
    Utf8(Option<&'a str>),
}

enum RuntimeValue<'a> {
    Scalar(RuntimeScalar<'a>, usize),
    Array(ArrayRef),
}

pub(crate) struct BoundExpression {
    input_schema: SchemaRef,
    nodes: Box<[BoundNode]>,
    output: ValueType,
}

enum BoundNode {
    Column(usize),
    Literal(Literal),
    Unary(UnaryOperator),
    Binary {
        operator: BinaryOperator,
        operand_type: DataType,
    },
}

/// Failure while binding an [`Expression`] to one exact logical input Schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExpressionBindError {
    /// A column index lies outside the bound input Schema.
    #[error("expression column {index} is outside a schema with {fields} fields")]
    ColumnOutOfBounds {
        /// Rejected zero-based field index.
        index: u32,
        /// Number of top-level fields in the input Schema.
        fields: usize,
    },
    /// A unary operator received a value of the wrong type.
    #[error("expression operator {operator} requires Boolean, found {actual}")]
    UnaryTypeMismatch {
        /// Operator being bound.
        operator: UnaryOperator,
        /// Actual operand type.
        actual: DataType,
    },
    /// A binary operator received operands of different types.
    #[error(
        "expression operator {operator} requires equal operand types, found {left} and {right}"
    )]
    BinaryTypeMismatch {
        /// Operator being bound.
        operator: BinaryOperator,
        /// Left operand type.
        left: DataType,
        /// Right operand type.
        right: DataType,
    },
    /// A binary operator does not support the otherwise matching operand type.
    #[error("expression operator {operator} does not support {data_type}")]
    UnsupportedOperand {
        /// Operator being bound.
        operator: BinaryOperator,
        /// Rejected operand type.
        data_type: DataType,
    },
    /// The postfix shape would retain too many live evaluation values.
    #[error("expression evaluation stack depth {depth} at node {node} exceeds the limit {maximum}")]
    EvaluationStackTooDeep {
        /// Zero-based node at which the limit was exceeded.
        node: usize,
        /// Required number of simultaneously live values.
        depth: usize,
        /// Stable evaluation-stack limit.
        maximum: usize,
    },
    /// The opaque postfix program violates its internal stack invariant.
    #[error("expression program is structurally invalid at node {node}")]
    InvalidProgram {
        /// Zero-based node at which validation failed.
        node: usize,
    },
}

/// Failure while evaluating an exact-Schema-bound expression.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExpressionError {
    /// Runtime input differs from the exact Schema used during binding.
    #[error("expression input schema differs from its bound schema")]
    SchemaMismatch,
    /// An Arrow array reports a supported type but is not its canonical array class.
    #[error("expression node {node} expected a canonical {expected} Arrow array")]
    ArrayTypeMismatch {
        /// Zero-based expression node reading the array.
        node: usize,
        /// Expected canonical Arrow data type.
        expected: DataType,
    },
    /// Repeating a UTF-8 literal would overflow Arrow's stable 32-bit offsets.
    #[error("expression node {node} exceeds the Utf8 offset range")]
    Utf8OffsetOverflow {
        /// Zero-based literal node that overflowed.
        node: usize,
    },
    /// The bound postfix program violates its internal stack invariant.
    #[error("bound expression program is structurally invalid at node {node}")]
    InvalidPlan {
        /// Zero-based node at which evaluation failed.
        node: usize,
    },
}

impl Expression {
    /// Creates a reference to one stable zero-based top-level input field.
    #[must_use]
    pub fn column(index: u32) -> Self {
        Self::from_nodes(vec![Node::Column(index)])
    }

    /// Creates one typed, possibly null, scalar literal.
    ///
    /// # Panics
    ///
    /// Panics when a UTF-8 value cannot be represented by Arrow's stable `Utf8`
    /// layout.
    #[must_use]
    pub fn literal(literal: Literal) -> Self {
        if let Literal::Utf8(Some(value)) = &literal {
            assert!(
                i32::try_from(value.len()).is_ok(),
                "Expression UTF-8 literal length must fit Arrow Utf8 offsets"
            );
        }
        Self::from_nodes(vec![Node::Literal(literal)])
    }

    /// Applies one unary operator to an expression.
    ///
    /// # Panics
    ///
    /// Panics only when the resulting program cannot fit the stable v1 `u32`
    /// node count.
    #[must_use]
    pub fn unary(operator: UnaryOperator, operand: Self) -> Self {
        let mut nodes = operand.nodes;
        nodes.push(Node::Unary(operator));
        Self::from_nodes(nodes)
    }

    /// Applies one binary operator to a left and right expression.
    ///
    /// # Panics
    ///
    /// Panics only when the resulting program cannot fit the stable v1 `u32`
    /// node count.
    #[must_use]
    pub fn binary(operator: BinaryOperator, left: Self, right: Self) -> Self {
        let mut nodes = left.nodes;
        nodes.extend(right.nodes);
        nodes.push(Node::Binary(operator));
        Self::from_nodes(nodes)
    }

    fn from_nodes(nodes: Vec<Node>) -> Self {
        assert!(!nodes.is_empty(), "Expression must contain one value");
        assert!(
            u32::try_from(nodes.len()).is_ok(),
            "Expression node count must fit the stable v1 format"
        );
        Self { nodes }
    }

    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        let count = u32::try_from(self.nodes.len())
            .expect("Expression construction enforces the stable v1 node count");
        output.extend_from_slice(&count.to_be_bytes());
        for node in &self.nodes {
            match node {
                Node::Column(index) => {
                    output.push(COLUMN_TAG);
                    output.extend_from_slice(&index.to_be_bytes());
                }
                Node::Literal(literal) => encode_literal(literal, output),
                Node::Unary(operator) => {
                    output.push(UNARY_TAG);
                    output.push(unary_tag(*operator));
                }
                Node::Binary(operator) => {
                    output.push(BINARY_TAG);
                    output.push(binary_tag(*operator));
                }
            }
        }
    }

    pub(crate) fn decode(cursor: &mut PayloadCursor<'_>) -> Result<Self, DefinitionCodecError> {
        let count = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("expression node count is invalid")
        })?;
        if count == 0 {
            return Err(DefinitionCodecError::InvalidPayload(
                "expression must contain at least one node",
            ));
        }
        // Every node consumes at least a tag and one payload byte. Validate
        // that lower bound before parsing, then grow only as nodes are actually
        // decoded so a forged count cannot trigger a large eager allocation.
        let minimum_encoded_length = count
            .checked_mul(2)
            .ok_or(DefinitionCodecError::Truncated)?;
        if minimum_encoded_length > cursor.remaining_len() {
            return Err(DefinitionCodecError::Truncated);
        }

        let mut nodes = Vec::new();
        let mut stack_depth = 0_usize;
        for _ in 0..count {
            let node = match cursor.read_u8()? {
                COLUMN_TAG => Node::Column(cursor.read_u32()?),
                BOOLEAN_LITERAL_TAG => Node::Literal(Literal::Boolean(decode_boolean(cursor)?)),
                INT64_LITERAL_TAG => Node::Literal(Literal::Int64(decode_option(
                    cursor,
                    PayloadCursor::read_i64,
                )?)),
                UINT64_LITERAL_TAG => Node::Literal(Literal::UInt64(decode_option(
                    cursor,
                    PayloadCursor::read_u64,
                )?)),
                UTF8_LITERAL_TAG => Node::Literal(Literal::Utf8(decode_utf8(cursor)?)),
                UNARY_TAG => Node::Unary(decode_unary(cursor.read_u8()?)?),
                BINARY_TAG => Node::Binary(decode_binary(cursor.read_u8()?)?),
                _ => {
                    return Err(DefinitionCodecError::InvalidPayload(
                        "expression node tag is unknown",
                    ));
                }
            };
            match node {
                Node::Column(_) | Node::Literal(_) => stack_depth += 1,
                Node::Unary(_) if stack_depth == 0 => {
                    return Err(DefinitionCodecError::InvalidPayload(
                        "expression unary node is missing its operand",
                    ));
                }
                Node::Unary(_) => {}
                Node::Binary(_) if stack_depth < 2 => {
                    return Err(DefinitionCodecError::InvalidPayload(
                        "expression binary node is missing an operand",
                    ));
                }
                Node::Binary(_) => stack_depth -= 1,
            }
            nodes.push(node);
        }
        if stack_depth != 1 {
            return Err(DefinitionCodecError::InvalidPayload(
                "expression does not produce exactly one value",
            ));
        }
        Ok(Self { nodes })
    }

    pub(crate) fn bind(
        &self,
        input_schema: SchemaRef,
    ) -> Result<BoundExpression, ExpressionBindError> {
        let mut types = Vec::new();
        let mut nodes = Vec::new();
        for (node_index, node) in self.nodes.iter().enumerate() {
            match node {
                Node::Column(encoded_index) => {
                    let index = usize::try_from(*encoded_index).map_err(|_| {
                        ExpressionBindError::ColumnOutOfBounds {
                            index: *encoded_index,
                            fields: input_schema.fields().len(),
                        }
                    })?;
                    let field = input_schema.fields().get(index).ok_or(
                        ExpressionBindError::ColumnOutOfBounds {
                            index: *encoded_index,
                            fields: input_schema.fields().len(),
                        },
                    )?;
                    types.push(ValueType {
                        data_type: field.data_type().clone(),
                        nullable: field.is_nullable() || field.data_type() == &DataType::Null,
                    });
                    nodes.push(BoundNode::Column(index));
                }
                Node::Literal(literal) => {
                    types.push(ValueType {
                        data_type: literal_data_type(literal),
                        nullable: literal_is_null(literal),
                    });
                    nodes.push(BoundNode::Literal(literal.clone()));
                }
                Node::Unary(operator) => {
                    let operand = types
                        .pop()
                        .ok_or(ExpressionBindError::InvalidProgram { node: node_index })?;
                    let output = bind_unary(*operator, operand)?;
                    types.push(output);
                    nodes.push(BoundNode::Unary(*operator));
                }
                Node::Binary(operator) => {
                    let right = types
                        .pop()
                        .ok_or(ExpressionBindError::InvalidProgram { node: node_index })?;
                    let left = types
                        .pop()
                        .ok_or(ExpressionBindError::InvalidProgram { node: node_index })?;
                    let operand_type = left.data_type.clone();
                    let output = bind_binary(*operator, left, right)?;
                    types.push(output);
                    nodes.push(BoundNode::Binary {
                        operator: *operator,
                        operand_type,
                    });
                }
            }
            if types.len() > MAX_EXPRESSION_STACK_DEPTH {
                return Err(ExpressionBindError::EvaluationStackTooDeep {
                    node: node_index,
                    depth: types.len(),
                    maximum: MAX_EXPRESSION_STACK_DEPTH,
                });
            }
        }
        let output = types.pop().ok_or(ExpressionBindError::InvalidProgram {
            node: self.nodes.len(),
        })?;
        if !types.is_empty() {
            return Err(ExpressionBindError::InvalidProgram {
                node: self.nodes.len(),
            });
        }
        Ok(BoundExpression {
            input_schema,
            nodes: nodes.into_boxed_slice(),
            output,
        })
    }
}

impl BoundExpression {
    pub(crate) const fn output_type(&self) -> &DataType {
        &self.output.data_type
    }

    pub(crate) const fn output_nullable(&self) -> bool {
        self.output.nullable
    }

    pub(crate) fn evaluate(
        &self,
        records: &arrow_array::RecordBatch,
    ) -> Result<ArrayRef, ExpressionError> {
        if records.schema_ref().as_ref() != self.input_schema.as_ref() {
            return Err(ExpressionError::SchemaMismatch);
        }
        let row_count = records.num_rows();
        let mut values = Vec::new();
        for (node_index, node) in self.nodes.iter().enumerate() {
            match node {
                BoundNode::Column(index) => {
                    values.push(RuntimeValue::Array(Arc::clone(records.column(*index))));
                }
                BoundNode::Literal(literal) => {
                    values.push(RuntimeValue::Scalar(
                        RuntimeScalar::from(literal),
                        node_index,
                    ));
                }
                BoundNode::Unary(operator) => {
                    let operand = values
                        .pop()
                        .ok_or(ExpressionError::InvalidPlan { node: node_index })?;
                    values.push(evaluate_unary(*operator, operand, node_index)?);
                }
                BoundNode::Binary {
                    operator,
                    operand_type,
                } => {
                    let right = values
                        .pop()
                        .ok_or(ExpressionError::InvalidPlan { node: node_index })?;
                    let left = values
                        .pop()
                        .ok_or(ExpressionError::InvalidPlan { node: node_index })?;
                    values.push(evaluate_binary(
                        *operator,
                        operand_type,
                        left,
                        right,
                        node_index,
                    )?);
                }
            }
        }
        let output = values.pop().ok_or(ExpressionError::InvalidPlan {
            node: self.nodes.len(),
        })?;
        let output = output.into_array(row_count)?;
        if !values.is_empty() || output.len() != row_count {
            return Err(ExpressionError::InvalidPlan {
                node: self.nodes.len(),
            });
        }
        Ok(output)
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Not => "Not",
            Self::IsNull => "IsNull",
        })
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Equal => "Equal",
            Self::NotEqual => "NotEqual",
            Self::And => "And",
            Self::Or => "Or",
        })
    }
}

fn bind_unary(
    operator: UnaryOperator,
    operand: ValueType,
) -> Result<ValueType, ExpressionBindError> {
    match operator {
        UnaryOperator::Not if operand.data_type != DataType::Boolean => {
            Err(ExpressionBindError::UnaryTypeMismatch {
                operator,
                actual: operand.data_type,
            })
        }
        UnaryOperator::Not => Ok(ValueType {
            data_type: DataType::Boolean,
            nullable: operand.nullable,
        }),
        UnaryOperator::IsNull => Ok(ValueType {
            data_type: DataType::Boolean,
            nullable: false,
        }),
    }
}

fn bind_binary(
    operator: BinaryOperator,
    left: ValueType,
    right: ValueType,
) -> Result<ValueType, ExpressionBindError> {
    if left.data_type != right.data_type {
        return Err(ExpressionBindError::BinaryTypeMismatch {
            operator,
            left: left.data_type,
            right: right.data_type,
        });
    }
    let supported = match operator {
        BinaryOperator::Equal | BinaryOperator::NotEqual => matches!(
            left.data_type,
            DataType::Boolean | DataType::Int64 | DataType::UInt64 | DataType::Utf8
        ),
        BinaryOperator::And | BinaryOperator::Or => left.data_type == DataType::Boolean,
    };
    if !supported {
        return Err(ExpressionBindError::UnsupportedOperand {
            operator,
            data_type: left.data_type,
        });
    }
    Ok(ValueType {
        data_type: DataType::Boolean,
        nullable: left.nullable || right.nullable,
    })
}

fn literal_data_type(literal: &Literal) -> DataType {
    match literal {
        Literal::Boolean(_) => DataType::Boolean,
        Literal::Int64(_) => DataType::Int64,
        Literal::UInt64(_) => DataType::UInt64,
        Literal::Utf8(_) => DataType::Utf8,
    }
}

const fn literal_is_null(literal: &Literal) -> bool {
    match literal {
        Literal::Boolean(value) => value.is_none(),
        Literal::Int64(value) => value.is_none(),
        Literal::UInt64(value) => value.is_none(),
        Literal::Utf8(value) => value.is_none(),
    }
}

impl<'a> From<&'a Literal> for RuntimeScalar<'a> {
    fn from(literal: &'a Literal) -> Self {
        match literal {
            Literal::Boolean(value) => Self::Boolean(*value),
            Literal::Int64(value) => Self::Int64(*value),
            Literal::UInt64(value) => Self::UInt64(*value),
            Literal::Utf8(value) => Self::Utf8(value.as_deref()),
        }
    }
}

impl RuntimeScalar<'_> {
    const fn is_null(self) -> bool {
        match self {
            Self::Boolean(value) => value.is_none(),
            Self::Int64(value) => value.is_none(),
            Self::UInt64(value) => value.is_none(),
            Self::Utf8(value) => value.is_none(),
        }
    }
}

impl RuntimeValue<'_> {
    fn into_array(self, row_count: usize) -> Result<ArrayRef, ExpressionError> {
        match self {
            Self::Array(array) => Ok(array),
            Self::Scalar(scalar, origin) => scalar_array(scalar, row_count, origin),
        }
    }
}

fn scalar_array(
    scalar: RuntimeScalar<'_>,
    row_count: usize,
    node: usize,
) -> Result<ArrayRef, ExpressionError> {
    let array: ArrayRef = match scalar {
        RuntimeScalar::Boolean(value) => Arc::new(BooleanArray::from(vec![value; row_count])),
        RuntimeScalar::Int64(value) => Arc::new(Int64Array::from(vec![value; row_count])),
        RuntimeScalar::UInt64(value) => Arc::new(UInt64Array::from(vec![value; row_count])),
        RuntimeScalar::Utf8(None) => Arc::new(StringArray::new_null(row_count)),
        RuntimeScalar::Utf8(Some(value)) => {
            let encoded_length = value
                .len()
                .checked_mul(row_count)
                .ok_or(ExpressionError::Utf8OffsetOverflow { node })?;
            if encoded_length > i32::MAX as usize {
                return Err(ExpressionError::Utf8OffsetOverflow { node });
            }
            Arc::new(StringArray::new_repeated(value, row_count))
        }
    };
    Ok(array)
}

fn evaluate_unary(
    operator: UnaryOperator,
    operand: RuntimeValue<'_>,
    node: usize,
) -> Result<RuntimeValue<'_>, ExpressionError> {
    match (operator, operand) {
        (UnaryOperator::Not, RuntimeValue::Scalar(RuntimeScalar::Boolean(value), _)) => Ok(
            RuntimeValue::Scalar(RuntimeScalar::Boolean(value.map(|value| !value)), node),
        ),
        (UnaryOperator::Not, RuntimeValue::Scalar(_, _)) => {
            Err(ExpressionError::InvalidPlan { node })
        }
        (UnaryOperator::Not, RuntimeValue::Array(operand)) => {
            let values = canonical_array::<BooleanArray>(&operand, node, DataType::Boolean)?;
            Ok(RuntimeValue::Array(Arc::new(BooleanArray::from(
                values
                    .iter()
                    .map(|value| value.map(|value| !value))
                    .collect::<Vec<_>>(),
            ))))
        }
        (UnaryOperator::IsNull, RuntimeValue::Scalar(operand, _)) => Ok(RuntimeValue::Scalar(
            RuntimeScalar::Boolean(Some(operand.is_null())),
            node,
        )),
        (UnaryOperator::IsNull, RuntimeValue::Array(operand)) => {
            let logical_nulls = operand.logical_nulls();
            Ok(RuntimeValue::Array(Arc::new(BooleanArray::from(
                (0..operand.len())
                    .map(|index| {
                        logical_nulls
                            .as_ref()
                            .is_some_and(|nulls| nulls.is_null(index))
                    })
                    .collect::<Vec<_>>(),
            ))))
        }
    }
}

fn evaluate_binary<'a>(
    operator: BinaryOperator,
    operand_type: &DataType,
    left: RuntimeValue<'a>,
    right: RuntimeValue<'a>,
    node: usize,
) -> Result<RuntimeValue<'a>, ExpressionError> {
    match (left, right) {
        (RuntimeValue::Scalar(left, _), RuntimeValue::Scalar(right, _)) => {
            evaluate_scalar_binary(operator, operand_type, left, right, node)
        }
        (RuntimeValue::Array(left), RuntimeValue::Array(right)) => {
            evaluate_array_binary(operator, operand_type, &left, &right, node)
                .map(RuntimeValue::Array)
        }
        (RuntimeValue::Array(array), RuntimeValue::Scalar(scalar, _)) => {
            evaluate_array_scalar_binary(operator, operand_type, &array, scalar, false, node)
                .map(RuntimeValue::Array)
        }
        (RuntimeValue::Scalar(scalar, _), RuntimeValue::Array(array)) => {
            evaluate_array_scalar_binary(operator, operand_type, &array, scalar, true, node)
                .map(RuntimeValue::Array)
        }
    }
}

fn evaluate_scalar_binary<'a>(
    operator: BinaryOperator,
    operand_type: &DataType,
    left: RuntimeScalar<'a>,
    right: RuntimeScalar<'a>,
    node: usize,
) -> Result<RuntimeValue<'a>, ExpressionError> {
    let value = match operator {
        BinaryOperator::And | BinaryOperator::Or => {
            let (RuntimeScalar::Boolean(left), RuntimeScalar::Boolean(right)) = (left, right)
            else {
                return Err(ExpressionError::InvalidPlan { node });
            };
            evaluate_boolean(operator, left, right, node)?
        }
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            let equal = match (operand_type, left, right) {
                (
                    DataType::Boolean,
                    RuntimeScalar::Boolean(left),
                    RuntimeScalar::Boolean(right),
                ) => nullable_equal(left, right),
                (DataType::Int64, RuntimeScalar::Int64(left), RuntimeScalar::Int64(right)) => {
                    nullable_equal(left, right)
                }
                (DataType::UInt64, RuntimeScalar::UInt64(left), RuntimeScalar::UInt64(right)) => {
                    nullable_equal(left, right)
                }
                (DataType::Utf8, RuntimeScalar::Utf8(left), RuntimeScalar::Utf8(right)) => {
                    nullable_equal(left, right)
                }
                _ => return Err(ExpressionError::InvalidPlan { node }),
            };
            comparison_result(operator, equal, node)?
        }
    };
    Ok(RuntimeValue::Scalar(RuntimeScalar::Boolean(value), node))
}

fn evaluate_array_scalar_binary(
    operator: BinaryOperator,
    operand_type: &DataType,
    array: &ArrayRef,
    scalar: RuntimeScalar<'_>,
    scalar_on_left: bool,
    node: usize,
) -> Result<ArrayRef, ExpressionError> {
    let values = match operator {
        BinaryOperator::And | BinaryOperator::Or => {
            let RuntimeScalar::Boolean(scalar) = scalar else {
                return Err(ExpressionError::InvalidPlan { node });
            };
            canonical_array::<BooleanArray>(array, node, DataType::Boolean)?
                .iter()
                .map(|value| {
                    let (left, right) = if scalar_on_left {
                        (scalar, value)
                    } else {
                        (value, scalar)
                    };
                    evaluate_boolean(operator, left, right, node)
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            let equal = match (operand_type, scalar) {
                (DataType::Boolean, RuntimeScalar::Boolean(scalar)) => compare_scalar(
                    canonical_array::<BooleanArray>(array, node, DataType::Boolean)?.iter(),
                    scalar,
                ),
                (DataType::Int64, RuntimeScalar::Int64(scalar)) => compare_scalar(
                    canonical_array::<Int64Array>(array, node, DataType::Int64)?.iter(),
                    scalar,
                ),
                (DataType::UInt64, RuntimeScalar::UInt64(scalar)) => compare_scalar(
                    canonical_array::<UInt64Array>(array, node, DataType::UInt64)?.iter(),
                    scalar,
                ),
                (DataType::Utf8, RuntimeScalar::Utf8(scalar)) => compare_scalar(
                    canonical_array::<StringArray>(array, node, DataType::Utf8)?.iter(),
                    scalar,
                ),
                _ => return Err(ExpressionError::InvalidPlan { node }),
            };
            equal
                .into_iter()
                .map(|equal| comparison_result(operator, equal, node))
                .collect::<Result<Vec<_>, _>>()?
        }
    };
    Ok(Arc::new(BooleanArray::from(values)))
}

fn evaluate_array_binary(
    operator: BinaryOperator,
    operand_type: &DataType,
    left: &ArrayRef,
    right: &ArrayRef,
    node: usize,
) -> Result<ArrayRef, ExpressionError> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        let left = canonical_array::<BooleanArray>(left, node, DataType::Boolean)?;
        let right = canonical_array::<BooleanArray>(right, node, DataType::Boolean)?;
        let values = left
            .iter()
            .zip(right.iter())
            .map(|(left, right)| evaluate_boolean(operator, left, right, node))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Arc::new(BooleanArray::from(values)));
    }

    let equal = match operand_type {
        DataType::Boolean => compare(
            canonical_array::<BooleanArray>(left, node, DataType::Boolean)?.iter(),
            canonical_array::<BooleanArray>(right, node, DataType::Boolean)?.iter(),
        ),
        DataType::Int64 => compare(
            canonical_array::<Int64Array>(left, node, DataType::Int64)?.iter(),
            canonical_array::<Int64Array>(right, node, DataType::Int64)?.iter(),
        ),
        DataType::UInt64 => compare(
            canonical_array::<UInt64Array>(left, node, DataType::UInt64)?.iter(),
            canonical_array::<UInt64Array>(right, node, DataType::UInt64)?.iter(),
        ),
        DataType::Utf8 => compare(
            canonical_array::<StringArray>(left, node, DataType::Utf8)?.iter(),
            canonical_array::<StringArray>(right, node, DataType::Utf8)?.iter(),
        ),
        _ => return Err(ExpressionError::InvalidPlan { node }),
    };
    let values = equal
        .into_iter()
        .map(|equal| comparison_result(operator, equal, node))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(BooleanArray::from(values)))
}

fn canonical_array<A: Array + 'static>(
    array: &ArrayRef,
    node: usize,
    expected: DataType,
) -> Result<&A, ExpressionError> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or(ExpressionError::ArrayTypeMismatch { node, expected })
}

fn compare<'a, T: PartialEq + 'a>(
    left: impl Iterator<Item = Option<T>> + 'a,
    right: impl Iterator<Item = Option<T>> + 'a,
) -> Vec<Option<bool>> {
    left.zip(right)
        .map(|(left, right)| nullable_equal(left, right))
        .collect()
}

fn compare_scalar<'a, T: Copy + PartialEq + 'a>(
    values: impl Iterator<Item = Option<T>> + 'a,
    scalar: Option<T>,
) -> Vec<Option<bool>> {
    values.map(|value| nullable_equal(value, scalar)).collect()
}

fn nullable_equal<T: PartialEq>(left: Option<T>, right: Option<T>) -> Option<bool> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left == right),
        (None, _) | (_, None) => None,
    }
}

fn comparison_result(
    operator: BinaryOperator,
    equal: Option<bool>,
    node: usize,
) -> Result<Option<bool>, ExpressionError> {
    match operator {
        BinaryOperator::Equal => Ok(equal),
        BinaryOperator::NotEqual => Ok(equal.map(|equal| !equal)),
        BinaryOperator::And | BinaryOperator::Or => Err(ExpressionError::InvalidPlan { node }),
    }
}

fn evaluate_boolean(
    operator: BinaryOperator,
    left: Option<bool>,
    right: Option<bool>,
    node: usize,
) -> Result<Option<bool>, ExpressionError> {
    match operator {
        BinaryOperator::And => Ok(kleene_and(left, right)),
        BinaryOperator::Or => Ok(kleene_or(left, right)),
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            Err(ExpressionError::InvalidPlan { node })
        }
    }
}

const fn kleene_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

const fn kleene_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn encode_literal(literal: &Literal, output: &mut Vec<u8>) {
    match literal {
        Literal::Boolean(value) => {
            output.push(BOOLEAN_LITERAL_TAG);
            encode_option(*value, output, |value, output| output.push(u8::from(value)));
        }
        Literal::Int64(value) => {
            output.push(INT64_LITERAL_TAG);
            encode_option(*value, output, |value, output| {
                output.extend_from_slice(&value.to_be_bytes());
            });
        }
        Literal::UInt64(value) => {
            output.push(UINT64_LITERAL_TAG);
            encode_option(*value, output, |value, output| {
                output.extend_from_slice(&value.to_be_bytes());
            });
        }
        Literal::Utf8(value) => {
            output.push(UTF8_LITERAL_TAG);
            encode_option(value.as_deref(), output, |value, output| {
                let length = u32::try_from(value.len())
                    .expect("Expression::literal validated the stable UTF-8 length");
                output.extend_from_slice(&length.to_be_bytes());
                output.extend_from_slice(value.as_bytes());
            });
        }
    }
}

fn encode_option<T>(value: Option<T>, output: &mut Vec<u8>, encode: impl FnOnce(T, &mut Vec<u8>)) {
    match value {
        Some(value) => {
            output.push(1);
            encode(value, output);
        }
        None => output.push(0),
    }
}

fn decode_option<'a, T>(
    cursor: &mut PayloadCursor<'a>,
    decode: impl FnOnce(&mut PayloadCursor<'a>) -> Result<T, DefinitionCodecError>,
) -> Result<Option<T>, DefinitionCodecError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => decode(cursor).map(Some),
        _ => Err(DefinitionCodecError::InvalidPayload(
            "expression literal presence marker is not canonical",
        )),
    }
}

fn decode_boolean(cursor: &mut PayloadCursor<'_>) -> Result<Option<bool>, DefinitionCodecError> {
    decode_option(cursor, |cursor| match cursor.read_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DefinitionCodecError::InvalidPayload(
            "expression Boolean literal is not canonical",
        )),
    })
}

fn decode_utf8(cursor: &mut PayloadCursor<'_>) -> Result<Option<String>, DefinitionCodecError> {
    decode_option(cursor, |cursor| {
        let length = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("expression UTF-8 length is invalid")
        })?;
        if i32::try_from(length).is_err() {
            return Err(DefinitionCodecError::InvalidPayload(
                "expression UTF-8 literal exceeds Arrow Utf8 offsets",
            ));
        }
        let bytes = cursor.read_bytes(length)?;
        let value = std::str::from_utf8(bytes).map_err(|_| {
            DefinitionCodecError::InvalidPayload("expression UTF-8 literal is invalid")
        })?;
        Ok(value.to_owned())
    })
}

const fn unary_tag(operator: UnaryOperator) -> u8 {
    match operator {
        UnaryOperator::Not => NOT_TAG,
        UnaryOperator::IsNull => IS_NULL_TAG,
    }
}

fn decode_unary(tag: u8) -> Result<UnaryOperator, DefinitionCodecError> {
    match tag {
        NOT_TAG => Ok(UnaryOperator::Not),
        IS_NULL_TAG => Ok(UnaryOperator::IsNull),
        _ => Err(DefinitionCodecError::InvalidPayload(
            "expression unary operator tag is unknown",
        )),
    }
}

const fn binary_tag(operator: BinaryOperator) -> u8 {
    match operator {
        BinaryOperator::Equal => EQUAL_TAG,
        BinaryOperator::NotEqual => NOT_EQUAL_TAG,
        BinaryOperator::And => AND_TAG,
        BinaryOperator::Or => OR_TAG,
    }
}

fn decode_binary(tag: u8) -> Result<BinaryOperator, DefinitionCodecError> {
    match tag {
        EQUAL_TAG => Ok(BinaryOperator::Equal),
        NOT_EQUAL_TAG => Ok(BinaryOperator::NotEqual),
        AND_TAG => Ok(BinaryOperator::And),
        OR_TAG => Ok(BinaryOperator::Or),
        _ => Err(DefinitionCodecError::InvalidPayload(
            "expression binary operator tag is unknown",
        )),
    }
}
