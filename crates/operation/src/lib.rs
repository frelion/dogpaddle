#![doc = include_str!("../README.md")]

mod codec;
mod definition;
mod expression;
pub mod operation;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
#[doc(hidden)]
pub use definition::{DataDeclaration, DataInstance, DataInstances, OperationBinding};
pub use definition::{
    MaterializeError, OperationBindError, OperationDefinition, OperationKind, OperationSchemaError,
};
pub use expression::{
    BinaryOperator, Expression, ExpressionBindError, ExpressionError, Literal,
    MAX_EXPRESSION_STACK_DEPTH, UnaryOperator,
};

#[cfg(test)]
mod tests;
