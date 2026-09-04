#![doc = include_str!("../README.md")]

mod codec;
mod definition;
mod expression;
pub mod operation;
mod resource;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
#[doc(hidden)]
pub use definition::{DataDeclaration, DataInstance, DataInstances, OperationBinding};
pub use definition::{
    MaterializeError, OperationBindError, OperationDefinition, OperationKind, OperationSchemaError,
};
pub use expression::{
    Expr, ExpressionBindError, ExpressionDefinitionError, ExpressionError, Operator, ScalarValue,
    cast, col, ident, lit, try_cast,
};
pub use resource::RuntimeResource;

#[cfg(test)]
mod tests;
