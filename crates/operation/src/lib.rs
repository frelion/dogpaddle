#![doc = include_str!("../README.md")]

mod codec;
mod definition;
mod expression;
pub mod operation;
mod resource;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
pub use definition::{
    ConstructedOperation, OperationBindError, OperationDefinition, OperationKind,
    OperationSchemaError, OperationSetupError,
};
pub use expression::{
    Expr, ExpressionBindError, ExpressionDefinitionError, ExpressionError, Operator, ScalarValue,
    cast, col, ident, lit, try_cast,
};
pub use resource::RuntimeResource;

#[cfg(test)]
mod tests;
