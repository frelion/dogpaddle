#![doc = include_str!("../README.md")]

mod codec;
mod definition;
mod expression;
pub mod operation;
mod resource;

pub use codec::{
    DefinitionCodecError, decode_definition, decode_inline_definition, encode_definition,
    encode_inline_definition,
};
#[doc(hidden)]
pub use definition::{
    DataDeclaration, DataInstance, DataInstances, InlineBinding, OperationBinding,
};
pub use definition::{
    InlineBindError, InlineDefinition, InlineEligibilityError, InlineOperationDefinition,
    MaterializeError, OperationBindError, OperationDefinition, OperationKind, OperationSchemaError,
};
pub use expression::{
    Expr, ExpressionBindError, ExpressionDefinitionError, ExpressionError, Operator, ScalarValue,
    cast, col, ident, lit, try_cast,
};
pub use resource::RuntimeResource;

#[cfg(test)]
mod tests;
