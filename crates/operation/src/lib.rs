#![doc = include_str!("../README.md")]

mod codec;
mod definition;
mod expression;
pub mod operation;
mod resource;
mod setup;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
#[doc(hidden)]
pub use definition::OperationBinding;
pub use definition::{
    OperationBindError, OperationDefinition, OperationKind, OperationSchemaError,
};
pub use expression::{
    Expr, ExpressionBindError, ExpressionDefinitionError, ExpressionError, Operator, ScalarValue,
    cast, col, ident, lit, try_cast,
};
pub use resource::RuntimeResource;
pub use setup::OperationSetupError;
#[doc(hidden)]
pub use setup::{create as create_operation, open as open_operation};

#[cfg(test)]
mod tests;
