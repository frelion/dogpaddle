#![doc = include_str!("../README.md")]

mod codec;
mod definition;
pub mod operation;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
#[doc(hidden)]
pub use definition::{DataDeclaration, DataInstance, DataInstances, OperationBinding};
pub use definition::{
    MaterializeError, OperationBindError, OperationDefinition, OperationKind, OperationSchemaError,
};

#[cfg(test)]
mod tests;
