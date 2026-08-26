#![doc = include_str!("../README.md")]

mod codec;
mod definition;
pub mod operation;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
#[doc(hidden)]
pub use definition::{DataDeclaration, DataInstance, DataInstances};
pub use definition::{MaterializeError, OperationDefinition};

#[cfg(test)]
mod tests;
