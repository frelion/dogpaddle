#![doc = include_str!("../README.md")]

mod codec;
mod definition;
pub mod operation;

pub use codec::{DefinitionCodecError, decode_definition, encode_definition};
pub use definition::{DataBindings, MaterializeError, OperationDefinition};

#[cfg(test)]
mod tests;
