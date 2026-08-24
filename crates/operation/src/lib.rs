#![doc = include_str!("../README.md")]

mod codec;
mod count;
mod definition;
mod sequence;

pub use codec::DefinitionCodecError;
pub use count::{CountData, CountDefinition, CountError, CountOperation};
pub use definition::OperationDefinition;
pub use sequence::{
    SequenceSourceData, SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
