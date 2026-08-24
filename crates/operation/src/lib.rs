#![doc = include_str!("../README.md")]

mod codec;
mod count;
mod definition;
mod sequence_source;

pub use codec::DefinitionCodecError;
pub use count::{CountData, CountDefinition, CountError, CountOperation};
pub use definition::OperationDefinition;
pub use sequence_source::{
    SequenceSourceData, SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
