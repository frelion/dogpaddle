#![doc = include_str!("../README.md")]

mod codec;
mod count;
mod definition;
mod operation;
mod sequence_source;

pub use codec::DefinitionCodecError;
pub use count::{CountData, CountDefinition, CountError, CountOperation};
pub use definition::OperationDefinition;
pub use operation::Operation;
pub use sequence_source::{
    SequenceSourceData, SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
