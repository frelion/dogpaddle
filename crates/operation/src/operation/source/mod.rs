//! Source operations that produce records without consuming upstream input.

mod sequence_source;

pub use sequence_source::{
    SequenceSourceData, SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation,
};
pub(crate) use sequence_source::{TAG, decode_definition};
