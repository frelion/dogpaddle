//! Source operations that produce records without consuming upstream input.

pub(crate) mod sequence_source;

pub use sequence_source::{SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation};
