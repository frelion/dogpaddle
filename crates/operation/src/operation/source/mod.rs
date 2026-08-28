//! Source operations that produce records without consuming upstream input.

pub(crate) mod sequence;

pub use sequence::{SequenceSourceDefinition, SequenceSourceError, SequenceSourceOperation};
