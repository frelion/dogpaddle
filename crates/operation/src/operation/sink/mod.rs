//! Sink operations that consume records without producing downstream output.

pub(crate) mod discard;

pub use discard::{DiscardDefinition, DiscardError, DiscardOperation};
