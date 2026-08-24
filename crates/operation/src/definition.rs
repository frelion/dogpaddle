use crate::{CountDefinition, SequenceSourceDefinition};

/// Closed, strongly typed union of operation definitions supported by `DogPaddle`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationDefinition {
    /// Monotonically increasing zero-input source.
    SequenceSource(SequenceSourceDefinition),
    /// Running count operation.
    Count(CountDefinition),
}

impl OperationDefinition {
    /// Returns the exact number of ordered upstream stages this definition accepts.
    #[must_use]
    pub const fn input_count(&self) -> usize {
        match self {
            Self::SequenceSource(_) => SequenceSourceDefinition::INPUT_COUNT,
            Self::Count(_) => CountDefinition::INPUT_COUNT,
        }
    }
}

impl From<SequenceSourceDefinition> for OperationDefinition {
    fn from(definition: SequenceSourceDefinition) -> Self {
        Self::SequenceSource(definition)
    }
}

impl From<CountDefinition> for OperationDefinition {
    fn from(definition: CountDefinition) -> Self {
        Self::Count(definition)
    }
}
