use std::num::NonZeroI64;

use thiserror::Error;

use super::Record;

const INSERTION_DIFF: NonZeroI64 = NonZeroI64::new(1).expect("one is non-zero");
const RETRACTION_DIFF: NonZeroI64 = NonZeroI64::new(-1).expect("negative one is non-zero");

/// One weighted change in an ordered differential stream.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Change {
    diff: NonZeroI64,
    record: Record,
}

impl Change {
    /// Constructs a change from a non-zero difference and a record.
    #[must_use]
    pub const fn new(diff: NonZeroI64, record: Record) -> Self {
        Self { diff, record }
    }

    /// Constructs a change from a raw signed difference.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeError::ZeroDiff`] when `diff` is zero.
    pub fn try_new(diff: i64, record: Record) -> Result<Self, ChangeError> {
        let diff = NonZeroI64::new(diff).ok_or(ChangeError::ZeroDiff)?;
        Ok(Self::new(diff, record))
    }

    /// Constructs a single insertion.
    #[must_use]
    pub const fn insertion(record: Record) -> Self {
        Self::new(INSERTION_DIFF, record)
    }

    /// Constructs a single retraction.
    #[must_use]
    pub const fn retraction(record: Record) -> Self {
        Self::new(RETRACTION_DIFF, record)
    }

    /// Returns this change's non-zero signed difference.
    #[must_use]
    pub const fn diff(&self) -> NonZeroI64 {
        self.diff
    }

    /// Returns the changed record.
    #[must_use]
    pub const fn record(&self) -> &Record {
        &self.record
    }

    /// Consumes this change into its difference and record.
    #[must_use]
    pub fn into_parts(self) -> (NonZeroI64, Record) {
        (self.diff, self.record)
    }
}

/// A change construction failure.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ChangeError {
    /// A zero difference carries no change and is not representable.
    #[error("a change difference cannot be zero")]
    ZeroDiff,
}
