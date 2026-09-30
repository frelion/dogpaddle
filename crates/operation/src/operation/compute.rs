use std::borrow::Cow;

use dogpaddle_change::Change;
use dogpaddle_store::{CodecError, StoreValue, TransactionAccess};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{OperationError, OperationInput};

/// One transaction's shared logical byte and head work-item allowance.
#[derive(Debug)]
pub struct StepBudget {
    head_remaining: usize,
    remaining_bytes: usize,
}

/// The current page must be rolled back and retried with fewer head items.
#[derive(Debug, Error)]
#[error("computation step budget exceeded")]
pub struct BudgetExceeded;

impl StepBudget {
    /// Creates the allowance for one transaction attempt.
    #[must_use]
    pub const fn new(head_items: usize, bytes: usize) -> Self {
        Self {
            head_remaining: head_items,
            remaining_bytes: bytes,
        }
    }
    /// Returns the unconsumed head scan or input-event allowance.
    #[must_use]
    pub const fn head_remaining(&self) -> usize {
        self.head_remaining
    }
    /// Returns the unconsumed shared logical byte allowance.
    #[must_use]
    pub const fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }
    /// Charges logical bytes shared by the head and all atomic tails.
    /// Charge known reads, allocations, and writes before performing them.
    /// # Errors
    /// Returns `BudgetExceeded` when the attempt cannot admit these bytes.
    pub fn charge(&mut self, bytes: usize) -> Result<(), BudgetExceeded> {
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or(BudgetExceeded)?;
        Ok(())
    }
    /// Consumes head work items. Atomic tails must not call this method.
    /// # Errors
    /// Returns `BudgetExceeded` when the head allowance is exhausted.
    pub fn consume_head(&mut self, items: usize) -> Result<(), BudgetExceeded> {
        self.head_remaining = self
            .head_remaining
            .checked_sub(items)
            .ok_or(BudgetExceeded)?;
        Ok(())
    }
}

/// One bounded computation page and its sole next position.
#[derive(Debug)]
pub struct Step {
    /// Ordered relational output, absent when this page emitted no events.
    pub output: Option<Change>,
    /// Next position in the complete immutable input.
    pub progress: Progress,
}

/// Progress through a frame's complete immutable input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Progress {
    /// The input has more work at this opaque position.
    More(Resume),
    /// Every event in this input has completed.
    Done,
}

/// Opaque, strictly encoded position owned only by the calling frame.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Resume {
    pub(crate) ordinal: u64,
    pub(crate) cursor: Cursor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Cursor {
    Batch,
    EquiJoin(super::transform::equi_join::state::JoinCursor),
    AsOf(super::transform::asof_join::state::AsOfCursor),
}

impl Resume {
    /// Initial position for a captured source page or atomic computation head.
    #[must_use]
    pub const fn batch() -> Self {
        Self {
            ordinal: 0,
            cursor: Cursor::Batch,
        }
    }
}

impl StoreValue for Resume {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let encoded = bincode::serde::encode_to_vec((1_u8, self), config())
            .map_err(|_| CodecError::new("resume cannot be encoded"))?;
        if encoded.len() > 64 * 1024 {
            return Err(CodecError::new("resume exceeds control byte limit"));
        }
        Ok(encoded)
    }
    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.len() > 64 * 1024 || bytes.first() != Some(&1) {
            return Err(CodecError::new("invalid resume version or length"));
        }
        let ((_, value), consumed): ((u8, Self), usize) =
            bincode::serde::borrow_decode_from_slice(bytes.as_ref(), config())
                .map_err(|_| CodecError::new("invalid resume"))?;
        if consumed != bytes.len() || value.encode_value()?.as_ref() != bytes.as_ref() {
            return Err(CodecError::new("non-canonical resume"));
        }
        Ok(value)
    }
}
fn config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_big_endian()
        .with_variable_int_encoding()
        .with_limit::<65536>()
}

/// A computation whose input may require multiple transactional pages.
pub trait PagedOperation: Send + 'static {
    /// Returns the pure initial position for this kernel.
    fn initial_resume(&self) -> Resume;
    /// Checks an opaque position against the kernel and complete input.
    /// # Errors
    /// Returns a malformed or incorrectly bound position error.
    fn validate_resume(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
    ) -> Result<(), OperationError>;
    /// Computes a page in the caller's transaction without retaining progress.
    /// # Errors
    /// Returns a semantic, state or allowance failure requiring rollback.
    fn step(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Step, OperationError>;
}

/// Logical Arrow bytes in a slice, excluding retained backing buffers.
pub(crate) fn logical_change_bytes(change: &Change) -> usize {
    change
        .records()
        .columns()
        .iter()
        .fold(change.num_rows().saturating_mul(8), |bytes, array| {
            bytes.saturating_add(logical_array_bytes(array.as_ref()))
        })
}
pub(crate) fn logical_array_bytes(array: &dyn arrow_array::Array) -> usize {
    use arrow_array::{Array, BinaryArray, ListArray, StringArray, StructArray};
    use arrow_schema::DataType;
    let validity = if array.nulls().is_some() {
        array.len().div_ceil(8)
    } else {
        0
    };
    let values = match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => array.len().div_ceil(8),
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("validated Utf8 array");
            let offsets = array.value_offsets();
            array
                .len()
                .saturating_add(1)
                .saturating_mul(4)
                .saturating_add(
                    usize::try_from(offsets[array.len()] - offsets[0])
                        .expect("validated nonnegative offsets"),
                )
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("validated Binary array");
            let offsets = array.value_offsets();
            array
                .len()
                .saturating_add(1)
                .saturating_mul(4)
                .saturating_add(
                    usize::try_from(offsets[array.len()] - offsets[0])
                        .expect("validated nonnegative offsets"),
                )
        }
        DataType::List(_) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("validated List array");
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[0]).expect("validated offsets");
            let length =
                usize::try_from(offsets[array.len()] - offsets[0]).expect("validated offsets");
            array
                .len()
                .saturating_add(1)
                .saturating_mul(4)
                .saturating_add(logical_array_bytes(
                    array.values().slice(start, length).as_ref(),
                ))
        }
        DataType::Struct(_) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated Struct array");
            array.columns().iter().fold(0_usize, |bytes, column| {
                bytes.saturating_add(logical_array_bytes(column.as_ref()))
            })
        }
        data_type => array.len().saturating_mul(
            data_type
                .primitive_width()
                .expect("validated DogPaddle primitive"),
        ),
    };
    validity.saturating_add(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_codec_rejects_oversized_declared_cursor_lengths_before_copying() {
        // Current version/ordinal/kernel plus each closed cursor payload.
        for encoded in [vec![1, 0, 1, 0, 1, 1, 7], vec![1, 0, 2, 1, 1, 7]] {
            let decoded = Resume::decode_value(Cow::Borrowed(&encoded)).unwrap();
            assert_eq!(decoded.encode_value().unwrap().as_ref(), encoded);
            for length in 0..encoded.len() {
                assert!(Resume::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
            }
            let mut huge = encoded[..encoded.len() - 2].to_vec();
            huge.push(253); // Canonical bincode u64 length prefix.
            huge.extend_from_slice(&u64::MAX.to_be_bytes());
            assert!(Resume::decode_value(Cow::Borrowed(&huge)).is_err());
            let mut trailing = encoded;
            trailing.push(0);
            assert!(Resume::decode_value(Cow::Borrowed(&trailing)).is_err());
        }
        assert!(Resume::decode_value(Cow::Borrowed(&vec![0; 64 * 1024 + 1])).is_err());
    }
}
