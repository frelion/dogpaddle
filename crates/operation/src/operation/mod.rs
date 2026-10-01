use std::error::Error;

use dogpaddle_change::Change;
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

mod boundary;
mod compute;
pub use boundary::{SinkOperation, SinkPending, SourceDelivery, SourceOperation};
pub use compute::{BudgetExceeded, PagedOperation, Progress, Resume, Step, StepBudget};
pub(crate) use compute::{Cursor, logical_array_bytes, logical_change_bytes};

pub(crate) mod relation;
pub mod scan;
pub mod sink;
pub mod transform;

/// Complete immutable input borrowed for a computation step.
#[derive(Clone, Copy, Debug)]
pub struct OperationInput<'change> {
    /// Zero-based ordinal in the Definition's ordered inputs.
    pub port: usize,
    /// Complete immutable input. A head selects its bounded window internally.
    pub change: &'change Change,
}

/// Concrete failure requiring the caller to roll back this step.
pub type OperationError = Box<dyn Error + Send + Sync + 'static>;

/// A transform that completes the offered slice in the caller's transaction.
///
/// Runtime state consists only of compiled expressions and typed handles.
/// Pending state and output are local to this call and disappear on rollback.
pub trait AtomicOperation: Send + 'static {
    /// Applies an input slice, charging shared logical bytes without consuming head items.
    /// # Errors
    /// Returns a semantic, state or budget failure requiring transaction rollback.
    fn apply(
        &self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Option<Change>, OperationError>;
}

/// A restored position does not belong to this kernel or immutable input.
#[derive(Debug, Error)]
#[error("operation resume is invalid")]
pub struct InvalidResume;

/// One materialized operation with its validated execution capability.
pub enum Operation {
    /// Transaction-local computation, also usable in a fused tail.
    Atomic(Box<dyn AtomicOperation>),
    /// Computation with an opaque caller-owned position.
    Paged(Box<dyn PagedOperation>),
    /// External source capture and its private published queue.
    Source(Box<dyn SourceOperation>),
    /// External sink delivery and its private outbox.
    Sink(Box<dyn SinkOperation>),
}
impl Operation {
    /// Returns the pure initial position for a computation head.
    #[must_use]
    pub fn initial_resume(&self) -> Resume {
        match self {
            Self::Paged(operation) => operation.initial_resume(),
            _ => Resume::batch(),
        }
    }
    /// Validates a position against this kernel and complete immutable input.
    /// # Errors
    /// Returns an error for a wrong variant, port, or position outside the input.
    pub fn validate_resume(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
    ) -> Result<(), OperationError> {
        match self {
            Self::Paged(operation) => operation.validate_resume(input, resume),
            Self::Atomic(_) | Self::Source(_)
                if matches!(resume.cursor, Cursor::Batch)
                    && resume.ordinal < u64::try_from(input.change.num_rows())? =>
            {
                Ok(())
            }
            _ => Err(InvalidResume.into()),
        }
    }
    /// Computes one page using the same allowance as all fused atomic tails.
    ///
    /// A captured Source page is sliced as identity data; this method performs
    /// no polling, capture or ACK. Every `More` differs from the supplied Resume.
    /// # Errors
    /// Returns semantic, state, malformed position or budget failures requiring rollback.
    pub fn step(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Step, OperationError> {
        if let Self::Paged(operation) = self {
            return operation.step(input, resume, access, budget);
        }
        self.validate_resume(input, resume)?;
        let start = usize::try_from(resume.ordinal)?;
        let length = budget.head_remaining().min(input.change.num_rows() - start);
        if length == 0 {
            return Err(BudgetExceeded.into());
        }
        let slice = if start == 0 && length == input.change.num_rows() {
            input.change.clone()
        } else {
            input.change.try_slice(start, length)?
        };
        budget.consume_head(length)?;
        let output = match self {
            Self::Atomic(operation) => operation.apply(
                OperationInput {
                    port: input.port,
                    change: &slice,
                },
                access,
                budget,
            )?,
            Self::Source(_) => Some(slice),
            _ => return Err(InvalidResume.into()),
        };
        let next = start + length;
        let progress = if next == input.change.num_rows() {
            Progress::Done
        } else {
            Progress::More(Resume {
                ordinal: u64::try_from(next)?,
                cursor: Cursor::Batch,
            })
        };
        Ok(Step { output, progress })
    }
}
