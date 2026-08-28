use std::error::Error;

use dogpaddle_change::Change;
use dogpaddle_store::TransactionAccess;

use crate::OperationDefinition;

pub mod sink;
pub mod source;
pub mod transform;

/// One complete input Change borrowed for an Operation turn.
#[derive(Clone, Copy, Debug)]
pub struct OperationInput<'change> {
    /// Zero-based ordinal in the Definition's ordered inputs.
    pub port: usize,
    /// Complete Change offered on `port`.
    pub change: &'change Change,
}

/// Progress made against the complete input Change offered to one turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputProgress {
    /// Retains the current input for another turn.
    ///
    /// The caller commits the Operation's state and optional output, but does
    /// not advance the input. Its next turn must receive the same complete
    /// Change on the same port.
    Keep,
    /// Completes the current input Change.
    ///
    /// The caller may advance to another input only after committing the
    /// Operation's state, optional output, and input progress atomically.
    Complete,
}

/// Work produced by a turn whose transaction may be committed.
#[derive(Clone, Debug)]
pub struct TurnCommit {
    /// Progress for the offered input, or `None` for an input-free Operation.
    ///
    /// This must be `Some` exactly when the turn received an
    /// [`OperationInput`].
    pub input: Option<InputProgress>,
    /// At most one owned output Change published by this turn.
    pub output: Option<Change>,
}

/// Decision returned by one Operation turn.
#[derive(Clone, Debug)]
pub enum TurnDecision {
    /// Makes no progress and asks the caller to roll back the turn.
    ///
    /// Any offered input remains current and must be offered again unchanged.
    Idle,
    /// Makes progress that the caller may commit atomically.
    Commit(TurnCommit),
}

/// Type-erased failure from one concrete Operation turn.
pub type OperationError = Box<dyn Error + Send + Sync + 'static>;

/// Runtime parent trait implemented by every materialized operation.
pub trait Operation: Send + Sync + 'static {
    /// Returns the pure definition that materialized this operation.
    fn definition(&self) -> &dyn OperationDefinition;

    /// Executes one turn in an existing transaction.
    ///
    /// A source receives `None`. An input Operation receives exactly one
    /// complete Change. The caller retains transaction ownership and interprets
    /// the returned decision:
    ///
    /// - [`TurnDecision::Idle`] rolls back every write from the turn.
    /// - [`InputProgress::Keep`] commits the Operation's state and optional
    ///   output without advancing the input. The same complete Change on the
    ///   same port is offered again on its next turn.
    /// - [`InputProgress::Complete`] commits the Operation's state, optional
    ///   output, and completion of the current Change atomically.
    ///
    /// An input-free Operation returns a commit with `input: None`. An
    /// Operation receiving an input returns a commit with `input: Some(_)`.
    /// Returning a shape that does not match the invocation is a protocol
    /// violation.
    ///
    /// A turn can be replayed after an idle decision, an error, or failure of
    /// the caller's enclosing commit. Implementations must therefore avoid
    /// non-transactional observable side effects inside `turn`; those require a
    /// separate idempotency protocol beyond this Store transaction contract.
    ///
    /// # Errors
    ///
    /// Returns an erased concrete Operation failure. The caller must roll back
    /// the transaction on any error and, when an input was offered, offer the
    /// same complete Change on the same port again on a later turn.
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError>;
}
