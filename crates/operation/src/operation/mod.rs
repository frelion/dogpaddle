use std::error::Error;

use dogpaddle_change::Change;
use dogpaddle_store::TransactionAccess;

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

/// Action returned by one Operation turn.
#[derive(Clone, Debug)]
pub enum Action {
    /// Makes no progress and asks the caller to roll back the turn.
    ///
    /// Any offered input remains current and must be offered again unchanged.
    Idle,
    /// Commits the Operation's state and optional output without completing an input.
    ///
    /// An input Operation retains the complete input Change for its next turn.
    /// An input-free Operation uses this action for a successful turn.
    Commit(Option<Change>),
    /// Commits the Operation's state, optional output, and input completion atomically.
    ///
    /// The caller advances the offered input only after the enclosing
    /// transaction commits successfully. Returning this action from an
    /// input-free turn is a protocol violation.
    Complete(Option<Change>),
}

/// Type-erased failure from one concrete Operation turn.
pub type OperationError = Box<dyn Error + Send + Sync + 'static>;

/// Runtime parent trait implemented by every materialized operation.
pub trait Operation: Send + 'static {
    /// Executes one turn in an existing transaction.
    ///
    /// A source receives `None`. An input Operation receives exactly one
    /// complete Change. The caller retains transaction ownership and interprets
    /// the returned action:
    ///
    /// - [`Action::Idle`] rolls back every write from the turn.
    /// - [`Action::Commit`] commits the Operation's state and optional output
    ///   without advancing an offered input. The same complete Change on the
    ///   same port is offered again on its next turn.
    /// - [`Action::Complete`] commits the Operation's state, optional output,
    ///   and completion of the offered Change atomically.
    ///
    /// An input-free Operation uses [`Action::Commit`] for successful work. An
    /// input Operation chooses [`Action::Commit`] to retain the current Change
    /// or [`Action::Complete`] to finish it. Returning [`Action::Complete`]
    /// without an input is a protocol violation.
    ///
    /// A turn can be replayed after an idle action, an error, or failure of
    /// the caller's enclosing commit. Implementations must therefore avoid
    /// non-transactional observable side effects inside `turn`; those require a
    /// separate idempotency protocol beyond this Store transaction contract.
    /// The mutable receiver may cache ephemeral runtime resources, but every
    /// fact that can affect replay semantics or cross-turn continuation must
    /// remain in declared durable data.
    ///
    /// # Errors
    ///
    /// Returns an erased concrete Operation failure. The caller must roll back
    /// the transaction on any error and, when an input was offered, offer the
    /// same complete Change on the same port again on a later turn.
    fn turn(
        &mut self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError>;
}
