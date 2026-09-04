use std::{error::Error, fmt};

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

/// Transactional decision produced by one prepared Operation turn.
#[derive(Debug)]
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

/// Type-erased failure before one prepared Operation turn commits.
pub type OperationError = Box<dyn Error + Send + Sync + 'static>;

/// Failure after one prepared Operation turn has committed.
///
/// This phase is deliberately distinct from [`OperationError`]: the local
/// Store transaction is already durable and cannot be rolled back.
#[derive(Debug)]
pub struct PostCommitError {
    source: OperationError,
}

impl PostCommitError {
    /// Wraps one concrete post-commit failure.
    ///
    /// An already erased [`OperationError`] can instead be converted with
    /// [`From`].
    pub fn new<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            source: Box::new(source),
        }
    }
}

impl fmt::Display for PostCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for PostCommitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<OperationError> for PostCommitError {
    fn from(source: OperationError) -> Self {
        Self { source }
    }
}

/// Work that may run only after the prepared turn's Store transaction commits.
///
/// Dropping this value abandons the work. In particular, dropping it after a
/// rollback, backpressure, or commit failure must not confirm an external
/// delivery. The callback itself is never run from [`Drop`].
///
/// The preceding transaction must persist everything needed to recover if the
/// process exits before the callback runs or if the callback fails. A callback
/// is a settlement of durable intent, never the sole owner of replay state.
/// After a callback error, the caller must not invoke the same runtime
/// Operation again; it must reconstruct it from durable state first.
#[must_use = "after-commit work must be run after a successful Store commit or deliberately dropped"]
pub struct AfterCommit<'turn> {
    effect: Option<Box<dyn FnOnce() -> Result<(), PostCommitError> + 'turn>>,
}

impl<'turn> AfterCommit<'turn> {
    /// Creates one consuming post-commit effect without executing it.
    pub fn new<F>(effect: F) -> Self
    where
        F: FnOnce() -> Result<(), PostCommitError> + 'turn,
    {
        Self {
            effect: Some(Box::new(effect)),
        }
    }

    /// Creates an empty completion for a wholly transactional turn.
    pub const fn none() -> Self {
        Self { effect: None }
    }

    /// Runs the completion after the enclosing Store transaction has committed.
    ///
    /// # Errors
    ///
    /// Returns the concrete post-commit failure. The Store transaction has
    /// already committed when this method is called and cannot be rolled back.
    pub fn run(self) -> Result<(), PostCommitError> {
        self.effect.map_or(Ok(()), |effect| effect())
    }
}

type PreparedApply<'turn> = Box<
    dyn for<'transaction> FnOnce(
            TransactionAccess<'transaction>,
        ) -> Result<(Action, AfterCommit<'turn>), OperationError>
        + 'turn,
>;

enum PreparedTurnInner<'turn> {
    Transactional {
        operation: &'turn mut dyn TransactionalOperation,
        input: Option<OperationInput<'turn>>,
    },
    Custom(PreparedApply<'turn>),
}

/// One transaction-ready Operation turn.
///
/// Applying this value consumes it, so its transactional body cannot run
/// twice. Dropping it abandons the turn without running post-commit work.
pub struct PreparedTurn<'turn> {
    inner: PreparedTurnInner<'turn>,
}

impl<'turn> PreparedTurn<'turn> {
    fn transactional(
        operation: &'turn mut dyn TransactionalOperation,
        input: Option<OperationInput<'turn>>,
    ) -> Self {
        Self {
            inner: PreparedTurnInner::Transactional { operation, input },
        }
    }

    /// Applies this turn inside an existing Store transaction.
    ///
    /// The body may access only its declared Store data through `access`. It
    /// must not commit the transaction or perform an observable effect that
    /// cannot be rolled back with it, unless that effect is protected by a
    /// separately specified durable idempotency protocol that makes replay
    /// after Store rollback safe.
    ///
    /// # Errors
    ///
    /// Returns a pre-commit failure without post-commit work. The caller must
    /// roll back the transaction. The Operation must remain
    /// safe to prepare again from unchanged durable state; a poisoned transient
    /// resource must be reset or marked for reconstruction before returning.
    pub fn apply(
        self,
        access: TransactionAccess<'_>,
    ) -> Result<(Action, AfterCommit<'turn>), OperationError> {
        match self.inner {
            PreparedTurnInner::Transactional { operation, input } => {
                let action = operation.apply(input, access)?;
                Ok((action, AfterCommit::none()))
            }
            PreparedTurnInner::Custom(apply) => apply(access),
        }
    }
}

/// Result of asking an Operation to produce one bounded turn.
#[must_use = "an Operation turn must be applied or deliberately abandoned"]
pub enum Turn<'turn> {
    /// The Operation currently has no work and no Store transaction is needed.
    Idle,
    /// One turn is ready to apply inside a Store transaction.
    Ready(PreparedTurn<'turn>),
}

impl<'turn> Turn<'turn> {
    /// Creates a ready turn from one consuming transactional body.
    ///
    /// The body returns both its transactional [`Action`] and any work that may
    /// run only after that transaction commits. Constructing this value does
    /// not execute the body; the caller does that through [`PreparedTurn::apply`].
    pub fn ready<F>(apply: F) -> Self
    where
        F: for<'transaction> FnOnce(
                TransactionAccess<'transaction>,
            )
                -> Result<(Action, AfterCommit<'turn>), OperationError>
            + 'turn,
    {
        Self::Ready(PreparedTurn {
            inner: PreparedTurnInner::Custom(Box::new(apply)),
        })
    }
}

/// Runtime parent trait implemented by every materialized operation.
pub trait Operation: Send + 'static {
    /// Produces one bounded turn while no Store write transaction is active.
    ///
    /// A source receives `None`. An input Operation receives exactly one
    /// complete Change. This phase may prepare bounded external work, but it
    /// must not confirm that work or advance any replay-sensitive fact before
    /// the returned prepared turn commits.
    ///
    /// [`Turn::Idle`] avoids opening a Store transaction. [`Turn::Ready`]
    /// contains a linear prepared turn that the caller applies once in a Store
    /// transaction. The caller interprets its returned [`Action`] as follows:
    ///
    /// - [`Action::Idle`] rolls back every write from the prepared turn.
    /// - [`Action::Commit`] commits Operation state and optional output without
    ///   advancing an offered input.
    /// - [`Action::Complete`] atomically commits Operation state, optional
    ///   output, and completion of the offered input.
    ///
    /// Only after that transaction commits may the caller run the returned
    /// [`AfterCommit`]. If preparation, application, output admission, or
    /// commit fails, the prepared turn or completion is dropped instead.
    /// Replay-sensitive continuation must remain in declared durable data;
    /// in-memory state may only cache reconstructible runtime resources.
    ///
    /// # Errors
    ///
    /// Returns a failure that occurred before a Store transaction was opened.
    /// No prepared turn exists and no external work may have been confirmed.
    /// A preparation error must leave this runtime safe to call again from
    /// unchanged durable state. An implementation that observes a poisoned
    /// transient resource must reset it itself, or remember to reconstruct it
    /// on its next turn, before returning.
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError>;
}

pub(crate) trait TransactionalOperation: Send + 'static {
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError>;
}

impl<O> Operation for O
where
    O: TransactionalOperation,
{
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        Ok(Turn::Ready(PreparedTurn::transactional(self, input)))
    }
}
