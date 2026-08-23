use dogpaddle_store::Transaction;

use crate::OperationError;

/// The immutable event pinned by a [`Stage`](crate::Stage).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event<'a> {
    /// The singleton work item of a zero-input operation.
    Start,
    /// One durable block from a named input port.
    Data {
        /// The input port.
        port: &'a str,
        /// The block's position in the upstream output.
        position: u64,
        /// Opaque operation data.
        bytes: &'a [u8],
    },
    /// The named input has no more blocks.
    End {
        /// The input port.
        port: &'a str,
    },
}

/// One pinned event and its last committed operation checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Work<'a> {
    event: Event<'a>,
    checkpoint: Option<&'a [u8]>,
}

impl<'a> Work<'a> {
    pub(crate) const fn new(event: Event<'a>, checkpoint: Option<&'a [u8]>) -> Self {
        Self { event, checkpoint }
    }

    /// Returns the pinned input event.
    #[must_use]
    pub const fn event(self) -> Event<'a> {
        self.event
    }

    /// Returns the last committed operation checkpoint, if any.
    #[must_use]
    pub const fn checkpoint(self) -> Option<&'a [u8]> {
        self.checkpoint
    }
}

/// The only durable boundaries an operation may request from its stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Abort this attempt and wait for another wakeup.
    Pending,
    /// Commit operation state and resume the same work without publishing output.
    /// The checkpoint may contain at most [`Self::MAX_CHECKPOINT_BYTES`] bytes.
    Checkpoint { checkpoint: Vec<u8> },
    /// Commit operation state, publish one output block, and resume the same work.
    /// The output and checkpoint are bounded by the associated size constants.
    Publish {
        output: Vec<u8>,
        checkpoint: Vec<u8>,
    },
    /// Commit final state, optionally publish one bounded block, and consume the work.
    Complete { output: Option<Vec<u8>> },
}

impl Decision {
    /// Largest output block accepted by a stage.
    pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

    /// Largest checkpoint accepted by a stage.
    pub const MAX_CHECKPOINT_BYTES: usize = 64 * 1024;
}

/// The semantic transition owned by one [`Stage`](crate::Stage).
///
/// An operation is synchronous and receives only a shared transaction borrow.
/// It may bind its injected collections to that borrow, but cannot begin or
/// commit a transaction. Fields on the operation object may cache only
/// discardable, reconstructible runtime state. Any semantic state that survives
/// a retry must be in the transaction or encoded in the returned checkpoint.
/// Object-field changes are not transactional: they remain after [`Decision::Pending`]
/// in the current process and disappear on reopen. They may affect timing or a
/// temporary `Pending`, but rebuilding them must not change the output or
/// operation state eventually committed for the same work.
pub trait Operation: Send + 'static {
    /// Stable identity and complete semantic configuration used to reject an
    /// incompatible reopen. This includes every durable handle binding, codec
    /// version, and option that can change observable behavior.
    fn fingerprint(&self) -> &[u8];

    /// Advances one pinned work item.
    ///
    /// # Errors
    ///
    /// Returning an error rolls back the attempt. Once Flow returns the matching
    /// stage failure, that failure is durable. A process crash before the failure
    /// record commits may retry the unobserved attempt.
    fn step(
        &mut self,
        work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError>;
}
