//! Concrete durable source capture and target delivery boundaries.
use super::OperationError;
pub use super::scan::cdc_runtime::SourceDelivery;
pub use super::sink::buffered::{SinkPending, SinkPrepared};
use dogpaddle_change::Change;
use dogpaddle_store::{ReadTransactionAccess, TransactionAccess};

/// Source-owned published queue and bootstrap capture protocol.
pub trait SourceOperation: Send + 'static {
    /// Restores bounded durable controls without performing external I/O.
    /// # Errors
    /// Returns an error for invalid durable source state.
    fn restore(&mut self, access: ReadTransactionAccess<'_>) -> Result<(), OperationError>;
    /// Polls one real delivery or prepares a concrete maintenance action.
    /// # Errors
    /// Returns source, conversion, or admission failures.
    fn poll(&mut self) -> Result<Option<SourceDelivery>, OperationError>;
    /// Captures all data and checkpoint atomically; false means capacity is full.
    /// # Errors
    /// Returns an error for invalid state or an oversized capture.
    fn record(
        &self,
        access: TransactionAccess<'_>,
        delivery: &mut SourceDelivery,
    ) -> Result<bool, OperationError>;
    /// Consumes the original delivery after commit and its required durable WAL barrier.
    /// # Errors
    /// Returns external ACK or connector shutdown failures.
    fn ack(&mut self, delivery: SourceDelivery) -> Result<(), OperationError>;
    /// Reads the schema-bound front without removing it or performing external I/O.
    ///
    /// The caller decodes these bytes using the Source's exact output Schema.
    /// Capture only appends; the front remains stable until `consume_published`.
    /// # Errors
    /// Returns storage or oversized durable payload errors.
    fn published(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<Vec<u8>>, OperationError>;
    /// Removes the published front in the transaction completing its computation.
    ///
    /// The caller must retain the front through every page and downstream call.
    /// Payload removal does not copy or decode the entry and does not affect ACK.
    /// # Errors
    /// Returns storage or invalid queue-state errors.
    fn consume_published(&self, access: TransactionAccess<'_>) -> Result<(), OperationError>;
}

/// A sink owns its bounded outbox, prepared fixed-ID plan, and ID frontier.
pub trait SinkOperation: Send + 'static {
    /// Enqueues a page in the same transaction as parent advancement.
    ///
    /// The caller restores the sink through `load` before servicing input.
    /// A false result performs no transaction writes or runtime-state changes,
    /// so the caller may durably retain its computed page for a later retry.
    /// # Errors
    /// Returns schema, admission, or storage failures.
    fn try_enqueue(
        &mut self,
        access: TransactionAccess<'_>,
        page: &Change,
    ) -> Result<bool, OperationError>;
    /// Reads one bounded prefix or the existing prepared front without target I/O.
    /// # Errors
    /// Returns durable-state or storage failures.
    fn load(
        &mut self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<SinkPending>, OperationError>;
    /// Plans fixed IDs outside a Store transaction, merging the loaded prefix.
    /// # Errors
    /// Returns target lookup or prefix admission failures.
    fn prepare(&mut self, pending: SinkPending) -> Result<SinkPrepared, OperationError>;
    /// Persists the prepared front before a commit and WAL barrier.
    /// # Errors
    /// Returns storage or inconsistent-front failures.
    fn persist_prepared(
        &self,
        access: TransactionAccess<'_>,
        prepared: &SinkPrepared,
    ) -> Result<(), OperationError>;
    /// Delivers the exact fixed-ID plan after its durable barrier.
    /// # Errors
    /// Returns target errors; reopening replays the same plan.
    fn deliver(&mut self, prepared: &SinkPrepared) -> Result<(), OperationError>;
    /// Settles delivered entries and frontier in one short Store transaction.
    /// # Errors
    /// Returns storage or inconsistent-front failures.
    fn settle(
        &mut self,
        access: TransactionAccess<'_>,
        prepared: &SinkPrepared,
    ) -> Result<(), OperationError>;
}
