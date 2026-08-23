//! Durable dataflow execution built from [`Flow`], [`Stage`], and [`Operation`].
//!
//! `Flow` owns an immutable directed graph and its fair scheduler. A `Stage`
//! owns one operation and is the only transaction/publication boundary.
//! `Operation` owns domain semantics but receives only a shared transaction
//! borrow, so it cannot begin or commit transactions.
//!
//! ```no_run
//! use dogpaddle_flow::{Decision, Event, Flow, Operation, OperationError, StepOutcome, Work};
//! use dogpaddle_store::{Cell, DataPlacement, Transaction};
//!
//! struct Source;
//!
//! impl Operation for Source {
//!     fn fingerprint(&self) -> &[u8] { b"source:v1" }
//!
//!     fn step(
//!         &mut self,
//!         _work: Work<'_>,
//!         _transaction: &Transaction<'_>,
//!     ) -> Result<Decision, OperationError> {
//!         Ok(Decision::Complete { output: Some(b"hello".to_vec()) })
//!     }
//! }
//!
//! struct Sink { count: Cell<u64> }
//!
//! impl Operation for Sink {
//!     fn fingerprint(&self) -> &[u8] { b"sink:v1:count" }
//!
//!     fn step(
//!         &mut self,
//!         work: Work<'_>,
//!         transaction: &Transaction<'_>,
//!     ) -> Result<Decision, OperationError> {
//!         if let Event::Data { .. } = work.event() {
//!             let mut count = self.count.access(transaction)?;
//!             count.set(&(count.get()?.unwrap_or(0) + 1))?;
//!         }
//!         Ok(Decision::Complete { output: None })
//!     }
//! }
//!
//! # fn run(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
//! let mut flow = Flow::create(path)?;
//! let count = Cell::new(flow.data("count", DataPlacement::Shared)?);
//! let source = flow.stage("source", &[], Source)?;
//! let sink = flow.stage("sink", &["input"], Sink { count })?;
//! flow.connect(source, sink, "input")?;
//!
//! loop {
//!     match flow.step()? {
//!         StepOutcome::Progress => {}
//!         StepOutcome::Idle => break, // wait for an external wakeup
//!         StepOutcome::Finished => break,
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Reopening—including resuming interrupted provisioning—requires the same data
//! declarations, graph, and operation fingerprints. A successful checkpoint,
//! published output, operation state, input cursor, and scheduler cursor are
//! committed atomically. `Pending` rolls back the entire attempt. Previously
//! published blocks remain valid if a later operation fails.
//!
//! Every edge retains at most one output block; fan-out waits for the slowest
//! consumer. A leaf stage cannot publish output. External effects are outside
//! the Store transaction and therefore need a stable idempotency key when crash
//! retries must not duplicate them.
//!
//! One Store path has one live Flow executor. The Store opens MDBX exclusively;
//! dropping the Flow releases that lease so another process can reopen it.

mod error;
mod flow;
mod operation;
mod stage;

pub use error::{FlowError, OperationError};
pub use flow::{Flow, StepOutcome};
pub use operation::{Decision, Event, Operation, Work};
pub use stage::Stage;
