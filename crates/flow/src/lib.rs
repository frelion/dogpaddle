#![doc = include_str!("../README.md")]

mod error;
mod flow;
mod operation;
mod stage;

pub use error::{FlowError, OperationError};
pub use flow::{Flow, StepOutcome};
pub use operation::{Decision, Event, Operation, Work};
pub use stage::Stage;
