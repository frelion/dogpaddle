#![doc = include_str!("../README.md")]

mod error;
mod flow;
mod format;
mod stage;
mod topology;

pub use error::FlowError;
pub use flow::{Flow, FlowBuilder};
pub use format::FlowDefinitionError;
pub use topology::{InvalidStageIdReason, StageRef, TopologyError};
