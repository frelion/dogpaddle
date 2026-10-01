#![doc = include_str!("../README.md")]

mod bundle;
mod checkpoint;
mod config;
mod connector;
mod error;
mod jvm;
mod protocol;

pub use checkpoint::Checkpoint;
pub use config::ConnectorConfig;
pub use connector::{Connector, Delivery, Record};
pub use error::{Error, ErrorKind};
pub use jvm::DebeziumRuntime;

#[cfg(test)]
mod tests;
