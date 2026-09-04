#![doc = include_str!("../README.md")]

mod checkpoint;
mod config;
mod connector;
mod distribution;
mod error;
mod jvm;
mod protocol;

pub use checkpoint::Checkpoint;
pub use config::ConnectorConfig;
pub use connector::{Connector, Delivery, Header, Record};
pub use error::{Error, ErrorKind};
pub use jvm::DebeziumRuntime;

#[cfg(test)]
mod tests;
