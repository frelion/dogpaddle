#![doc = include_str!("../README.md")]

mod aggregate;
mod compiler;
mod endpoint;
mod error;
mod lower;
mod program;

pub use error::SqlError;
pub use program::SqlProgram;
