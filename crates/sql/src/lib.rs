#![doc = include_str!("../README.md")]

mod aggregate;
mod endpoint;
mod error;
mod plan;
mod program;
mod syntax;

pub use error::SqlError;
pub use program::SqlProgram;
