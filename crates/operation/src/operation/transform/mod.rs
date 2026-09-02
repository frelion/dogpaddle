//! Transform operations that consume upstream records and produce derived records.

pub(crate) mod count;
pub(crate) mod project;

pub use count::{CountDefinition, CountError, CountOperation};
pub use project::{ProjectDefinition, ProjectError, ProjectOperation, ProjectSchemaError};
