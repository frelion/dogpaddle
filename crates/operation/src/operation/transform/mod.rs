//! Transform operations that consume upstream records and produce derived records.

pub(crate) mod count;

pub use count::{CountData, CountDefinition, CountError, CountOperation};
