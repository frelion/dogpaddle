//! Transform operations that consume upstream records and produce derived records.

mod count;

pub use count::{CountData, CountDefinition, CountError, CountOperation};
pub(crate) use count::{TAG, decode_definition};
