//! Transform operations that consume upstream records and produce derived records.

pub(crate) mod count;
pub(crate) mod extend;
pub(crate) mod filter;
pub(crate) mod project;
pub(crate) mod select;
pub(crate) mod union_all;

pub use count::{CountDefinition, CountError, CountOperation};
pub use extend::{
    ExtendDefinition, ExtendDefinitionError, ExtendError, ExtendOperation, ExtendSchemaError,
};
pub use filter::{FilterDefinition, FilterError, FilterOperation, FilterSchemaError};
pub use project::{ProjectDefinition, ProjectError, ProjectOperation, ProjectSchemaError};
pub use select::{
    SelectDefinition, SelectDefinitionError, SelectError, SelectOperation, SelectSchemaError,
};
pub use union_all::{UnionAllDefinition, UnionAllError, UnionAllOperation, UnionAllSchemaError};
