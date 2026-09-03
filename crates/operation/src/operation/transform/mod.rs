//! Transform operations that consume upstream records and produce derived records.

pub(crate) mod extend;
pub(crate) mod filter;
pub(crate) mod project;
pub(crate) mod running_event_count;
pub(crate) mod schema_align;
pub(crate) mod select;
pub(crate) mod union_all;

pub use extend::{
    ExtendDefinition, ExtendDefinitionError, ExtendError, ExtendOperation, ExtendSchemaError,
};
pub use filter::{FilterDefinition, FilterError, FilterOperation, FilterSchemaError};
pub use project::{ProjectDefinition, ProjectError, ProjectOperation, ProjectSchemaError};
pub use running_event_count::{
    RunningEventCountDefinition, RunningEventCountError, RunningEventCountOperation,
};
pub use schema_align::{
    SchemaAlignDefinition, SchemaAlignDefinitionError, SchemaAlignError, SchemaAlignField,
    SchemaAlignFieldError, SchemaAlignOperation, SchemaAlignSchemaError,
};
pub use select::{
    SelectDefinition, SelectDefinitionError, SelectError, SelectOperation, SelectSchemaError,
};
pub use union_all::{UnionAllDefinition, UnionAllError, UnionAllOperation, UnionAllSchemaError};
