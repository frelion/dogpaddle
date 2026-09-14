//! Transform operations that consume input records and produce derived records.

pub(crate) mod aggregate;
pub(crate) mod asof_join;
pub(crate) mod distinct;
pub(crate) mod equi_join;
pub(crate) mod extend;
pub(crate) mod filter;
pub(crate) mod project;
pub(crate) mod running_event_count;
pub(crate) mod schema_align;
pub(crate) mod select;
pub(crate) mod union_all;

pub use aggregate::{
    AggregateCall, AggregateDefinition, AggregateDefinitionError, AggregateError,
    AggregateOperation, AggregateSchemaError,
};
pub use asof_join::{
    AsOfDirection, AsOfEqualityKey, AsOfEqualityMode, AsOfEquidistantPreference,
    AsOfJoinDefinition, AsOfJoinDefinitionError, AsOfJoinError, AsOfJoinKind, AsOfJoinOperation,
    AsOfJoinSchemaError, AsOfOrderKey, AsOfTieBreak, AsOfTieFallback,
};
pub use distinct::{DistinctDefinition, DistinctError, DistinctOperation};
pub use equi_join::{
    EquiJoinDefinition, EquiJoinDefinitionError, EquiJoinError, EquiJoinKind, EquiJoinOperation,
    EquiJoinSchemaError,
};
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
