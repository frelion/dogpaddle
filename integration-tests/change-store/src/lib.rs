//! Shared fixtures and oracles for the external Change + Store test package.
//!
//! This crate contains no product behavior. It gives correctness tests and
//! benchmark targets one definition of the persisted workload.

mod fixture;
mod oracle;
mod persona;
mod store_fixture;
mod workload;

pub use fixture::{narrow_change, narrow_schema, wide_change, wide_schema, wide_with_payloads};
pub use oracle::{Event, assert_change_eq, checksum_change, flatten_narrow};
pub use persona::{
    ChangeWorkloadSpec, ChurnEvent, ChurnModel, ChurnValidationError, DiffModel, GeneratedChange,
    PersonaWorkload, ProjectionDescriptor, ProjectionProfile, SchemaDescriptor, WorkloadDescriptor,
    WorkloadPersona, churn_changes, flatten_churn_changes, generate_persona_change,
    generate_persona_workload, valid_churn_events, validate_churn,
};
pub use store_fixture::StoreFixture;
pub use workload::{EncodedWorkload, encoded_wide_workload};
