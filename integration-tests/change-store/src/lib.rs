//! Focused data for the public Change + `SubscribedLog` seam.
//!
//! This package contains no product behavior. Correctness tests and benchmarks
//! share only one nested Change and representative exact-Schema entries.

mod fixture;

pub use fixture::{
    EncodedChange, EncodedChanges, fixed_schema_changes_fixture, nested_change_fixture,
};
