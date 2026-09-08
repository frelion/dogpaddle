//! Focused data for the public Change + `SubscribedLog` seam.
//!
//! This package contains no product behavior. Correctness tests and benchmarks
//! share only nested projected Changes and representative encoded entries.

mod fixture;

pub use fixture::{
    EncodedChanges, ProjectableFixture, assert_change_eq, heterogeneous_changes_fixture,
    projectable_fixture,
};
