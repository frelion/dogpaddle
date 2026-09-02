//! Focused data for the public Change + `AppendLog` seam.
//!
//! This package contains no product behavior. Correctness tests and benchmarks
//! share only nested projected Changes and representative encoded entries.

mod fixture;

pub use fixture::{
    EncodedChanges, ProjectableFixture, assert_change_eq, heterogeneous_pages_fixture,
    order_checksum, projectable_fixture,
};
