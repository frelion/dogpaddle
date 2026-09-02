//! Three focused fixtures for the public Change + `AppendLog` seam.
//!
//! This package contains no product behavior. Correctness tests and benchmarks
//! share only the minimum data needed to exercise logical ordering, projected
//! ownership, and heterogeneous storage pages.

mod fixture;

pub use fixture::{
    DiffEvent, EncodedChanges, OrderedDiffFixture, ProjectableFixture, assert_change_eq,
    flatten_ordered, heterogeneous_pages_fixture, order_checksum, ordered_diff_fixture,
    projectable_fixture, wide_change,
};
