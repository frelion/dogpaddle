//! Crate-private exact-row state shared by relational operations.

mod bucket;
mod row;
mod weights;

pub(crate) use bucket::CollisionBucket;
pub(crate) use row::{RowDigest, RowError, canonical_row, encode_canonical, row_digest, row_hash};
pub(crate) use weights::{RowWeightError, RowWeights, apply_weight};

#[cfg(test)]
mod tests;
