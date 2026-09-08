//! Crate-private exact-row state shared by relational operations.

mod row;

pub(crate) use row::{RowError, canonical_row, encode_canonical, row_hash};

#[cfg(test)]
mod tests;
