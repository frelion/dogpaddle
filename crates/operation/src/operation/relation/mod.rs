//! Crate-private row identity and ordering shared by relational operations.

mod order;
mod row;

pub(crate) use order::{OrderError, indexable, order_key, ordered_value};
pub(crate) use row::{RowError, canonical_row, decode_canonical_row, encode_canonical, row_hash};

#[cfg(test)]
mod tests;
