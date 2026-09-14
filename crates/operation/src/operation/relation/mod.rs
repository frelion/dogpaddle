//! Crate-private exact-row state shared by relational operations.

mod row;

pub(crate) use row::{
    RowError, canonical_row, canonical_row_bounded, canonical_row_size_bounded,
    decode_canonical_row, encode_canonical, row_hash,
};

#[cfg(test)]
mod tests;
