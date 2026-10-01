//! Crate-private row identity and ordering shared by relational operations.

mod order;
mod row;

pub(crate) use order::{OrderError, indexable, order_key, ordered_value};
pub(crate) use row::{
    ArrowOutput, RowError, canonical_row_bounded, canonical_row_size_bounded,
    decode_canonical_rows_bounded, encode_canonical, encode_canonical_bounded, row_hash,
};

#[cfg(test)]
mod tests;
