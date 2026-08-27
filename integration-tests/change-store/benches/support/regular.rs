use dogpaddle_change::{Change, ChangeProjection, decode_change_projected};
use dogpaddle_store::CodecError as StoreCodecError;

pub(crate) fn checked_product(label: &str, left: usize, right: usize) -> usize {
    left.checked_mul(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

pub(crate) fn checked_sum(label: &str, left: usize, right: usize) -> usize {
    left.checked_add(right)
        .unwrap_or_else(|| panic!("{label} exceeds usize"))
}

pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}
