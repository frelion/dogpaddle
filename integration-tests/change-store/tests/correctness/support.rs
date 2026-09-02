use dogpaddle_change::{Change, ChangeProjection, decode_change, decode_change_projected};
use dogpaddle_store::CodecError as StoreCodecError;

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}
