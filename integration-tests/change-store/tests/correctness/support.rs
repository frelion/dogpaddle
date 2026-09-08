use dogpaddle_change::{Change, ChangeProjection, decode_change, decode_change_projected};

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, dogpaddle_change::CodecError> {
    decode_change(encoded)
}

pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, dogpaddle_change::CodecError> {
    decode_change_projected(encoded, projection)
}
