use super::super::SchemaBoundChangeCodec;
use super::support::*;

#[test]
fn complete_decode_rejects_invalid_utf8_values() {
    let change = layout_change();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let mut encoded = codec.encode(&change).unwrap();
    let values = field_buffer_range(&encoded, &codec, "label", 2);
    encoded[values.start] = 0xff;
    assert_arrow_error(&codec.decode(&encoded));
    assert_arrow_error(&codec.decode_owned(encoded));
}

#[test]
fn complete_decode_rejects_invalid_list_offsets() {
    let change = layout_change();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let mut encoded = codec.encode(&change).unwrap();
    let offsets = field_buffer_range(&encoded, &codec, "items", 1);
    let last = offsets.start + change.num_rows() * size_of::<i32>();
    encoded[last..last + size_of::<i32>()].copy_from_slice(&i32::MAX.to_le_bytes());
    assert_arrow_error(&codec.decode(&encoded));
    assert_arrow_error(&codec.decode_owned(encoded));
}
