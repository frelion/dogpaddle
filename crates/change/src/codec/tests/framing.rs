use super::super::{CodecError, SchemaBoundChangeCodec};
use super::support::*;
use crate::ChangeError;
use arrow_ipc::{FieldNode, MetadataVersion};

#[test]
fn decoder_rejects_incomplete_noncanonical_or_unsupported_batches() {
    let change = simple_change(&[-1, 1]);
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    // Prefix and fingerprint failures have their own public tests; every batch truncation is invalid.
    for end in 40..encoded.len() {
        assert_both_invalid_encoding(&encoded[..end], &codec);
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_both_invalid_encoding(&trailing, &codec);
    let mut legacy = encoded.clone();
    legacy.drain(40..44);
    assert_both_invalid_encoding(&legacy, &codec);
    assert_both_invalid_encoding(&encoded[..40], &codec);
    let mut twice = encoded[..encoded.len() - 8].to_vec();
    twice.extend_from_slice(&encoded[40..]);
    assert_both_invalid_encoding(&twice, &codec);
    let mut oversized = encoded.clone();
    oversized[44..48].copy_from_slice(&(i32::MAX - 7).to_le_bytes());
    assert_both_invalid_encoding(&oversized, &codec);
    let metadata = ipc_batch_metadata(1, None, None, i64::MAX - 7, false);
    assert_both_invalid_encoding(&replace_batch_message(&encoded, &metadata, &[]), &codec);
    let compressed =
        replace_batch_message(&encoded, &ipc_batch_metadata(1, None, None, 0, true), &[]);
    assert_both_invalid_encoding(&compressed, &codec);
    let mut wrong_version = encoded.clone();
    let message = arrow_ipc::root_as_message(&encoded[48..]).unwrap();
    let version = message._tab.vtable().get(arrow_ipc::Message::VT_VERSION);
    let at = 48 + message._tab.loc() + usize::from(version);
    wrong_version[at..at + 2].copy_from_slice(&MetadataVersion::V4.0.to_le_bytes());
    assert_both_invalid_encoding(&wrong_version, &codec);
}

#[test]
fn decoder_rechecks_diff_values_and_nonempty_batch() {
    let change = simple_change(&[1]);
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let mut encoded = codec.encode(&change).unwrap();
    let diff = field_buffer_range(&encoded, &codec, "$dogpaddle.diff", 1);
    encoded[diff.start..diff.start + 8].copy_from_slice(&0_i64.to_le_bytes());
    assert!(matches!(
        codec.decode(&encoded),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));
    assert!(matches!(
        codec.decode_owned(encoded.clone()),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));
    let metadata = ipc_batch_metadata(0, Some(&[FieldNode::new(0, 0)]), Some(&[]), 0, false);
    assert_both_invalid_encoding(&replace_batch_message(&encoded, &metadata, &[]), &codec);
}
