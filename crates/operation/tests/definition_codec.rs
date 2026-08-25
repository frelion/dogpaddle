use dogpaddle_operation::{
    DefinitionCodecError, decode_definition, encode_definition,
    operation::{source::SequenceSourceDefinition, transform::CountDefinition},
};

fn prefix(tag: u16) -> Vec<u8> {
    let mut encoded = b"dogpaddle.operation\0".to_vec();
    encoded.extend_from_slice(&1_u16.to_be_bytes());
    encoded.extend_from_slice(&tag.to_be_bytes());
    encoded
}

#[test]
fn count_definition_has_a_stable_canonical_encoding() {
    let definition = CountDefinition::new();
    let expected = prefix(2);

    assert_eq!(encode_definition(&definition), expected);
    assert_eq!(
        encode_definition(decode_definition(&expected).unwrap().as_ref()),
        expected
    );
}

#[test]
fn sequence_source_definition_has_a_stable_canonical_encoding() {
    let definition = SequenceSourceDefinition::new(42);
    let mut expected = prefix(1);
    expected.extend_from_slice(&42_u64.to_be_bytes());

    assert_eq!(encode_definition(&definition), expected);
    assert_eq!(
        encode_definition(decode_definition(&expected).unwrap().as_ref()),
        expected
    );
}

#[test]
fn definition_decoder_rejects_non_canonical_or_unknown_input() {
    assert_eq!(
        decode_definition(b"short").unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_magic = prefix(2);
    invalid_magic[0] ^= 0xff;
    assert_eq!(
        decode_definition(&invalid_magic).unwrap_err(),
        DefinitionCodecError::InvalidMagic
    );

    let mut unsupported = prefix(2);
    let version_offset = b"dogpaddle.operation\0".len();
    unsupported[version_offset..version_offset + 2].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unsupported).unwrap_err(),
        DefinitionCodecError::UnsupportedVersion(2)
    );

    assert_eq!(
        decode_definition(&prefix(99)).unwrap_err(),
        DefinitionCodecError::UnknownTag(99)
    );

    let mut trailing = prefix(2);
    trailing.push(0);
    assert_eq!(
        decode_definition(&trailing).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );

    let mut truncated_sequence = prefix(1);
    truncated_sequence.extend_from_slice(&[0; 7]);
    assert_eq!(
        decode_definition(&truncated_sequence).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut trailing_sequence = prefix(1);
    trailing_sequence.extend_from_slice(&[0; 9]);
    assert_eq!(
        decode_definition(&trailing_sequence).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );
}
