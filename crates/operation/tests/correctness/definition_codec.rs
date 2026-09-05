use std::panic::{AssertUnwindSafe, catch_unwind};

use dogpaddle_operation::{
    DefinitionCodecError, decode_definition, encode_definition,
    operation::transform::RunningEventCountDefinition,
};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const HEADER_LEN: usize = MAGIC.len() + size_of::<u16>() * 2;

#[test]
fn definition_envelope_rejects_invalid_magic_version_unknown_tag_and_trailing_bytes() {
    let canonical = encode_definition(&RunningEventCountDefinition::new());
    assert_eq!(&canonical[..MAGIC.len()], MAGIC);
    assert_eq!(
        &canonical[MAGIC.len()..MAGIC.len() + size_of::<u16>()],
        &1_u16.to_be_bytes()
    );

    assert_eq!(
        decode_definition(b"short").unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_magic = canonical.clone();
    invalid_magic[0] ^= 0xff;
    assert_eq!(
        decode_definition(&invalid_magic).unwrap_err(),
        DefinitionCodecError::InvalidMagic
    );

    let mut unsupported = canonical.clone();
    unsupported[MAGIC.len()..MAGIC.len() + size_of::<u16>()].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unsupported).unwrap_err(),
        DefinitionCodecError::UnsupportedVersion(2)
    );

    let mut unknown = canonical.clone();
    unknown[MAGIC.len() + size_of::<u16>()..HEADER_LEN].copy_from_slice(&99_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unknown).unwrap_err(),
        DefinitionCodecError::UnknownTag(99)
    );

    let mut trailing = canonical;
    trailing.push(0);
    assert_eq!(
        decode_definition(&trailing).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );
}

#[test]
fn definition_envelope_rejects_every_truncated_prefix() {
    let canonical = encode_definition(&RunningEventCountDefinition::new());
    for length in 0..canonical.len() {
        assert_eq!(
            decode_definition(&canonical[..length]).unwrap_err(),
            DefinitionCodecError::Truncated,
            "wrong error for outer envelope prefix {length}/{}",
            canonical.len()
        );
    }
}

#[test]
fn definition_decoder_never_panics_for_deterministic_arbitrary_bytes() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for length in 0..=256 {
        let mut input = vec![0_u8; length];
        for byte in &mut input {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&input)));
        assert!(result.is_ok(), "decoder panicked for input length {length}");
    }
}
