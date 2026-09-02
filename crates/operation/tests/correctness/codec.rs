use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_operation::{
    DefinitionCodecError, OperationDefinition, decode_definition, encode_definition,
    operation::{
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ProjectDefinition},
    },
};

use super::support::decode_hex;

const COUNT_V1: &str = include_str!("../fixtures/v1/count_definition.hex");
const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");
const PROJECT_V1: &str = include_str!("../fixtures/v1/project_fields_0_2.hex");
const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_source_start_42.hex");

fn golden_cases() -> Vec<(Vec<u8>, Box<dyn OperationDefinition>)> {
    vec![
        (decode_hex(COUNT_V1), Box::new(CountDefinition::new())),
        (decode_hex(DISCARD_V1), Box::new(DiscardDefinition::new())),
        (
            decode_hex(PROJECT_V1),
            Box::new(ProjectDefinition::new([0, 2])),
        ),
        (
            decode_hex(SEQUENCE_V1),
            Box::new(SequenceSourceDefinition::new(42)),
        ),
    ]
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn arbitrary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ]))
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[test]
fn every_builtin_definition_has_stable_v1_golden_bytes() {
    for (golden, definition) in golden_cases() {
        assert_eq!(encode_definition(definition.as_ref()), golden);
        let decoded = decode_definition(&golden).unwrap();
        assert_eq!(decoded.kind(), definition.kind());
        assert_eq!(encode_definition(decoded.as_ref()), golden);
    }
}

#[test]
fn every_decoded_builtin_golden_reconstructs_its_schema_binding() {
    let sequence = decode_definition(&decode_hex(SEQUENCE_V1)).unwrap();
    assert_eq!(
        sequence.bind(&[]).unwrap().output_schema(),
        Some(&value_schema())
    );

    let arbitrary = arbitrary_schema();
    let count = decode_definition(&decode_hex(COUNT_V1)).unwrap();
    assert_eq!(
        count
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema(),
        Some(&count_schema())
    );

    let project = decode_definition(&decode_hex(PROJECT_V1)).unwrap();
    let expected = Arc::new(arbitrary.project(&[0, 2]).unwrap());
    assert_eq!(
        project
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    let discard = decode_definition(&decode_hex(DISCARD_V1)).unwrap();
    assert!(
        discard
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn every_truncated_golden_prefix_is_rejected() {
    for (golden, _) in golden_cases() {
        for length in 0..golden.len() {
            assert_eq!(
                decode_definition(&golden[..length]).unwrap_err(),
                DefinitionCodecError::Truncated,
                "wrong error for prefix length {length} of {} bytes",
                golden.len(),
            );
        }
    }
}

#[test]
fn definition_decoder_rejects_non_canonical_or_unknown_input() {
    let count = decode_hex(COUNT_V1);
    assert_eq!(
        decode_definition(b"short").unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_magic = count.clone();
    invalid_magic[0] ^= 0xff;
    assert_eq!(
        decode_definition(&invalid_magic).unwrap_err(),
        DefinitionCodecError::InvalidMagic
    );

    let version_offset = b"dogpaddle.operation\0".len();
    let mut unsupported = count.clone();
    unsupported[version_offset..version_offset + 2].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unsupported).unwrap_err(),
        DefinitionCodecError::UnsupportedVersion(2)
    );

    let mut unknown = count.clone();
    unknown[version_offset + 2..version_offset + 4].copy_from_slice(&99_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unknown).unwrap_err(),
        DefinitionCodecError::UnknownTag(99)
    );

    for (mut golden, _) in golden_cases() {
        golden.push(0);
        assert_eq!(
            decode_definition(&golden).unwrap_err(),
            DefinitionCodecError::TrailingBytes
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
