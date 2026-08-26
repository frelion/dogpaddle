use std::panic::catch_unwind;

use dogpaddle_flow::{Flow, FlowDefinitionError, FlowError};
use dogpaddle_operation::DefinitionCodecError;

use super::support::{fixture_bytes, publish_definition, rewrite_checksum};

const FLOW_MAGIC: &[u8] = b"dogpaddle.flow\0";
const OPERATION_MAGIC: &[u8] = b"dogpaddle.operation\0";
const V1_SEQUENCE_COUNT: &str = include_str!("../fixtures/v1/sequence_source_count.hex");

#[test]
fn open_reports_semantic_errors_after_a_valid_checksum() {
    let root = tempfile::tempdir().unwrap();
    let original = fixture_bytes(V1_SEQUENCE_COUNT);

    let mut invalid_magic = original.clone();
    invalid_magic[0] ^= 1;
    rewrite_checksum(&mut invalid_magic);
    let error = open_error(root.path(), "invalid-magic", &invalid_magic);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::InvalidMagic)
    ));

    let mut unsupported_version = original.clone();
    let version = FLOW_MAGIC.len();
    unsupported_version[version..version + 2].copy_from_slice(&2_u16.to_be_bytes());
    rewrite_checksum(&mut unsupported_version);
    let error = open_error(root.path(), "unsupported-version", &unsupported_version);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::UnsupportedVersion(2))
    ));

    let mut invalid_utf8 = original.clone();
    let source_id = find_first(&invalid_utf8, b"source");
    invalid_utf8[source_id] = 0xff;
    rewrite_checksum(&mut invalid_utf8);
    let error = open_error(root.path(), "invalid-utf8", &invalid_utf8);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::InvalidUtf8)
    ));

    let mut unknown_source = original.clone();
    let source_reference = find_last(&unknown_source, b"source");
    unknown_source[source_reference..source_reference + 6].copy_from_slice(b"ghost!");
    rewrite_checksum(&mut unknown_source);
    let error = open_error(root.path(), "unknown-source", &unknown_source);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::UnknownSource {
            stage,
            source_id,
        }) if stage == "count" && source_id == "ghost!"
    ));

    let mut unknown_operation = original.clone();
    let operation = find_first(&unknown_operation, OPERATION_MAGIC);
    let tag = operation + OPERATION_MAGIC.len() + size_of::<u16>();
    unknown_operation[tag..tag + 2].copy_from_slice(&99_u16.to_be_bytes());
    rewrite_checksum(&mut unknown_operation);
    let error = open_error(root.path(), "unknown-operation", &unknown_operation);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::Operation(
            DefinitionCodecError::UnknownTag(99)
        ))
    ));

    let mut truncated_operation = original;
    let operation = find_first(&truncated_operation, OPERATION_MAGIC);
    let operation_length = operation - size_of::<u32>();
    truncated_operation[operation_length..operation].copy_from_slice(&31_u32.to_be_bytes());
    rewrite_checksum(&mut truncated_operation);
    let error = open_error(root.path(), "truncated-operation", &truncated_operation);
    assert!(matches!(
        error,
        FlowError::Definition(FlowDefinitionError::Operation(
            DefinitionCodecError::Truncated
        ))
    ));
}

#[test]
fn open_never_panics_for_deterministic_malformed_and_mutated_definitions() {
    let root = tempfile::tempdir().unwrap();
    let original = fixture_bytes(V1_SEQUENCE_COUNT);
    let mut cases = Vec::new();

    for length in [
        0,
        1,
        FLOW_MAGIC.len() - 1,
        FLOW_MAGIC.len(),
        FLOW_MAGIC.len() + 1,
        21,
        22,
        original.len() - 5,
        original.len() - 4,
        original.len() - 1,
    ] {
        cases.push((format!("truncated-{length}"), original[..length].to_vec()));
    }

    let mutation_stride = (original.len() / 16).max(1);
    for index in (0..original.len()).step_by(mutation_stride) {
        let mut mutated = original.clone();
        mutated[index] ^= 0x80;
        cases.push((format!("bit-flip-{index}"), mutated));
    }

    for (name, encoded) in [
        ("zeros-64", vec![0; 64]),
        ("ones-64", vec![0xff; 64]),
        (
            "byte-cycle-257",
            (0_u8..=u8::MAX).chain(std::iter::once(0)).collect(),
        ),
    ] {
        cases.push((name.to_owned(), encoded));
    }

    for (index, (name, encoded)) in cases.into_iter().enumerate() {
        let path = root.path().join(format!("case-{index:03}"));
        publish_definition(&path, &encoded);
        let outcome = catch_unwind(|| Flow::open(&path));
        assert!(outcome.is_ok(), "Flow::open panicked for {name}");
        let Err(error) = outcome.unwrap() else {
            panic!("malformed definition {name} unexpectedly opened");
        };
        assert!(
            matches!(error, FlowError::Definition(_)),
            "malformed definition {name} returned non-definition error: {error:?}"
        );
    }
}

fn open_error(root: &std::path::Path, name: &str, encoded: &[u8]) -> FlowError {
    let path = root.join(name);
    publish_definition(&path, encoded);
    let Err(error) = Flow::open(path) else {
        panic!("mutated definition unexpectedly opened");
    };
    error
}

fn find_first(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("fixture contains marker")
}

fn find_last(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
        .expect("fixture contains marker")
}
