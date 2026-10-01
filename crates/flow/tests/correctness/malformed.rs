use std::{error::Error, panic::catch_unwind};

use dogpaddle_flow::{FlowDefinitionError, FlowError, FlowFactory};
use dogpaddle_operation::{
    col,
    operation::{
        scan::SequenceScanDefinition, sink::DiscardDefinition, transform::SelectDefinition,
    },
};

use super::support::{
    fixture_bytes, publish_definition, read_published_definition, rewrite_checksum,
};

const FLOW_MAGIC: &[u8] = b"dogpaddle.flow\0";
const HEADER: usize = FLOW_MAGIC.len() + 2;
const GOLDEN: &str = include_str!("../fixtures/v1/sequence_scan_running_event_count_discard.hex");

fn frame(json: &str) -> Vec<u8> {
    let mut bytes = FLOW_MAGIC.to_vec();
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(json.as_bytes());
    bytes.extend_from_slice(&[0; 4]);
    rewrite_checksum(&mut bytes);
    bytes
}

fn payload(bytes: &[u8]) -> &str {
    std::str::from_utf8(&bytes[HEADER..bytes.len() - 4]).unwrap()
}

fn definition_error(root: &std::path::Path, name: &str, encoded: &[u8]) -> FlowDefinitionError {
    let path = root.join(name);
    publish_definition(&path, encoded);
    let Err(FlowError::Definition(error)) = FlowFactory::new(&path).open() else {
        panic!("mutated definition did not return a definition error");
    };
    assert_eq!(read_published_definition(&path), encoded);
    error
}

#[test]
fn open_checks_integrity_before_json_and_reports_only_safe_categories() {
    let root = tempfile::tempdir().unwrap();
    let original = fixture_bytes(GOLDEN);
    let mut bad_magic = original.clone();
    bad_magic[0] ^= 1;
    assert_eq!(
        definition_error(root.path(), "magic", &bad_magic),
        FlowDefinitionError::InvalidMagic
    );
    let mut bad_crc = original.clone();
    bad_crc[HEADER] = 0xff;
    assert_eq!(
        definition_error(root.path(), "crc", &bad_crc),
        FlowDefinitionError::IntegrityMismatch
    );
    let mut version = original.clone();
    version[FLOW_MAGIC.len()..HEADER].copy_from_slice(&2_u16.to_be_bytes());
    rewrite_checksum(&mut version);
    assert_eq!(
        definition_error(root.path(), "version", &version),
        FlowDefinitionError::UnsupportedVersion(2)
    );
    for (index, json) in [
        "{", "{}", "[]", "null", r#"{"owner_identity":2,"operations":[]}"#,
        r#"{"owner_identity":null,"operations":[{"id":"secret-id","definition":{"secret-variant":{}},"inputs":[]}]}"#,
        r#"{"owner_identity":null,"operations":[],"secret-field":"secret-value"}"#,
    ].into_iter().enumerate() {
        let error = definition_error(root.path(), &format!("json-{index}"), &frame(json));
        assert!(matches!(error, FlowDefinitionError::InvalidJson { .. }));
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
        assert!(error.source().is_none());
    }
    let mut utf8 = original.clone();
    utf8[HEADER] = 0xff;
    rewrite_checksum(&mut utf8);
    assert!(matches!(
        definition_error(root.path(), "utf8", &utf8),
        FlowDefinitionError::InvalidJson { .. }
    ));
}

#[test]
fn open_rejects_noncanonical_json_and_extra_values_without_rewriting() {
    let root = tempfile::tempdir().unwrap();
    let original = fixture_bytes(GOLDEN);
    let json = payload(&original);
    for (index, forged) in [
        format!(" {json}"),
        format!("{json} "),
        json.replacen("\"owner_identity\":null,", "", 1),
        json.replacen(
            "\"owner_identity\":null",
            "\"owner_identity\":null,\"owner_identity\":null",
            1,
        ),
        format!("{json}null"),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(matches!(
            definition_error(root.path(), &format!("canonical-{index}"), &frame(&forged)),
            FlowDefinitionError::NonCanonical | FlowDefinitionError::InvalidJson { .. }
        ));
    }
}

#[test]
fn metadata_duplicates_order_and_absent_overrides_are_checked_by_the_whole_plan() {
    use arrow_schema::Metadata;
    use dogpaddle_operation::operation::transform::SelectField;
    let root = tempfile::tempdir().unwrap();
    let mut factory = FlowFactory::new(root.path().join("valid"));
    let source = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let selected = factory.operation(
        "select",
        SelectDefinition::try_new([SelectField {
            name: "value".into(),
            expression: col("value"),
            nullable: Some(true),
            metadata: Some(Metadata::from([("a", "first"), ("z", "last")])),
        }])
        .unwrap()
        .with_metadata(Metadata::from([("owner", "test"), ("version", "1")])),
        [source],
    );
    factory.operation("sink", DiscardDefinition::new(), [selected]);
    drop(factory.build().unwrap());
    let original = read_published_definition(&root.path().join("valid"));
    let json = payload(&original);
    for (index, (needle, replacement)) in [
        (r#""owner":"test""#, r#""owner":"other","owner":"test""#),
        (r#""a":"first""#, r#""a":"other","a":"first""#),
        (r#""a":"first","z":"last""#, r#""z":"last","a":"first""#),
        (
            r#""owner":"test","version":"1""#,
            r#""version":"1","owner":"test""#,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let forged = json.replace(needle, replacement);
        assert_ne!(forged, json);
        assert_eq!(
            definition_error(root.path(), &format!("metadata-{index}"), &frame(&forged)),
            FlowDefinitionError::NonCanonical
        );
    }
    let mut ordinary_factory = FlowFactory::new(root.path().join("ordinary"));
    let source = ordinary_factory.operation("s", SequenceScanDefinition::new(0), []);
    let selected = ordinary_factory.operation(
        "p",
        SelectDefinition::try_new([("value", col("value"))]).unwrap(),
        [source],
    );
    ordinary_factory.operation("d", DiscardDefinition::new(), [selected]);
    drop(ordinary_factory.build().unwrap());
    let ordinary = read_published_definition(&root.path().join("ordinary"));
    let ordinary = payload(&ordinary);
    for (index, forged) in [
        ordinary.replacen("\"}]}}", "\",\"nullable\":null}]}}", 1),
        ordinary.replacen("\"}]}}", "\",\"metadata\":null}]}}", 1),
        ordinary.replacen("]}}", "],\"metadata\":null}}", 1),
    ]
    .into_iter()
    .enumerate()
    {
        assert_ne!(forged, ordinary);
        assert_eq!(
            definition_error(root.path(), &format!("null-{index}"), &frame(&forged)),
            FlowDefinitionError::NonCanonical
        );
    }
    // Explicit empty metadata remains a distinct plan and clears inherited metadata.
    let empty = json
        .replace(r#"{"a":"first","z":"last"}"#, "{}")
        .replace(r#"{"owner":"test","version":"1"}"#, "{}");
    let empty_path = root.path().join("empty");
    publish_definition(&empty_path, &frame(&empty));
    assert!(matches!(
        FlowFactory::new(&empty_path).open(),
        Err(FlowError::MissingResource { .. })
    ));
}

#[test]
fn persisted_inputs_must_precede_their_consumers_even_in_an_acyclic_graph() {
    let root = tempfile::tempdir().unwrap();
    let forward = frame(
        r#"{"owner_identity":null,"operations":[{"id":"count","definition":{"running_event_count":{}},"inputs":[1]},{"id":"scan","definition":{"sequence_scan":{"start":7}},"inputs":[]},{"id":"sink","definition":{"discard":{}},"inputs":[0]}]}"#,
    );
    assert_eq!(
        definition_error(root.path(), "forward", &forward),
        FlowDefinitionError::Topology(dogpaddle_flow::TopologyError::InputNotEarlier {
            operation: "count".into(),
            input: 1
        })
    );
    let original = fixture_bytes(GOLDEN);
    for input in [2, 99] {
        let forged =
            payload(&original).replace(r#""inputs":[1]"#, &format!(r#""inputs":[{input}]"#));
        assert_eq!(
            definition_error(root.path(), &format!("input-{input}"), &frame(&forged)),
            FlowDefinitionError::Topology(dogpaddle_flow::TopologyError::InputNotEarlier {
                operation: "sink".into(),
                input
            })
        );
    }
}

#[test]
fn open_never_panics_for_truncated_or_mutated_definitions() {
    let root = tempfile::tempdir().unwrap();
    let original = fixture_bytes(GOLDEN);
    for length in 0..original.len() {
        let bytes = &original[..length];
        assert!(
            catch_unwind(|| definition_error(root.path(), &format!("prefix-{length}"), bytes))
                .is_ok()
        );
    }
    let mut state = 0xbb67_ae85_84ca_a73b_u64;
    for length in 0..=128 {
        let mut bytes = frame("");
        bytes.truncate(HEADER);
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(state.to_le_bytes()[0]);
        }
        bytes.extend_from_slice(&[0; 4]);
        rewrite_checksum(&mut bytes);
        assert!(
            catch_unwind(|| definition_error(root.path(), &format!("random-{length}"), &bytes))
                .is_ok()
        );
    }
}

#[test]
fn old_binary_definition_is_rejected_without_rewriting_it() {
    let root = tempfile::tempdir().unwrap();
    // Exact former valid sequence -> count -> discard persistent golden.
    let bytes = fixture_bytes(
        "646f67706164646c652e666c6f770000010000000003000000047363616e00000033646f67706164646c652e6f7065726174696f6e0000017b2273657175656e63655f7363616e223a7b227374617274223a377d7d0000000000000005636f756e7400000030646f67706164646c652e6f7065726174696f6e0000017b2272756e6e696e675f6576656e745f636f756e74223a7b7d7d00000001000000000000000473696e6b00000024646f67706164646c652e6f7065726174696f6e0000017b2264697363617264223a7b7d7d0000000100000001cb7bd0c7",
    );
    assert!(matches!(
        definition_error(root.path(), "old", &bytes),
        FlowDefinitionError::InvalidJson { .. }
    ));
}

#[test]
fn oversized_definition_is_rejected_before_decoding_without_rewriting_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let bytes = vec![0; 9 * 1024 * 1024];
    publish_definition(&path, &bytes);
    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::Store(_))
    ));
    assert_eq!(read_published_definition(&path), bytes);
}

#[test]
fn fresh_build_checks_the_complete_encoded_size_without_a_decode_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.operation("scan", SequenceScanDefinition::new(0), []);
    let select = factory.operation(
        "select",
        SelectDefinition::try_new([("value", dogpaddle_operation::col("value"))])
            .unwrap()
            .with_metadata([("size".to_owned(), "x".repeat(8 * 1024 * 1024))]),
        [scan],
    );
    factory.operation("sink", DiscardDefinition::new(), [select]);
    assert!(matches!(
        factory.build(),
        Err(FlowError::Definition(FlowDefinitionError::LengthOverflow(
            "definition"
        )))
    ));
    assert!(!path.exists());
}
