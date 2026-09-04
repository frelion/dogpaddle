use std::fmt::Write as _;
use std::fs;

use dogpaddle_debezium::{Checkpoint, ConnectorConfig, DebeziumRuntime, ErrorKind};
use tempfile::tempdir;

#[test]
fn connector_configuration_redacts_values_and_rejects_runtime_owned_keys() {
    let config = ConnectorConfig::new("orders", "example.Connector")
        .unwrap()
        .property("database.password", "super-secret")
        .unwrap();

    let debug = format!("{config:?}");
    assert!(debug.contains("database.password"));
    assert!(!debug.contains("super-secret"));

    for key in [
        "offset.storage",
        "offset.storage.file.filename",
        "offset.commit.policy",
        "offset.flush.interval.ms",
        "tasks.max",
        "record.processing.order",
        "transforms",
        "transforms.unwrap.type",
        "predicates.only_table.type",
        "key.converter.schemas.enable",
        "value.converter",
        "header.converter",
        "dogpaddle.runtime.id",
    ] {
        let error = ConnectorConfig::new("orders", "example.Connector")
            .unwrap()
            .property(key, "forbidden")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration, "{key}");
        assert!(!error.to_string().contains("forbidden"), "{key}");
    }
}

#[test]
fn connector_configuration_enforces_checkpoint_and_java_delivery_bounds() {
    let maximum_binding = "x".repeat(1024 * 1024);
    assert!(ConnectorConfig::new(&maximum_binding, "connector").is_ok());

    let excessive_binding = format!("{maximum_binding}x");
    assert_eq!(
        ConnectorConfig::new(excessive_binding, "connector")
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidConfiguration
    );

    assert!(
        ConnectorConfig::new("e", "c")
            .unwrap()
            .max_delivery_bytes(67)
            .is_err()
    );
    assert!(
        ConnectorConfig::new("e", "c")
            .unwrap()
            .max_delivery_bytes(68)
            .is_ok()
    );
    assert!(
        ConnectorConfig::new("e", "c")
            .unwrap()
            .max_delivery_bytes(i32::MAX as usize)
            .is_ok()
    );
}

#[test]
fn checkpoint_validation_is_complete_and_debug_is_opaque() {
    let bytes = checkpoint_bytes(
        "orders",
        "example.Connector",
        &[(b"a", Some(b"one")), (b"b", Some(b"two"))],
    );
    let checkpoint = Checkpoint::from_bytes(bytes.clone()).unwrap();

    assert_eq!(checkpoint.as_bytes(), bytes);
    let debug = format!("{checkpoint:?}");
    assert!(debug.contains("Checkpoint"));
    assert!(!debug.contains("one"));

    let mut corrupt = bytes;
    corrupt[12] ^= 1;
    let error = Checkpoint::from_bytes(corrupt).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidCheckpoint);
}

#[test]
fn checkpoint_wire_matches_the_java_bridge_golden() {
    let bytes = checkpoint_bytes(
        "engine-a",
        "connector.A",
        &[(b"\x00\xff", Some(b"\x01\x02")), (b"z", Some(b"\x03"))],
    );

    assert_eq!(
        hex(&bytes),
        concat!(
            "4450444243503031000100000008656e67696e652d61",
            "0000000b636f6e6e6563746f722e4100000002",
            "0000000200ff000000020102000000017a00000001031ef7d5c2"
        )
    );
    assert_eq!(
        Checkpoint::from_bytes(bytes.clone()).unwrap().as_bytes(),
        bytes
    );
}

#[test]
fn checkpoint_rejects_unsorted_or_duplicate_raw_keys() {
    for entries in [
        vec![
            (b"b".as_slice(), Some(b"one".as_slice())),
            (b"a", Some(b"two".as_slice())),
        ],
        vec![
            (b"a".as_slice(), Some(b"one".as_slice())),
            (b"a", Some(b"two".as_slice())),
        ],
    ] {
        let error =
            Checkpoint::from_bytes(checkpoint_bytes("orders", "example.Connector", &entries))
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidCheckpoint);
    }
}

#[test]
fn checkpoint_rejects_a_tombstone_in_a_complete_image() {
    let bytes = checkpoint_bytes("orders", "example.Connector", &[(b"a", None)]);

    assert_eq!(
        Checkpoint::from_bytes(bytes).unwrap_err().kind(),
        ErrorKind::InvalidCheckpoint
    );
}

#[test]
fn checkpoint_rejects_non_canonical_bounds_and_blank_bindings() {
    let blank = checkpoint_bytes(" ", "example.Connector", &[]);
    assert_eq!(
        Checkpoint::from_bytes(blank).unwrap_err().kind(),
        ErrorKind::InvalidCheckpoint
    );

    let excessive_entries = checkpoint_bytes_with_count("orders", "example.Connector", 1_000_001);
    assert_eq!(
        Checkpoint::from_bytes(excessive_entries)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidCheckpoint
    );
}

#[test]
fn runtime_rejects_a_legacy_jar_directory_without_a_bundled_jvm() {
    let directory = tempdir().unwrap();
    fs::create_dir(directory.path().join("lib")).unwrap();

    let error = DebeziumRuntime::open(directory.path()).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidBundle);
    assert!(error.to_string().contains("MANIFEST"));
}

fn checkpoint_bytes(
    engine_name: &str,
    connector_class: &str,
    entries: &[(&[u8], Option<&[u8]>)],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DPDBCP01");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    push_u32_bytes(&mut bytes, engine_name.as_bytes());
    push_u32_bytes(&mut bytes, connector_class.as_bytes());
    bytes.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for (key, value) in entries {
        push_u32_bytes(&mut bytes, key);
        match value {
            Some(value) => {
                bytes.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
                bytes.extend_from_slice(value);
            }
            None => bytes.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    bytes
}

fn checkpoint_bytes_with_count(
    engine_name: &str,
    connector_class: &str,
    entry_count: u32,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DPDBCP01");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    push_u32_bytes(&mut bytes, engine_name.as_bytes());
    push_u32_bytes(&mut bytes, connector_class.as_bytes());
    bytes.extend_from_slice(&entry_count.to_be_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    bytes
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn push_u32_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    target.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
    target.extend_from_slice(bytes);
}
