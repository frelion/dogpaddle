use std::fmt::Write as _;
use std::fs;

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::bundle::{Bundle, expected_manifest};
use crate::config::ConnectorConfig;
use crate::protocol::decode_delivery;
use crate::{Checkpoint, ErrorKind};

const FIXTURE_JARS: &[&str] = &[
    "connect-api-4.3.0.jar",
    "connect-json-4.3.0.jar",
    "connect-runtime-4.3.0.jar",
    "debezium-embedded-3.6.2.Final.jar",
    "dogpaddle-debezium-bridge.jar",
    "slf4j-simple-1.7.36.jar",
];

#[test]
fn bundle_opens_the_pinned_runtime_and_exact_jar_set() {
    let directory = fake_bundle();
    let bundle = Bundle::open(directory.path()).unwrap();

    assert_eq!(bundle.root(), directory.path().canonicalize().unwrap());
    assert!(bundle.classpath().contains("dogpaddle-debezium-bridge.jar"));
    assert!(bundle.jvm_library().ends_with(expected_jvm_relative_path()));
}

#[test]
fn bundle_manifest_and_runtime_release_are_compatibility_boundaries() {
    let directory = fake_bundle();
    let manifest = expected_manifest().unwrap().replace(
        "java.runtime.version=21.0.12.1+1",
        "java.runtime.version=21",
    );
    fs::write(directory.path().join("MANIFEST"), manifest).unwrap();

    let error = Bundle::open(directory.path()).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidBundle);
    assert!(error.to_string().contains("MANIFEST"));

    let directory = fake_bundle();
    fs::write(
        directory.path().join("runtime/release"),
        runtime_release().replace("21.0.12.1+1", "21"),
    )
    .unwrap();
    let error = Bundle::open(directory.path()).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidBundle);
    assert!(error.to_string().contains("SEMANTIC_VERSION"));
}

#[test]
fn bundle_requires_runtime_security_legal_and_distribution_evidence() {
    let required = [
        "runtime-sbom.json",
        "TEMURIN-NOTICE.md",
        "runtime/NOTICE",
        "runtime/release",
        "runtime/bin/java",
        "runtime/lib/modules",
        "runtime/lib/security/cacerts",
        "runtime/lib/tzdb.dat",
        "runtime/legal/java.base/LICENSE",
        "debezium/bom.json",
        "debezium/THIRD-PARTY-NOTICES.md",
    ];

    for relative in required {
        let directory = fake_bundle();
        fs::remove_file(directory.path().join(relative)).unwrap();

        let error = Bundle::open(directory.path()).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidBundle, "{relative}");
        assert!(error.to_string().contains(relative), "{relative}: {error}");
    }

    let empty = fake_bundle();
    fs::write(empty.path().join("runtime/NOTICE"), []).unwrap();
    let error = Bundle::open(empty.path()).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidBundle);
    assert!(error.to_string().contains("runtime/NOTICE"));
}

#[test]
fn resume_checkpoint_must_fit_inside_the_delivery_bound() {
    let config = ConnectorConfig::new("e", "c")
        .unwrap()
        .max_delivery_bytes(68)
        .unwrap();
    let bytes = checkpoint_bytes("e", "c", &[(b"key", &[0; 32])]);
    let checkpoint = Checkpoint::from_bytes(bytes).unwrap();

    let error = config
        .validate_delivery_bound(Some(&checkpoint))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
}

#[test]
fn bundle_rejects_a_corrupt_or_unlisted_distribution_jar() {
    let directory = fake_bundle();
    fs::write(
        directory
            .path()
            .join("debezium/lib/dogpaddle-debezium-bridge.jar"),
        b"corrupt",
    )
    .unwrap();
    assert!(Bundle::open(directory.path()).is_err());

    let directory = fake_bundle();
    fs::write(directory.path().join("debezium/lib/shadow.jar"), b"shadow").unwrap();
    assert!(Bundle::open(directory.path()).is_err());
}

#[test]
fn delivery_decoder_preserves_all_fields_and_source_order() {
    let checkpoint = checkpoint_bytes("engine", "connector", &[(b"offset", b"42")]);
    let records = [
        EncodedRecord {
            topic: Some("orders"),
            partition: Some(3),
            timestamp: Some(17),
            key: Some(br#"{"schema":{"type":"int32"},"payload":7}"#),
            value: Some(br#"{"schema":{"type":"string"},"payload":"first"}"#),
            headers: &[("trace", Some(br#"{"schema":null,"payload":"a"}"#))],
        },
        EncodedRecord {
            topic: None,
            partition: None,
            timestamp: None,
            key: None,
            value: Some(br#"{"schema":null,"payload":"second"}"#),
            headers: &[],
        },
    ];
    let bytes = delivery_bytes(&checkpoint, &records);

    let decoded = decode_delivery(&bytes, bytes.len()).unwrap();

    assert_eq!(decoded.checkpoint.as_bytes(), checkpoint);
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].topic(), Some("orders"));
    assert_eq!(decoded.records[0].kafka_partition(), Some(3));
    assert_eq!(decoded.records[0].timestamp(), Some(17));
    assert_eq!(decoded.records[0].key(), records[0].key);
    assert_eq!(decoded.records[0].value(), records[0].value);
    assert_eq!(decoded.records[0].headers()[0].key(), "trace");
    assert_eq!(
        decoded.records[0].headers()[0].value(),
        records[0].headers[0].1
    );
    assert_eq!(decoded.records[1].topic(), None);
    assert_eq!(decoded.records[1].kafka_partition(), None);
    assert_eq!(decoded.records[1].timestamp(), None);
    assert_eq!(decoded.records[1].key(), None);
    assert_eq!(decoded.records[1].value(), records[1].value);
}

#[test]
fn delivery_wire_matches_the_java_bridge_golden() {
    let bytes = decode_hex(concat!(
        "44504442445630310001000000334450444243503031000100000008656e67696e652d61",
        "000000116578616d706c652e436f6e6e6563746f720000000033b0f22100000002000000",
        "05746f706963010000000101000000000000000a0000003f7b22736368656d61223a7b22",
        "74797065223a22737472696e67222c226f7074696f6e616c223a66616c73657d2c227061",
        "796c6f6164223a226669727374227d0000003f7b22736368656d61223a7b227479706522",
        "3a22737472696e67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164",
        "223a226669727374227d0000000100000007617474656d7074000000387b22736368656d",
        "61223a7b2274797065223a22696e743332222c226f7074696f6e616c223a66616c73657d",
        "2c227061796c6f6164223a337d00000005746f706963010000000201000000000000000b",
        "000000407b22736368656d61223a7b2274797065223a22737472696e67222c226f707469",
        "6f6e616c223a66616c73657d2c227061796c6f6164223a227365636f6e64227d00000040",
        "7b22736368656d61223a7b2274797065223a22737472696e67222c226f7074696f6e616c",
        "223a66616c73657d2c227061796c6f6164223a227365636f6e64227d00000000f6e0afee",
    ));

    let decoded = decode_delivery(&bytes, bytes.len()).unwrap();

    assert_eq!(bytes.len(), 468);
    assert_eq!(
        decoded.checkpoint.as_bytes(),
        checkpoint_bytes("engine-a", "example.Connector", &[])
    );
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].topic(), Some("topic"));
    assert_eq!(decoded.records[0].kafka_partition(), Some(1));
    assert_eq!(decoded.records[0].timestamp(), Some(10));
    assert!(decoded.records[0].key().unwrap().ends_with(br#""first"}"#));
    assert_eq!(decoded.records[0].key(), decoded.records[0].value());
    assert_eq!(decoded.records[0].headers()[0].key(), "attempt");
    assert!(
        decoded.records[0].headers()[0]
            .value()
            .unwrap()
            .ends_with(br"3}")
    );
    assert_eq!(decoded.records[1].topic(), Some("topic"));
    assert_eq!(decoded.records[1].kafka_partition(), Some(2));
    assert_eq!(decoded.records[1].timestamp(), Some(11));
    assert!(decoded.records[1].key().unwrap().ends_with(br#""second"}"#));
    assert_eq!(decoded.records[1].key(), decoded.records[1].value());
    assert!(decoded.records[1].headers().is_empty());
}

#[test]
fn delivery_decoder_rejects_corruption_and_non_canonical_framing() {
    let checkpoint = checkpoint_bytes("engine", "connector", &[]);
    let record = EncodedRecord::empty();

    let mut corrupt = delivery_bytes(&checkpoint, &[record]);
    corrupt[12] ^= 1;
    assert_eq!(
        decode_delivery(&corrupt, corrupt.len()).unwrap_err().kind(),
        ErrorKind::Protocol
    );

    let mut trailing = delivery_bytes(&checkpoint, &[record]);
    trailing.truncate(trailing.len() - size_of::<u32>());
    trailing.push(0);
    append_checksum(&mut trailing);
    assert_eq!(
        decode_delivery(&trailing, trailing.len())
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );

    let empty = delivery_bytes(&checkpoint, &[]);
    assert_eq!(
        decode_delivery(&empty, empty.len()).unwrap_err().kind(),
        ErrorKind::Protocol
    );
}

#[test]
fn delivery_decoder_applies_bounds_before_copying_nested_frames() {
    let checkpoint = checkpoint_bytes("engine", "connector", &[]);
    let bytes = delivery_bytes(&checkpoint, &[EncodedRecord::empty()]);
    assert_eq!(
        decode_delivery(&bytes, bytes.len() - 1).unwrap_err().kind(),
        ErrorKind::DeliveryTooLarge
    );

    let mut oversized_checkpoint = Vec::new();
    oversized_checkpoint.extend_from_slice(b"DPDBDV01");
    oversized_checkpoint.extend_from_slice(&1_u16.to_be_bytes());
    oversized_checkpoint.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_be_bytes());
    append_checksum(&mut oversized_checkpoint);
    assert_eq!(
        decode_delivery(&oversized_checkpoint, oversized_checkpoint.len())
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );
}

fn write_fake_distribution(root: &std::path::Path) {
    let library = root.join("lib");
    fs::create_dir(&library).unwrap();
    fs::write(
        root.join("MANIFEST"),
        concat!(
            "dogpaddle.debezium.distribution=1\n",
            "bridge.protocol=1\n",
            "debezium.version=3.6.2.Final\n",
            "kafka.connect.version=4.3.0\n",
        ),
    )
    .unwrap();
    fs::write(root.join("bom.json"), b"{}\n").unwrap();
    fs::write(root.join("THIRD-PARTY-NOTICES.md"), b"fixture notices\n").unwrap();

    for name in FIXTURE_JARS {
        let contents = format!("fixture:{name}");
        fs::write(library.join(name), &contents).unwrap();
    }
    write_fake_checksums(root);
}

fn fake_bundle() -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir(root.join("debezium")).unwrap();
    fs::create_dir_all(root.join(expected_jvm_relative_path()).parent().unwrap()).unwrap();
    write_fake_distribution(&root.join("debezium"));
    fs::write(root.join("MANIFEST"), expected_manifest().unwrap()).unwrap();
    fs::write(root.join("runtime-sbom.json"), b"{}\n").unwrap();
    fs::write(root.join("TEMURIN-NOTICE.md"), b"fixture notice\n").unwrap();
    fs::write(root.join(expected_jvm_relative_path()), b"fixture JVM").unwrap();
    fs::create_dir_all(root.join("runtime/bin")).unwrap();
    fs::create_dir_all(root.join("runtime/lib/security")).unwrap();
    fs::create_dir_all(root.join("runtime/legal/java.base")).unwrap();
    fs::write(root.join("runtime/NOTICE"), b"fixture runtime notice\n").unwrap();
    fs::write(root.join("runtime/release"), runtime_release()).unwrap();
    fs::write(root.join("runtime/bin/java"), b"fixture Java launcher\n").unwrap();
    fs::write(root.join("runtime/lib/modules"), b"fixture module image\n").unwrap();
    fs::write(root.join("runtime/lib/security/cacerts"), b"fixture CAs\n").unwrap();
    fs::write(root.join("runtime/lib/tzdb.dat"), b"fixture time zones\n").unwrap();
    fs::write(
        root.join("runtime/legal/java.base/LICENSE"),
        b"fixture runtime license\n",
    )
    .unwrap();
    directory
}

fn runtime_release() -> String {
    let (os_name, os_arch, libc) = if cfg!(target_os = "macos") {
        (
            "Darwin",
            if cfg!(target_arch = "aarch64") {
                "aarch64"
            } else {
                "x86_64"
            },
            "default",
        )
    } else {
        (
            "Linux",
            if cfg!(target_arch = "aarch64") {
                "aarch64"
            } else {
                "x86_64"
            },
            "gnu",
        )
    };
    format!(
        concat!(
            "IMPLEMENTOR=\"Eclipse Adoptium\"\n",
            "SEMANTIC_VERSION=\"21.0.12.1+1\"\n",
            "IMAGE_TYPE=\"JRE\"\n",
            "JVM_VARIANT=\"Hotspot\"\n",
            "OS_NAME=\"{}\"\n",
            "OS_ARCH=\"{}\"\n",
            "LIBC=\"{}\"\n",
        ),
        os_name, os_arch, libc
    )
}

fn expected_jvm_relative_path() -> &'static std::path::Path {
    if cfg!(target_os = "macos") {
        std::path::Path::new("runtime/lib/server/libjvm.dylib")
    } else {
        std::path::Path::new("runtime/lib/server/libjvm.so")
    }
}

fn write_fake_checksums(root: &std::path::Path) {
    let mut checksums = String::new();
    for name in FIXTURE_JARS {
        let contents = fs::read(root.join("lib").join(name)).unwrap();
        let digest = Sha256::digest(contents);
        for byte in digest {
            write!(&mut checksums, "{byte:02x}").unwrap();
        }
        writeln!(&mut checksums, "  lib/{name}").unwrap();
    }
    fs::write(root.join("SHA256SUMS"), checksums).unwrap();
}

#[derive(Clone, Copy)]
struct EncodedRecord<'a> {
    topic: Option<&'a str>,
    partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    headers: &'a [(&'a str, Option<&'a [u8]>)],
}

impl EncodedRecord<'_> {
    const fn empty() -> Self {
        Self {
            topic: None,
            partition: None,
            timestamp: None,
            key: None,
            value: None,
            headers: &[],
        }
    }
}

fn delivery_bytes(checkpoint: &[u8], records: &[EncodedRecord<'_>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DPDBDV01");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    push_u32_bytes(&mut bytes, checkpoint);
    bytes.extend_from_slice(&u32::try_from(records.len()).unwrap().to_be_bytes());
    for record in records {
        push_nullable_bytes(&mut bytes, record.topic.map(str::as_bytes));
        push_optional_i32(&mut bytes, record.partition);
        push_optional_i64(&mut bytes, record.timestamp);
        push_nullable_bytes(&mut bytes, record.key);
        push_nullable_bytes(&mut bytes, record.value);
        bytes.extend_from_slice(&u32::try_from(record.headers.len()).unwrap().to_be_bytes());
        for (key, value) in record.headers {
            push_u32_bytes(&mut bytes, key.as_bytes());
            push_nullable_bytes(&mut bytes, *value);
        }
    }
    append_checksum(&mut bytes);
    bytes
}

fn checkpoint_bytes(engine: &str, connector: &str, entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DPDBCP01");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    push_u32_bytes(&mut bytes, engine.as_bytes());
    push_u32_bytes(&mut bytes, connector.as_bytes());
    bytes.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for (key, value) in entries {
        push_u32_bytes(&mut bytes, key);
        bytes.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(value);
    }
    append_checksum(&mut bytes);
    bytes
}

fn push_u32_bytes(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
    target.extend_from_slice(value);
}

fn push_nullable_bytes(target: &mut Vec<u8>, value: Option<&[u8]>) {
    if let Some(value) = value {
        target.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        target.extend_from_slice(value);
    } else {
        target.extend_from_slice(&(-1_i32).to_be_bytes());
    }
}

fn push_optional_i32(target: &mut Vec<u8>, value: Option<i32>) {
    if let Some(value) = value {
        target.push(1);
        target.extend_from_slice(&value.to_be_bytes());
    } else {
        target.push(0);
    }
}

fn push_optional_i64(target: &mut Vec<u8>, value: Option<i64>) {
    if let Some(value) = value {
        target.push(1);
        target.extend_from_slice(&value.to_be_bytes());
    } else {
        target.push(0);
    }
}

fn append_checksum(target: &mut Vec<u8>) {
    target.extend_from_slice(&crc32fast::hash(target).to_be_bytes());
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

const fn hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => panic!("invalid hex fixture"),
    }
}
