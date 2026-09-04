use std::fmt::Write as _;
use std::fs;

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::bundle::{Bundle, expected_manifest};
use crate::config::ConnectorConfig;
use crate::distribution::Distribution;
use crate::jvm::decode_status;
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
fn bundle_requires_the_exact_checksum_verified_file_set() {
    let directory = fake_bundle();
    let bundle = Bundle::open(directory.path()).unwrap();

    assert_eq!(bundle.root(), directory.path().canonicalize().unwrap());
    assert!(
        bundle
            .distribution()
            .classpath()
            .contains("dogpaddle-debezium-bridge.jar")
    );
    assert!(bundle.jvm_library().ends_with(expected_jvm_relative_path()));

    fs::write(
        directory.path().join(expected_jvm_relative_path()),
        b"corrupt JVM",
    )
    .unwrap();
    assert_eq!(
        Bundle::open(directory.path()).err().unwrap().kind(),
        ErrorKind::InvalidDistribution
    );
}

#[test]
fn bundle_rejects_unlisted_and_non_regular_entries() {
    let unlisted = fake_bundle();
    fs::write(unlisted.path().join("runtime/unlisted"), b"unlisted").unwrap();
    assert!(Bundle::open(unlisted.path()).is_err());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let linked = fake_bundle();
        let library = linked.path().join(expected_jvm_relative_path());
        fs::rename(&library, linked.path().join("runtime/real-libjvm")).unwrap();
        symlink("../../real-libjvm", &library).unwrap();
        assert!(Bundle::open(linked.path()).is_err());
    }
}

#[test]
fn bundle_manifest_is_bound_to_the_current_target_and_temurin_release() {
    let directory = fake_bundle();
    let manifest = expected_manifest().unwrap().replace(
        "java.runtime.version=21.0.12.1+1",
        "java.runtime.version=21",
    );
    fs::write(directory.path().join("MANIFEST"), manifest).unwrap();
    write_bundle_checksums(directory.path());

    let error = Bundle::open(directory.path()).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidDistribution);
    assert!(error.to_string().contains("MANIFEST"));
}

#[test]
fn bundle_checksums_reject_non_canonical_paths_and_order() {
    let traversal = fake_bundle();
    fs::write(
        traversal.path().join("SHA256SUMS"),
        concat!(
            "0000000000000000000000000000000000000000000000000000000000000000",
            "  runtime/../MANIFEST\n",
        ),
    )
    .unwrap();
    assert!(Bundle::open(traversal.path()).is_err());

    let unsorted = fake_bundle();
    let mut lines = fs::read_to_string(unsorted.path().join("SHA256SUMS"))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    lines.swap(0, 1);
    fs::write(
        unsorted.path().join("SHA256SUMS"),
        format!("{}\n", lines.join("\n")),
    )
    .unwrap();
    assert!(Bundle::open(unsorted.path()).is_err());
}

#[test]
fn bundle_fingerprint_tracks_the_runtime_and_debezium_together() {
    let directory = fake_bundle();
    let original = *Bundle::open(directory.path()).unwrap().fingerprint();

    fs::write(
        directory.path().join(expected_jvm_relative_path()),
        b"replacement JVM",
    )
    .unwrap();
    write_bundle_checksums(directory.path());

    let replacement = *Bundle::open(directory.path()).unwrap().fingerprint();
    assert_ne!(original, replacement);
}

#[test]
fn bundle_accepts_an_exactly_checksummed_optional_binary_directory() {
    let directory = fake_bundle();
    fs::create_dir(directory.path().join("bin")).unwrap();
    fs::write(directory.path().join("bin/dogpaddle"), b"fixture host").unwrap();
    write_bundle_checksums(directory.path());

    assert!(Bundle::open(directory.path()).is_ok());
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
        write_bundle_checksums(directory.path());

        let error = Bundle::open(directory.path()).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidDistribution, "{relative}");
        assert!(error.to_string().contains(relative), "{relative}: {error}");
    }

    let empty = fake_bundle();
    fs::write(empty.path().join("runtime/NOTICE"), []).unwrap();
    write_bundle_checksums(empty.path());
    let error = Bundle::open(empty.path()).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidDistribution);
    assert!(error.to_string().contains("empty: runtime/NOTICE"));
}

#[test]
fn resume_checkpoint_must_fit_inside_the_delivery_bound() {
    let config = ConnectorConfig::new("e", "c")
        .unwrap()
        .max_delivery_bytes(76)
        .unwrap();
    let bytes = checkpoint_bytes("e", "c", &[(b"key", &[0; 32])]);
    let checkpoint = Checkpoint::from_bytes(bytes).unwrap();

    let error = config
        .validate_delivery_bound(Some(&checkpoint))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
}

#[test]
fn distribution_requires_an_exact_checksum_verified_jar_set() {
    let directory = fake_distribution();
    let distribution = Distribution::open(directory.path()).unwrap();
    assert!(
        distribution
            .classpath()
            .contains("dogpaddle-debezium-bridge.jar")
    );

    fs::write(
        directory.path().join("lib/dogpaddle-debezium-bridge.jar"),
        b"corrupt",
    )
    .unwrap();
    assert!(Distribution::open(directory.path()).is_err());
}

#[test]
fn distribution_rejects_an_unlisted_jar_before_jvm_startup() {
    let directory = fake_distribution();
    fs::write(directory.path().join("lib/shadow.jar"), b"shadow").unwrap();

    assert!(Distribution::open(directory.path()).is_err());
}

#[test]
fn distribution_rejects_non_regular_or_oversized_metadata_files() {
    let oversized = fake_distribution();
    fs::write(oversized.path().join("MANIFEST"), vec![b'x'; 1024 * 1024]).unwrap();
    assert!(Distribution::open(oversized.path()).is_err());

    let non_regular = fake_distribution();
    let manifest = non_regular.path().join("MANIFEST");
    fs::remove_file(&manifest).unwrap();
    fs::create_dir(&manifest).unwrap();
    assert!(Distribution::open(non_regular.path()).is_err());
}

#[test]
fn bridge_status_maps_only_the_stable_delivery_size_code() {
    let too_large = decode_status(
        br#"{"protocol":1,"kind":"status","state":"failed","failure_kind":"delivery_too_large"}"#,
    )
    .unwrap();
    assert_eq!(
        too_large.reported_error_kind(ErrorKind::ConnectorFailed),
        ErrorKind::DeliveryTooLarge
    );

    let ordinary =
        decode_status(br#"{"protocol":1,"kind":"status","state":"failed","failure_kind":null}"#)
            .unwrap();
    assert_eq!(
        ordinary.reported_error_kind(ErrorKind::ConnectorFailed),
        ErrorKind::ConnectorFailed
    );
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
    let bytes = delivery_bytes(9, &checkpoint, &records);

    let decoded = decode_delivery(&bytes, bytes.len()).unwrap();

    assert_eq!(decoded.token, 9);
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
        "44504442445630310001000000000000002a0000003344504442435030310001",
        "00000008656e67696e652d61000000116578616d706c652e436f6e6e656374",
        "6f720000000033b0f2210000000200000005746f7069630100000001010000",
        "00000000000a0000003f7b22736368656d61223a7b2274797065223a227374",
        "72696e67222c226f7074696f6e616c223a66616c73657d2c227061796c6f61",
        "64223a226669727374227d0000003f7b22736368656d61223a7b2274797065",
        "223a22737472696e67222c226f7074696f6e616c223a66616c73657d2c2270",
        "61796c6f6164223a226669727374227d0000000100000007617474656d7074",
        "000000387b22736368656d61223a7b2274797065223a22696e743332222c22",
        "6f7074696f6e616c223a66616c73657d2c227061796c6f6164223a337d0000",
        "0005746f706963010000000201000000000000000b000000407b2273636865",
        "6d61223a7b2274797065223a22737472696e67222c226f7074696f6e616c22",
        "3a66616c73657d2c227061796c6f6164223a227365636f6e64227d00000040",
        "7b22736368656d61223a7b2274797065223a22737472696e67222c226f7074",
        "696f6e616c223a66616c73657d2c227061796c6f6164223a227365636f6e",
        "64227d00000000903264ff",
    ));

    let decoded = decode_delivery(&bytes, bytes.len()).unwrap();

    assert_eq!(bytes.len(), 476);
    assert_eq!(decoded.token, 42);
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

    let mut corrupt = delivery_bytes(1, &checkpoint, &[record]);
    corrupt[12] ^= 1;
    assert_eq!(
        decode_delivery(&corrupt, corrupt.len()).unwrap_err().kind(),
        ErrorKind::Protocol
    );

    let mut trailing = delivery_bytes(1, &checkpoint, &[record]);
    trailing.truncate(trailing.len() - size_of::<u32>());
    trailing.push(0);
    append_checksum(&mut trailing);
    assert_eq!(
        decode_delivery(&trailing, trailing.len())
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );

    let empty = delivery_bytes(1, &checkpoint, &[]);
    assert_eq!(
        decode_delivery(&empty, empty.len()).unwrap_err().kind(),
        ErrorKind::Protocol
    );
}

#[test]
fn delivery_decoder_applies_bounds_before_copying_nested_frames() {
    let checkpoint = checkpoint_bytes("engine", "connector", &[]);
    let bytes = delivery_bytes(1, &checkpoint, &[EncodedRecord::empty()]);
    assert_eq!(
        decode_delivery(&bytes, bytes.len() - 1).unwrap_err().kind(),
        ErrorKind::DeliveryTooLarge
    );

    let mut oversized_checkpoint = Vec::new();
    oversized_checkpoint.extend_from_slice(b"DPDBDV01");
    oversized_checkpoint.extend_from_slice(&1_u16.to_be_bytes());
    oversized_checkpoint.extend_from_slice(&1_i64.to_be_bytes());
    oversized_checkpoint.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_be_bytes());
    append_checksum(&mut oversized_checkpoint);
    assert_eq!(
        decode_delivery(&oversized_checkpoint, oversized_checkpoint.len())
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );
}

fn fake_distribution() -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    write_fake_distribution(directory.path());
    directory
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
    fs::write(root.join("runtime/release"), b"JAVA_VERSION=fixture\n").unwrap();
    fs::write(root.join("runtime/bin/java"), b"fixture Java launcher\n").unwrap();
    fs::write(root.join("runtime/lib/modules"), b"fixture module image\n").unwrap();
    fs::write(root.join("runtime/lib/security/cacerts"), b"fixture CAs\n").unwrap();
    fs::write(root.join("runtime/lib/tzdb.dat"), b"fixture time zones\n").unwrap();
    fs::write(
        root.join("runtime/legal/java.base/LICENSE"),
        b"fixture runtime license\n",
    )
    .unwrap();
    write_bundle_checksums(root);
    directory
}

fn expected_jvm_relative_path() -> &'static std::path::Path {
    if cfg!(target_os = "macos") {
        std::path::Path::new("runtime/lib/server/libjvm.dylib")
    } else {
        std::path::Path::new("runtime/lib/server/libjvm.so")
    }
}

fn write_bundle_checksums(root: &std::path::Path) {
    let mut paths = Vec::new();
    collect_fixture_files(root, root, &mut paths);
    paths.sort();
    let mut checksums = String::new();
    for relative in paths {
        if relative == "SHA256SUMS" {
            continue;
        }
        let digest = Sha256::digest(fs::read(root.join(&relative)).unwrap());
        for byte in digest {
            write!(&mut checksums, "{byte:02x}").unwrap();
        }
        writeln!(&mut checksums, "  {relative}").unwrap();
    }
    fs::write(root.join("SHA256SUMS"), checksums).unwrap();
}

fn collect_fixture_files(
    root: &std::path::Path,
    directory: &std::path::Path,
    paths: &mut Vec<String>,
) {
    for entry in fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            collect_fixture_files(root, &entry.path(), paths);
        } else {
            paths.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(std::path::MAIN_SEPARATOR, "/"),
            );
        }
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

fn delivery_bytes(token: i64, checkpoint: &[u8], records: &[EncodedRecord<'_>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DPDBDV01");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&token.to_be_bytes());
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
