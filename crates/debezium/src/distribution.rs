use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{Error, ErrorKind};

const BRIDGE_JAR: &str = "dogpaddle-debezium-bridge.jar";
const MANIFEST_FILE: &str = "MANIFEST";
const CHECKSUM_FILE: &str = "SHA256SUMS";
const MAX_MANIFEST_FILE_BYTES: u64 = EXPECTED_MANIFEST.len() as u64;
const MAX_CHECKSUM_FILE_BYTES: u64 = 1024 * 1024;
const EXPECTED_MANIFEST: &str = concat!(
    "dogpaddle.debezium.distribution=1\n",
    "bridge.protocol=1\n",
    "debezium.version=3.6.2.Final\n",
    "kafka.connect.version=4.3.0\n",
);
const REQUIRED_JARS: &[&str] = &[
    BRIDGE_JAR,
    "connect-api-4.3.0.jar",
    "connect-json-4.3.0.jar",
    "connect-runtime-4.3.0.jar",
    "debezium-embedded-3.6.2.Final.jar",
    "slf4j-simple-1.7.36.jar",
];

pub(crate) struct Distribution {
    classpath: String,
}

impl Distribution {
    pub(crate) fn open(root: &Path) -> Result<Self, Error> {
        let root = root.canonicalize().map_err(|_| {
            invalid(format!(
                "Debezium distribution does not exist: {}",
                root.display()
            ))
        })?;
        if !root.is_dir() {
            return Err(invalid("Debezium distribution root is not a directory"));
        }
        validate_manifest(&root)?;

        let library_dir = root.join("lib");
        if !library_dir.is_dir() {
            return Err(invalid(format!(
                "Debezium distribution has no lib directory: {}",
                library_dir.display()
            )));
        }

        let expected = read_checksums(&root)?;
        let jars = read_jars(&library_dir)?;
        let actual_names = jars
            .iter()
            .map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("Debezium JAR name must be valid UTF-8"))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let expected_names = expected.keys().cloned().collect::<BTreeSet<_>>();
        if actual_names != expected_names {
            return Err(invalid(
                "Debezium distribution JARs do not exactly match SHA256SUMS",
            ));
        }
        for required in REQUIRED_JARS {
            if !actual_names.contains(*required) {
                return Err(invalid(format!(
                    "Debezium distribution is missing required JAR {required}"
                )));
            }
        }
        for path in &jars {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid("Debezium JAR name must be valid UTF-8"))?;
            let expected_digest = expected
                .get(name)
                .ok_or_else(|| invalid("Debezium JAR has no checksum entry"))?;
            if &sha256(path)? != expected_digest {
                return Err(invalid(format!(
                    "Debezium distribution checksum does not match for {name}"
                )));
            }
        }

        let classpath = env::join_paths(&jars)
            .map_err(|_| invalid("Debezium JAR paths cannot form a JVM classpath"))?
            .into_string()
            .map_err(|_| invalid("Debezium distribution path must be valid UTF-8"))?;
        Ok(Self { classpath })
    }

    pub(crate) fn classpath(&self) -> &str {
        &self.classpath
    }
}

fn validate_manifest(root: &Path) -> Result<(), Error> {
    let manifest = read_bounded_regular_file(
        &root.join(MANIFEST_FILE),
        MAX_MANIFEST_FILE_BYTES,
        "Debezium distribution MANIFEST is missing",
        "Debezium distribution MANIFEST is invalid",
    )?;
    if manifest != EXPECTED_MANIFEST.as_bytes() {
        return Err(invalid(
            "Debezium distribution MANIFEST is not the supported pinned version",
        ));
    }
    Ok(())
}

fn read_checksums(root: &Path) -> Result<BTreeMap<String, [u8; 32]>, Error> {
    let path = root.join(CHECKSUM_FILE);
    let contents = read_bounded_regular_file(
        &path,
        MAX_CHECKSUM_FILE_BYTES,
        "Debezium distribution SHA256SUMS is missing",
        "Debezium distribution SHA256SUMS is invalid",
    )?;
    let contents = String::from_utf8(contents)
        .map_err(|_| invalid("Debezium distribution SHA256SUMS is not valid UTF-8"))?;
    if !contents.ends_with('\n') {
        return Err(invalid(
            "Debezium distribution SHA256SUMS is not canonically terminated",
        ));
    }
    let mut checksums = BTreeMap::new();
    for line in contents.lines() {
        let (digest, relative) = line
            .split_once("  lib/")
            .ok_or_else(|| invalid("Debezium SHA256SUMS has invalid framing"))?;
        if relative.is_empty()
            || relative.contains('/')
            || relative.contains('\\')
            || Path::new(relative)
                .extension()
                .is_none_or(|extension| extension != "jar")
        {
            return Err(invalid("Debezium SHA256SUMS has an invalid JAR path"));
        }
        let digest = decode_sha256(digest)?;
        if checksums.insert(relative.to_owned(), digest).is_some() {
            return Err(invalid("Debezium SHA256SUMS contains a duplicate JAR"));
        }
    }
    if checksums.is_empty() {
        return Err(invalid("Debezium SHA256SUMS contains no JARs"));
    }
    Ok(checksums)
}

fn read_bounded_regular_file(
    path: &Path,
    maximum_bytes: u64,
    missing_message: &str,
    invalid_message: &str,
) -> Result<Vec<u8>, Error> {
    // Reject a stable special file before opening it; opening a FIFO can block.
    // The opened descriptor is then inspected and read exactly once so normal
    // path replacement cannot make validation inspect one file and read another.
    let path_metadata =
        fs::symlink_metadata(path).map_err(|_| invalid(missing_message.to_owned()))?;
    if !path_metadata.file_type().is_file() || path_metadata.len() > maximum_bytes {
        return Err(invalid(invalid_message.to_owned()));
    }

    let file = File::open(path).map_err(|_| invalid(invalid_message.to_owned()))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid(invalid_message.to_owned()))?;
    if !metadata.is_file() || metadata.len() > maximum_bytes {
        return Err(invalid(invalid_message.to_owned()));
    }

    let capacity =
        usize::try_from(metadata.len()).map_err(|_| invalid(invalid_message.to_owned()))?;
    let mut contents = Vec::with_capacity(capacity);
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|_| invalid(invalid_message.to_owned()))?;
    if u64::try_from(contents.len()).map_or(true, |length| length > maximum_bytes) {
        return Err(invalid(invalid_message.to_owned()));
    }
    Ok(contents)
}

fn read_jars(library_dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut jars = Vec::new();
    for entry in fs::read_dir(library_dir)
        .map_err(|_| invalid("cannot read Debezium distribution lib directory"))?
    {
        let entry = entry.map_err(|_| invalid("cannot read a Debezium distribution entry"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| invalid("cannot inspect a Debezium distribution entry"))?;
        let path = entry.path();
        if !file_type.is_file() || path.extension().is_none_or(|extension| extension != "jar") {
            return Err(invalid(
                "Debezium distribution lib directory may contain only regular JAR files",
            ));
        }
        jars.push(path);
    }
    jars.sort();
    Ok(jars)
}

fn sha256(path: &Path) -> Result<[u8; 32], Error> {
    let mut file = File::open(path).map_err(|_| invalid("cannot open a Debezium JAR"))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| invalid("cannot read a Debezium JAR"))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], Error> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("Debezium SHA256SUMS contains an invalid digest"));
    }
    let mut decoded = [0_u8; 32];
    for (target, pair) in decoded.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *target = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Result<u8, Error> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid("Debezium SHA256SUMS contains an invalid digest")),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidDistribution, message)
}
