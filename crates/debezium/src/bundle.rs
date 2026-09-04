use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{Error, ErrorKind};

const BUNDLE_MANIFEST: &str = "MANIFEST";
const RUNTIME_RELEASE: &str = "runtime/release";
const DISTRIBUTION_MANIFEST: &str = "debezium/MANIFEST";
const DISTRIBUTION_CHECKSUMS: &str = "debezium/SHA256SUMS";
const DISTRIBUTION_LIBRARY: &str = "debezium/lib";
const JAVA_RUNTIME_VERSION: &str = "21.0.12.1+1";
const MAX_MANIFEST_BYTES: u64 = 1024;
const MAX_RELEASE_BYTES: u64 = 64 * 1024;
const MAX_CHECKSUM_BYTES: u64 = 1024 * 1024;
const MAX_JAR_BYTES: u64 = 512 * 1024 * 1024;
const REQUIRED_FILES: &[&str] = &[
    "runtime-sbom.json",
    "TEMURIN-NOTICE.md",
    "runtime/NOTICE",
    "runtime/bin/java",
    "runtime/lib/modules",
    "runtime/lib/security/cacerts",
    "runtime/lib/tzdb.dat",
    "runtime/legal/java.base/LICENSE",
    "debezium/bom.json",
    "debezium/THIRD-PARTY-NOTICES.md",
];
const REQUIRED_JARS: &[&str] = &[
    "dogpaddle-debezium-bridge.jar",
    "connect-api-4.3.0.jar",
    "connect-json-4.3.0.jar",
    "connect-runtime-4.3.0.jar",
    "debezium-embedded-3.6.2.Final.jar",
    "slf4j-simple-1.7.36.jar",
];
const EXPECTED_DISTRIBUTION_MANIFEST: &str = concat!(
    "dogpaddle.debezium.distribution=1\n",
    "bridge.protocol=1\n",
    "debezium.version=3.6.2.Final\n",
    "kafka.connect.version=4.3.0\n",
);

pub(crate) struct Bundle {
    root: PathBuf,
    classpath: String,
    jvm_library: PathBuf,
}

impl Bundle {
    pub(crate) fn open(root: &Path) -> Result<Self, Error> {
        let root = root.canonicalize().map_err(|_| {
            invalid(format!(
                "Debezium runtime bundle does not exist: {}",
                root.display()
            ))
        })?;
        if !root.is_dir() {
            return Err(invalid("Debezium runtime bundle root is not a directory"));
        }

        validate_bundle_manifest(&root)?;
        validate_runtime_release(&root)?;
        for relative in REQUIRED_FILES {
            required_regular_file(&root, relative)?;
        }
        let jvm_library = required_regular_file(
            &root,
            jvm_relative_path()?
                .to_str()
                .ok_or_else(|| invalid("bundled JVM library path is not valid UTF-8"))?,
        )?;
        let classpath = validate_distribution(&root)?;

        Ok(Self {
            root,
            classpath,
            jvm_library,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn classpath(&self) -> &str {
        &self.classpath
    }

    pub(crate) fn jvm_library(&self) -> &Path {
        &self.jvm_library
    }
}

fn validate_bundle_manifest(root: &Path) -> Result<(), Error> {
    let manifest = read_bounded_required(root, BUNDLE_MANIFEST, MAX_MANIFEST_BYTES)?;
    if manifest != expected_manifest()?.as_bytes() {
        return Err(invalid(
            "Debezium runtime bundle MANIFEST is not for this supported target and runtime",
        ));
    }
    Ok(())
}

pub(crate) fn expected_manifest() -> Result<String, Error> {
    Ok(format!(
        concat!(
            "dogpaddle.debezium.bundle=1\n",
            "target={}\n",
            "java.runtime.vendor=Eclipse Temurin\n",
            "java.runtime.version={}\n",
        ),
        target_triple()?,
        JAVA_RUNTIME_VERSION,
    ))
}

fn validate_runtime_release(root: &Path) -> Result<(), Error> {
    let contents = read_bounded_required(root, RUNTIME_RELEASE, MAX_RELEASE_BYTES)?;
    let contents = String::from_utf8(contents)
        .map_err(|_| invalid("bundled Java runtime release marker is not valid UTF-8"))?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let (key, quoted) = line
            .split_once('=')
            .ok_or_else(|| invalid("bundled Java runtime release marker is malformed"))?;
        let value = quoted
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or_else(|| invalid("bundled Java runtime release marker is malformed"))?;
        if key.is_empty() || values.insert(key, value).is_some() {
            return Err(invalid(
                "bundled Java runtime release marker has an invalid or duplicate key",
            ));
        }
    }

    let (os_name, os_arch, libc) = match target_triple()? {
        "x86_64-unknown-linux-gnu" => ("Linux", "x86_64", "gnu"),
        "aarch64-unknown-linux-gnu" => ("Linux", "aarch64", "gnu"),
        "x86_64-apple-darwin" => ("Darwin", "x86_64", "default"),
        "aarch64-apple-darwin" => ("Darwin", "aarch64", "default"),
        _ => unreachable!("target_triple returns only supported targets"),
    };
    for (key, expected) in [
        ("IMPLEMENTOR", "Eclipse Adoptium"),
        ("SEMANTIC_VERSION", JAVA_RUNTIME_VERSION),
        ("IMAGE_TYPE", "JRE"),
        ("JVM_VARIANT", "Hotspot"),
        ("OS_NAME", os_name),
        ("OS_ARCH", os_arch),
        ("LIBC", libc),
    ] {
        if values.get(key).copied() != Some(expected) {
            return Err(invalid(format!(
                "bundled Java runtime release marker has incompatible {key}"
            )));
        }
    }
    Ok(())
}

fn validate_distribution(root: &Path) -> Result<String, Error> {
    let manifest = read_bounded_required(root, DISTRIBUTION_MANIFEST, MAX_MANIFEST_BYTES)?;
    if manifest != EXPECTED_DISTRIBUTION_MANIFEST.as_bytes() {
        return Err(invalid(
            "Debezium distribution MANIFEST is not the supported pinned version",
        ));
    }

    let expected = read_distribution_checksums(root)?;
    let jars = read_distribution_jars(root)?;
    if jars.len() != expected.len() || !jars.keys().eq(expected.keys()) {
        return Err(invalid(
            "Debezium distribution JARs do not exactly match SHA256SUMS",
        ));
    }
    for required in REQUIRED_JARS {
        if !jars.contains_key(*required) {
            return Err(invalid(format!(
                "Debezium distribution is missing required JAR {required}"
            )));
        }
    }
    for (name, path) in &jars {
        if sha256(path)? != expected[name] {
            return Err(invalid(format!(
                "Debezium distribution checksum does not match for {name}"
            )));
        }
    }

    env::join_paths(jars.values())
        .map_err(|_| invalid("Debezium JAR paths cannot form a JVM classpath"))?
        .into_string()
        .map_err(|_| invalid("Debezium distribution path must be valid UTF-8"))
}

fn read_distribution_checksums(root: &Path) -> Result<BTreeMap<String, [u8; 32]>, Error> {
    let contents = read_bounded_required(root, DISTRIBUTION_CHECKSUMS, MAX_CHECKSUM_BYTES)?;
    let contents = String::from_utf8(contents)
        .map_err(|_| invalid("Debezium distribution SHA256SUMS is not valid UTF-8"))?;
    if !contents.ends_with('\n') {
        return Err(invalid(
            "Debezium distribution SHA256SUMS is not canonically terminated",
        ));
    }

    let mut checksums = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for line in contents.lines() {
        let (digest, name) = line
            .split_once("  lib/")
            .ok_or_else(|| invalid("Debezium distribution SHA256SUMS has invalid framing"))?;
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || Path::new(name)
                .extension()
                .is_none_or(|extension| extension != "jar")
        {
            return Err(invalid(
                "Debezium distribution SHA256SUMS has an invalid JAR path",
            ));
        }
        if previous.is_some_and(|previous| previous >= name) {
            return Err(invalid(
                "Debezium distribution SHA256SUMS paths are not strictly sorted",
            ));
        }
        previous = Some(name);
        checksums.insert(name.to_owned(), decode_sha256(digest)?);
    }
    if checksums.is_empty() {
        return Err(invalid("Debezium distribution SHA256SUMS contains no JARs"));
    }
    Ok(checksums)
}

fn read_distribution_jars(root: &Path) -> Result<BTreeMap<String, PathBuf>, Error> {
    let library = required_directory(root, DISTRIBUTION_LIBRARY)?;
    let mut jars = BTreeMap::new();
    for entry in fs::read_dir(&library)
        .map_err(|_| invalid("cannot read Debezium distribution lib directory"))?
    {
        let entry = entry.map_err(|_| invalid("cannot read a Debezium distribution entry"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| invalid("cannot inspect a Debezium distribution entry"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("Debezium JAR name must be valid UTF-8"))?;
        if !file_type.is_file()
            || Path::new(&name)
                .extension()
                .is_none_or(|extension| extension != "jar")
        {
            return Err(invalid(
                "Debezium distribution lib directory may contain only regular JAR files",
            ));
        }
        let metadata = entry
            .metadata()
            .map_err(|_| invalid("cannot inspect a Debezium JAR"))?;
        if metadata.len() == 0 || metadata.len() > MAX_JAR_BYTES {
            return Err(invalid(
                "Debezium distribution JARs must be non-empty bounded regular files",
            ));
        }
        if jars.insert(name, entry.path()).is_some() {
            return Err(invalid("Debezium distribution contains a duplicate JAR"));
        }
    }
    Ok(jars)
}

fn required_directory(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|_| invalid(format!("Debezium runtime bundle is missing {relative}")))?;
    if !metadata.file_type().is_dir() {
        return Err(invalid(format!(
            "Debezium runtime bundle entry is not a directory: {relative}"
        )));
    }
    let canonical = path.canonicalize().map_err(|_| {
        invalid(format!(
            "cannot resolve runtime bundle directory {relative}"
        ))
    })?;
    if !canonical.starts_with(root) {
        return Err(invalid(format!(
            "Debezium runtime bundle directory escapes its root: {relative}"
        )));
    }
    Ok(canonical)
}

fn required_regular_file(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(&path).map_err(|_| {
        invalid(format!(
            "Debezium runtime bundle is missing required file {relative}"
        ))
    })?;
    if !metadata.file_type().is_file() || metadata.len() == 0 {
        return Err(invalid(format!(
            "Debezium runtime bundle required file must be regular and non-empty: {relative}"
        )));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| invalid(format!("cannot resolve runtime bundle file {relative}")))?;
    if !canonical.starts_with(root) {
        return Err(invalid(format!(
            "Debezium runtime bundle file escapes its root: {relative}"
        )));
    }
    Ok(canonical)
}

fn read_bounded_required(root: &Path, relative: &str, maximum: u64) -> Result<Vec<u8>, Error> {
    let path = required_regular_file(root, relative)?;
    let file = File::open(&path)
        .map_err(|_| invalid(format!("cannot open runtime bundle file {relative}")))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid(format!("cannot inspect runtime bundle file {relative}")))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(invalid(format!(
            "runtime bundle file exceeds its size bound: {relative}"
        )));
    }
    let mut contents = Vec::with_capacity(
        usize::try_from(metadata.len())
            .map_err(|_| invalid(format!("runtime bundle file is too large: {relative}")))?,
    );
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|_| invalid(format!("cannot read runtime bundle file {relative}")))?;
    if u64::try_from(contents.len()).map_or(true, |length| length > maximum) {
        return Err(invalid(format!(
            "runtime bundle file grew beyond its size bound: {relative}"
        )));
    }
    Ok(contents)
}

fn target_triple() -> Result<&'static str, Error> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") if cfg!(target_env = "gnu") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") if cfg!(target_env = "gnu") => Ok("aarch64-unknown-linux-gnu"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        _ => Err(invalid(
            "embedded Debezium bundles support only x86_64/aarch64 Linux and macOS",
        )),
    }
}

fn jvm_relative_path() -> Result<&'static Path, Error> {
    match target_triple()? {
        "x86_64-unknown-linux-gnu" | "aarch64-unknown-linux-gnu" => {
            Ok(Path::new("runtime/lib/server/libjvm.so"))
        }
        "x86_64-apple-darwin" | "aarch64-apple-darwin" => {
            Ok(Path::new("runtime/lib/server/libjvm.dylib"))
        }
        _ => unreachable!("target_triple returns only supported targets"),
    }
}

fn sha256(path: &Path) -> Result<[u8; 32], Error> {
    let file = File::open(path).map_err(|_| invalid("cannot open a Debezium JAR"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("cannot inspect an opened Debezium JAR"))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_JAR_BYTES {
        return Err(invalid(
            "Debezium distribution JARs must remain non-empty bounded regular files",
        ));
    }
    let mut reader = file.take(MAX_JAR_BYTES.saturating_add(1));
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut total = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| invalid("cannot read a Debezium JAR"))?;
        if read == 0 {
            if total == 0 {
                return Err(invalid("Debezium distribution JAR became empty"));
            }
            return Ok(digest.finalize().into());
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| invalid("cannot size a Debezium JAR"))?)
            .ok_or_else(|| invalid("Debezium distribution JAR size overflowed"))?;
        if total > MAX_JAR_BYTES {
            return Err(invalid(
                "Debezium distribution JAR grew beyond its size bound",
            ));
        }
        digest.update(&buffer[..read]);
    }
}

fn decode_sha256(value: &str) -> Result<[u8; 32], Error> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "Debezium distribution SHA256SUMS contains an invalid digest",
        ));
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
        _ => Err(invalid(
            "Debezium distribution SHA256SUMS contains an invalid digest",
        )),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidBundle, message)
}
