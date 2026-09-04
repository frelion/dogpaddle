use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::distribution::Distribution;
use crate::{Error, ErrorKind};

const MANIFEST_FILE: &str = "MANIFEST";
const CHECKSUM_FILE: &str = "SHA256SUMS";
const RUNTIME_SBOM_FILE: &str = "runtime-sbom.json";
const TEMURIN_NOTICE_FILE: &str = "TEMURIN-NOTICE.md";
const MAX_MANIFEST_BYTES: u64 = 1024;
const MAX_CHECKSUM_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ENTRY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ENTRIES: usize = 100_000;
const MAX_DIRECTORY_DEPTH: usize = 32;
const REQUIRED_BUNDLE_FILES: &[&str] = &[
    MANIFEST_FILE,
    RUNTIME_SBOM_FILE,
    TEMURIN_NOTICE_FILE,
    "runtime/NOTICE",
    "runtime/release",
    "runtime/bin/java",
    "runtime/lib/modules",
    "runtime/lib/security/cacerts",
    "runtime/lib/tzdb.dat",
    "runtime/legal/java.base/LICENSE",
    "debezium/MANIFEST",
    "debezium/SHA256SUMS",
    "debezium/bom.json",
    "debezium/THIRD-PARTY-NOTICES.md",
];

pub(crate) struct Bundle {
    root: PathBuf,
    distribution: Distribution,
    jvm_library: PathBuf,
    fingerprint: [u8; 32],
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

        validate_top_level(&root)?;
        let manifest = validate_manifest(&root)?;
        let expected = read_checksums(&root)?;
        let actual = collect_files(&root)?;
        let actual_names = actual.keys().cloned().collect::<BTreeSet<_>>();
        let expected_names = expected.keys().cloned().collect::<BTreeSet<_>>();
        if actual_names != expected_names {
            return Err(invalid(
                "Debezium runtime bundle files do not exactly match SHA256SUMS",
            ));
        }

        for (relative, expected_digest) in &expected {
            let file = actual
                .get(relative)
                .expect("matching bundle file sets contain every checksum path");
            if &sha256(file)? != expected_digest {
                return Err(invalid(format!(
                    "Debezium runtime bundle checksum does not match for {relative}"
                )));
            }
        }

        validate_required_files(&actual)?;
        let jvm_library = root.join(jvm_relative_path()?);

        let distribution = Distribution::open(&root.join("debezium"))?;
        let fingerprint = fingerprint(&manifest, &expected);
        Ok(Self {
            root,
            distribution,
            jvm_library,
            fingerprint,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) const fn distribution(&self) -> &Distribution {
        &self.distribution
    }

    pub(crate) fn jvm_library(&self) -> &Path {
        &self.jvm_library
    }

    pub(crate) const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

fn validate_required_files(actual: &BTreeMap<String, PathBuf>) -> Result<(), Error> {
    for relative in REQUIRED_BUNDLE_FILES.iter().copied().chain(std::iter::once(
        jvm_relative_path()?
            .to_str()
            .ok_or_else(|| invalid("bundled JVM library path is not valid UTF-8"))?,
    )) {
        let path = actual.get(relative).ok_or_else(|| {
            invalid(format!(
                "Debezium runtime bundle is missing required file {relative}"
            ))
        })?;
        if path
            .metadata()
            .map_err(|_| invalid(format!("cannot inspect required bundle file {relative}")))?
            .len()
            == 0
        {
            return Err(invalid(format!(
                "Debezium runtime bundle required file is empty: {relative}"
            )));
        }
    }
    Ok(())
}

fn validate_top_level(root: &Path) -> Result<(), Error> {
    let mut names = BTreeSet::new();
    for entry in
        fs::read_dir(root).map_err(|_| invalid("cannot read the Debezium runtime bundle root"))?
    {
        let entry = entry.map_err(|_| invalid("cannot read a runtime bundle entry"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("runtime bundle paths must be valid UTF-8"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| invalid("cannot inspect a runtime bundle entry"))?;
        let valid = match name.as_str() {
            MANIFEST_FILE | CHECKSUM_FILE | RUNTIME_SBOM_FILE | TEMURIN_NOTICE_FILE => {
                file_type.is_file()
            }
            "runtime" | "debezium" | "bin" => file_type.is_dir(),
            _ => false,
        };
        if !valid {
            return Err(invalid(
                "Debezium runtime bundle contains an unexpected top-level entry",
            ));
        }
        names.insert(name);
    }

    for required in [
        MANIFEST_FILE,
        CHECKSUM_FILE,
        RUNTIME_SBOM_FILE,
        TEMURIN_NOTICE_FILE,
        "runtime",
        "debezium",
    ] {
        if !names.contains(required) {
            return Err(invalid(format!(
                "Debezium runtime bundle is missing required entry {required}"
            )));
        }
    }
    Ok(())
}

fn validate_manifest(root: &Path) -> Result<Vec<u8>, Error> {
    let manifest = read_bounded_regular_file(
        &root.join(MANIFEST_FILE),
        MAX_MANIFEST_BYTES,
        "Debezium runtime bundle MANIFEST is missing",
        "Debezium runtime bundle MANIFEST is invalid",
    )?;
    let expected = expected_manifest()?;
    if manifest != expected.as_bytes() {
        return Err(invalid(
            "Debezium runtime bundle MANIFEST is not for this supported target and runtime",
        ));
    }
    Ok(manifest)
}

pub(crate) fn expected_manifest() -> Result<String, Error> {
    let target = target_triple()?;
    Ok(format!(
        concat!(
            "dogpaddle.debezium.bundle=1\n",
            "target={}\n",
            "java.runtime.vendor=Eclipse Temurin\n",
            "java.runtime.version=21.0.12.1+1\n",
        ),
        target
    ))
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
    match std::env::consts::OS {
        "linux" => Ok(Path::new("runtime/lib/server/libjvm.so")),
        "macos" => Ok(Path::new("runtime/lib/server/libjvm.dylib")),
        _ => Err(invalid(
            "embedded Debezium bundles support only Linux and macOS",
        )),
    }
}

fn read_checksums(root: &Path) -> Result<BTreeMap<String, [u8; 32]>, Error> {
    let contents = read_bounded_regular_file(
        &root.join(CHECKSUM_FILE),
        MAX_CHECKSUM_BYTES,
        "Debezium runtime bundle SHA256SUMS is missing",
        "Debezium runtime bundle SHA256SUMS is invalid",
    )?;
    let contents = String::from_utf8(contents)
        .map_err(|_| invalid("runtime bundle SHA256SUMS is not valid UTF-8"))?;
    if !contents.ends_with('\n') {
        return Err(invalid(
            "runtime bundle SHA256SUMS is not canonically terminated",
        ));
    }

    let mut checksums = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for line in contents.lines() {
        let (digest, relative) = line
            .split_once("  ")
            .ok_or_else(|| invalid("runtime bundle SHA256SUMS has invalid framing"))?;
        validate_relative_file(relative)?;
        if previous.is_some_and(|previous| previous >= relative) {
            return Err(invalid(
                "runtime bundle SHA256SUMS paths are not strictly sorted",
            ));
        }
        previous = Some(relative);
        checksums.insert(relative.to_owned(), decode_sha256(digest)?);
    }
    if checksums.is_empty() {
        return Err(invalid("runtime bundle SHA256SUMS contains no files"));
    }
    Ok(checksums)
}

fn validate_relative_file(relative: &str) -> Result<(), Error> {
    if relative.is_empty()
        || relative.contains('\\')
        || relative == CHECKSUM_FILE
        || !Path::new(relative)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(invalid(
            "runtime bundle SHA256SUMS contains an invalid relative path",
        ));
    }
    let allowed = relative == MANIFEST_FILE
        || relative == RUNTIME_SBOM_FILE
        || relative == TEMURIN_NOTICE_FILE
        || relative.starts_with("runtime/")
        || relative.starts_with("debezium/")
        || relative.starts_with("bin/");
    if !allowed {
        return Err(invalid(
            "runtime bundle SHA256SUMS contains an unexpected path",
        ));
    }
    Ok(())
}

fn collect_files(root: &Path) -> Result<BTreeMap<String, PathBuf>, Error> {
    let mut files = BTreeMap::new();
    let mut budget = TraversalBudget::default();
    for file in [MANIFEST_FILE, RUNTIME_SBOM_FILE, TEMURIN_NOTICE_FILE] {
        collect_regular_file(root, Path::new(file), &mut files, &mut budget)?;
    }
    for directory in ["runtime", "debezium"] {
        collect_directory(root, Path::new(directory), 1, &mut files, &mut budget)?;
    }
    if root.join("bin").is_dir() {
        collect_directory(root, Path::new("bin"), 1, &mut files, &mut budget)?;
    }
    Ok(files)
}

#[derive(Default)]
struct TraversalBudget {
    entries: usize,
    total_bytes: u64,
}

fn collect_directory(
    root: &Path,
    relative: &Path,
    depth: usize,
    files: &mut BTreeMap<String, PathBuf>,
    budget: &mut TraversalBudget,
) -> Result<(), Error> {
    if depth > MAX_DIRECTORY_DEPTH {
        return Err(invalid("runtime bundle directory nesting is too deep"));
    }
    let directory = root.join(relative);
    for entry in
        fs::read_dir(&directory).map_err(|_| invalid("cannot read a runtime bundle directory"))?
    {
        budget.entries = budget
            .entries
            .checked_add(1)
            .ok_or_else(|| invalid("runtime bundle contains too many entries"))?;
        if budget.entries > MAX_ENTRIES {
            return Err(invalid("runtime bundle contains too many entries"));
        }
        let entry = entry.map_err(|_| invalid("cannot read a runtime bundle entry"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| invalid("cannot inspect a runtime bundle entry"))?;
        let child = relative.join(entry.file_name());
        if file_type.is_dir() {
            collect_directory(root, &child, depth + 1, files, budget)?;
        } else if file_type.is_file() {
            collect_regular_file(root, &child, files, budget)?;
        } else {
            return Err(invalid(
                "runtime bundle may contain only directories and regular files",
            ));
        }
    }
    Ok(())
}

fn collect_regular_file(
    root: &Path,
    relative: &Path,
    files: &mut BTreeMap<String, PathBuf>,
    budget: &mut TraversalBudget,
) -> Result<(), Error> {
    let relative = relative
        .to_str()
        .ok_or_else(|| invalid("runtime bundle paths must be valid UTF-8"))?
        .replace(std::path::MAIN_SEPARATOR, "/");
    validate_relative_file(&relative)?;
    let path = root.join(relative.as_str());
    let metadata =
        fs::symlink_metadata(&path).map_err(|_| invalid("cannot inspect a runtime bundle file"))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_ENTRY_BYTES {
        return Err(invalid(
            "runtime bundle entries must be bounded regular files",
        ));
    }
    budget.total_bytes = budget
        .total_bytes
        .checked_add(metadata.len())
        .ok_or_else(|| invalid("runtime bundle is too large"))?;
    if budget.total_bytes > MAX_TOTAL_BYTES {
        return Err(invalid("runtime bundle is too large"));
    }
    if files.insert(relative, path).is_some() {
        return Err(invalid("runtime bundle contains a duplicate path"));
    }
    Ok(())
}

fn read_bounded_regular_file(
    path: &Path,
    maximum_bytes: u64,
    missing_message: &str,
    invalid_message: &str,
) -> Result<Vec<u8>, Error> {
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

fn sha256(path: &Path) -> Result<[u8; 32], Error> {
    let mut file = File::open(path).map_err(|_| invalid("cannot open a runtime bundle file"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("cannot inspect an opened runtime bundle file"))?;
    if !metadata.is_file() || metadata.len() > MAX_ENTRY_BYTES {
        return Err(invalid(
            "runtime bundle entries must remain bounded regular files",
        ));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut remaining = MAX_ENTRY_BYTES.saturating_add(1);
    while remaining != 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .expect("bundle read limit fits in usize");
        let read = file
            .read(&mut buffer[..limit])
            .map_err(|_| invalid("cannot read a runtime bundle file"))?;
        if read == 0 {
            return Ok(digest.finalize().into());
        }
        digest.update(&buffer[..read]);
        remaining -= u64::try_from(read).expect("read byte count fits in u64");
    }
    Err(invalid("runtime bundle file grew beyond its size bound"))
}

fn fingerprint(manifest: &[u8], checksums: &BTreeMap<String, [u8; 32]>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"dogpaddle.debezium.bundle.fingerprint.v1\0");
    digest.update(manifest);
    for (name, checksum) in checksums {
        digest.update(
            u64::try_from(name.len())
                .expect("validated runtime bundle paths fit in u64")
                .to_be_bytes(),
        );
        digest.update(name.as_bytes());
        digest.update(checksum);
    }
    digest.finalize().into()
}

fn decode_sha256(value: &str) -> Result<[u8; 32], Error> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "runtime bundle SHA256SUMS contains an invalid digest",
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
            "runtime bundle SHA256SUMS contains an invalid digest",
        )),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidDistribution, message)
}
