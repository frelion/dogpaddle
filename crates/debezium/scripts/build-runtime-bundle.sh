#!/usr/bin/env bash
set -euo pipefail

readonly JAVA_RUNTIME_VENDOR="Eclipse Temurin"
readonly JAVA_RUNTIME_VERSION="21.0.12.1+1"

usage() {
  echo 'usage: build-runtime-bundle.sh TARGET [EXECUTABLE [EXECUTABLE_NAME]]' >&2
}

if [[ "$#" -lt 1 || "$#" -gt 3 ]]; then
  usage
  exit 1
fi

target="$1"
executable="${2:-}"
executable_name="${3:-}"

case "$target" in
  x86_64-unknown-linux-gnu|aarch64-unknown-linux-gnu)
    jvm_relative_path="lib/server/libjvm.so"
    ;;
  x86_64-apple-darwin|aarch64-apple-darwin)
    jvm_relative_path="lib/server/libjvm.dylib"
    ;;
  *)
    echo "unsupported Debezium runtime target: $target" >&2
    exit 1
    ;;
esac

if [[ -n "$executable" ]]; then
  if [[ ! -f "$executable" || ! -x "$executable" ]]; then
    echo "bundle executable is not an executable regular file: $executable" >&2
    exit 1
  fi
  if [[ -z "$executable_name" ]]; then
    executable_name="$(basename -- "$executable")"
  fi
  if [[ ! "$executable_name" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "invalid bundle executable name: $executable_name" >&2
    exit 1
  fi
elif [[ -n "$executable_name" ]]; then
  usage
  exit 1
fi

for command in awk basename cat chmod cp curl dirname find grep mkdir mktemp mv python3 rm rmdir sed sort tar; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if ! command -v sha256sum >/dev/null 2>&1 \
  && ! command -v shasum >/dev/null 2>&1; then
  echo 'missing SHA-256 command: install sha256sum or shasum' >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
crate_dir="$(cd -- "$script_dir/.." && pwd)"
repo_dir="$(cd -- "$crate_dir/../.." && pwd)"
bridge_dir="$crate_dir/bridge"
asset_lock="$crate_dir/runtime-assets/temurin-$JAVA_RUNTIME_VERSION.tsv"
distribution_dir="${DOGPADDLE_DEBEZIUM_DISTRIBUTION:-$bridge_dir/target/distribution}"
bundle_parent="$bridge_dir/target/bundles"
download_dir="${DOGPADDLE_DEBEZIUM_RUNTIME_CACHE:-$repo_dir/target/debezium-runtime-downloads}"
bundle_name="dogpaddle-debezium-runtime-$target"
bundle_root="$bundle_parent/$bundle_name"
archive="$bundle_parent/$bundle_name.tar.gz"

if [[ ! -f "$asset_lock" ]]; then
  echo "missing runtime asset lock: $asset_lock" >&2
  exit 1
fi
python3 - "$distribution_dir" <<'PY'
import hashlib
import json
import re
import stat
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected_manifest = (
    b"dogpaddle.debezium.distribution=1\n"
    b"bridge.protocol=1\n"
    b"debezium.version=3.6.2.Final\n"
    b"kafka.connect.version=4.3.0\n"
)
required_jars = {
    "connect-api-4.3.0.jar",
    "connect-json-4.3.0.jar",
    "connect-runtime-4.3.0.jar",
    "debezium-embedded-3.6.2.Final.jar",
    "dogpaddle-debezium-bridge.jar",
    "slf4j-simple-1.7.36.jar",
}

def regular_nonempty(path: Path, maximum: int) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(metadata.st_mode) and 0 < metadata.st_size <= maximum

metadata_files = {
    "MANIFEST": 1024,
    "SHA256SUMS": 8 * 1024 * 1024,
    "bom.json": 64 * 1024 * 1024,
    "THIRD-PARTY-NOTICES.md": 64 * 1024 * 1024,
}
for relative, maximum in metadata_files.items():
    if not regular_nonempty(root / relative, maximum):
        raise SystemExit(f"invalid Debezium distribution metadata: {root / relative}")
if (root / "MANIFEST").read_bytes() != expected_manifest:
    raise SystemExit(f"Debezium distribution MANIFEST is not pinned: {root / 'MANIFEST'}")
try:
    bom = json.loads((root / "bom.json").read_bytes())
    component = bom["metadata"]["component"]
except (KeyError, TypeError, ValueError) as error:
    raise SystemExit("Debezium distribution BOM is not a valid CycloneDX document") from error
if (
    bom.get("bomFormat") != "CycloneDX"
    or component.get("group") != "dev.dogpaddle"
    or component.get("name") != "dogpaddle-debezium-bridge"
    or component.get("version") != "0.1.0"
):
    raise SystemExit("Debezium distribution BOM has an unexpected root component")

checksum_bytes = (root / "SHA256SUMS").read_bytes()
if not checksum_bytes.endswith(b"\n"):
    raise SystemExit("Debezium distribution SHA256SUMS is not canonically terminated")
expected = {}
for raw_line in checksum_bytes.splitlines():
    try:
        digest, relative = raw_line.decode("ascii").split("  lib/", 1)
    except (UnicodeDecodeError, ValueError) as error:
        raise SystemExit("Debezium distribution SHA256SUMS has invalid framing") from error
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(
        r"[^/\\]+\.jar", relative
    ):
        raise SystemExit("Debezium distribution SHA256SUMS has an invalid entry")
    if relative in expected:
        raise SystemExit(f"duplicate Debezium distribution checksum: {relative}")
    expected[relative] = digest

library = root / "lib"
try:
    library_metadata = library.lstat()
except OSError as error:
    raise SystemExit(f"cannot inspect Debezium distribution library: {library}") from error
if not stat.S_ISDIR(library_metadata.st_mode):
    raise SystemExit(f"Debezium distribution library is not a real directory: {library}")
try:
    entries = list(library.iterdir())
except OSError as error:
    raise SystemExit(f"cannot read Debezium distribution library: {library}") from error
actual = {}
for path in entries:
    if not regular_nonempty(path, 512 * 1024 * 1024) or not path.name.endswith(".jar"):
        raise SystemExit(f"Debezium distribution lib contains a non-JAR file: {path}")
    actual[path.name] = path
if set(actual) != set(expected):
    raise SystemExit("Debezium distribution JARs do not exactly match SHA256SUMS")
missing = required_jars - set(actual)
if missing:
    raise SystemExit(f"Debezium distribution is missing required JARs: {sorted(missing)}")
for name, path in actual.items():
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    digest = digest.hexdigest()
    if digest != expected[name]:
        raise SystemExit(f"Debezium distribution checksum does not match for {name}")
PY

if [[ -n "$executable" ]]; then
  python3 - "$target" "$executable" <<'PY'
import struct
import sys

target, executable = sys.argv[1:]
with open(executable, "rb") as source:
    header = source.read(20)

if target.endswith("-unknown-linux-gnu"):
    machines = {62: "x86_64", 183: "aarch64"}
    expected = 62 if target.startswith("x86_64-") else 183
    if len(header) < 20 or header[:4] != b"\x7fELF" or header[4:6] != b"\x02\x01":
        raise SystemExit(
            f"bundle executable is not a 64-bit little-endian ELF file for {target}: {executable}"
        )
    actual = struct.unpack_from("<H", header, 18)[0]
    if actual != expected:
        raise SystemExit(
            f"bundle executable architecture does not match {target}: "
            f"expected {machines[expected]}, found {machines.get(actual, f'ELF machine {actual}')}"
        )
else:
    cpu_types = {0x01000007: "x86_64", 0x0100000C: "aarch64"}
    expected = 0x01000007 if target.startswith("x86_64-") else 0x0100000C
    if len(header) < 8 or header[:4] != b"\xcf\xfa\xed\xfe":
        raise SystemExit(
            f"bundle executable is not a thin 64-bit little-endian Mach-O file for {target}: "
            f"{executable}; universal binaries are not supported"
        )
    actual = struct.unpack_from("<I", header, 4)[0]
    if actual != expected:
        raise SystemExit(
            f"bundle executable architecture does not match {target}: "
            f"expected {cpu_types[expected]}, found {cpu_types.get(actual, f'Mach-O CPU {actual:#x}')}"
        )
PY
fi

asset_line="$(awk -F '\t' -v target="$target" '$1 == target { print; count += 1 } END { if (count != 1) exit 1 }' "$asset_lock")" || {
  echo "runtime asset lock has no unique entry for $target" >&2
  exit 1
}
IFS=$'\t' read -r _ jre_url jre_sha256 sbom_url sbom_sha256 <<<"$asset_line"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

download_verified() {
  local url="$1"
  local expected="$2"
  local destination="$3"
  if [[ -f "$destination" && "$(sha256_file "$destination")" == "$expected" ]]; then
    return
  fi
  local temporary="$destination.part"
  if ! curl --continue-at - --fail --location --retry 3 --output "$temporary" "$url"; then
    rm -f -- "$temporary"
    curl --fail --location --retry 3 --output "$temporary" "$url"
  fi
  local actual
  actual="$(sha256_file "$temporary")"
  if [[ "$actual" != "$expected" ]]; then
    rm -f -- "$temporary"
    echo "SHA-256 mismatch for $url: expected $expected, got $actual" >&2
    exit 1
  fi
  mv -- "$temporary" "$destination"
}

mkdir -p -- "$download_dir" "$bundle_parent"
lock_dir="$bundle_parent/.$bundle_name.lock"
if ! mkdir -- "$lock_dir" 2>/dev/null; then
  echo "another bundle build is already running for $target: $lock_dir" >&2
  exit 1
fi
staging=""
cleanup() {
  if [[ -n "$staging" ]]; then
    rm -rf -- "$staging"
  fi
  rmdir -- "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT

jre_archive="$download_dir/${jre_url##*/}"
sbom_file="$download_dir/${sbom_url##*/}"
download_verified "$jre_url" "$jre_sha256" "$jre_archive"
download_verified "$sbom_url" "$sbom_sha256" "$sbom_file"

staging="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-debezium-bundle.XXXXXX")"

extracted="$staging/extracted"
assembled="$staging/$bundle_name"
mkdir -p -- "$extracted" "$assembled/runtime" "$assembled/debezium"
python3 - "$jre_archive" "$jre_sha256" "$extracted" <<'PY'
import hashlib
import os
import posixpath
import shutil
import sys
import tarfile
from pathlib import PurePosixPath

archive_path, expected_digest, destination_path = sys.argv[1:]

with open(archive_path, "rb") as source:
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
    actual_digest = digest.hexdigest()
    if actual_digest != expected_digest:
        raise SystemExit(
            f"pinned Temurin archive changed before extraction: "
            f"expected {expected_digest}, got {actual_digest}"
        )
    source.seek(0)

    with tarfile.open(fileobj=source, mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) > 100_000:
            raise SystemExit("pinned Temurin archive contains too many entries")

        by_name = {}
        total_size = 0
        for member in members:
            if "\\" in member.name:
                raise SystemExit(f"non-portable path in pinned Temurin archive: {member.name}")
            normalized = posixpath.normpath(member.name)
            path = PurePosixPath(normalized)
            if (
                normalized in ("", ".")
                or path.is_absolute()
                or ".." in path.parts
                or normalized != member.name.rstrip("/")
            ):
                raise SystemExit(f"unsafe path in pinned Temurin archive: {member.name}")
            if normalized in by_name:
                raise SystemExit(f"duplicate path in pinned Temurin archive: {member.name}")
            if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise SystemExit(f"unsupported entry in pinned Temurin archive: {member.name}")
            if member.isfile():
                if member.size > 512 * 1024 * 1024:
                    raise SystemExit(f"oversized file in pinned Temurin archive: {member.name}")
                total_size += member.size
                if total_size > 2 * 1024 * 1024 * 1024:
                    raise SystemExit("pinned Temurin archive expands beyond the bundle size limit")
            by_name[normalized] = member

        def link_target(member):
            target = PurePosixPath(member.linkname)
            if target.is_absolute() or "\\" in member.linkname:
                raise SystemExit(f"unsafe link in pinned Temurin archive: {member.name}")
            base = posixpath.dirname(member.name) if member.issym() else ""
            normalized = posixpath.normpath(posixpath.join(base, member.linkname))
            path = PurePosixPath(normalized)
            if normalized in ("", ".") or path.is_absolute() or ".." in path.parts:
                raise SystemExit(f"escaping link in pinned Temurin archive: {member.name}")
            return normalized

        def resolve_regular(name, seen):
            if name in seen:
                raise SystemExit(f"link cycle in pinned Temurin archive: {name}")
            member = by_name.get(name)
            if member is None:
                raise SystemExit(f"missing link target in pinned Temurin archive: {name}")
            if member.isfile():
                return member
            if member.issym() or member.islnk():
                return resolve_regular(link_target(member), {*seen, name})
            raise SystemExit(f"Temurin link target is not a regular file: {name}")

        for member in members:
            if member.issym() or member.islnk():
                resolve_regular(link_target(member), {member.name})

        root = os.path.realpath(destination_path)

        def output_path(name):
            return os.path.join(root, *PurePosixPath(name).parts)

        for member in sorted(
            (member for member in members if member.isdir()),
            key=lambda item: len(PurePosixPath(item.name).parts),
        ):
            path = output_path(member.name)
            os.makedirs(path, exist_ok=True)
            os.chmod(path, member.mode & 0o777)

        for member in (member for member in members if member.isfile()):
            path = output_path(member.name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            extracted = archive.extractfile(member)
            if extracted is None:
                raise SystemExit(f"cannot read pinned Temurin archive entry: {member.name}")
            try:
                with open(path, "xb") as output:
                    shutil.copyfileobj(extracted, output, length=1024 * 1024)
            finally:
                extracted.close()
            if os.path.getsize(path) != member.size:
                raise SystemExit(f"truncated pinned Temurin archive entry: {member.name}")
            os.chmod(path, member.mode & 0o777)

        for member in (member for member in members if member.issym() or member.islnk()):
            source_member = resolve_regular(link_target(member), {member.name})
            path = output_path(member.name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            shutil.copyfile(output_path(source_member.name), path)
            os.chmod(path, source_member.mode & 0o777)
PY

java_home=""
java_home_count=0
while IFS= read -r candidate; do
  candidate_home="$(cd -- "$(dirname -- "$candidate")/.." && pwd)"
  if [[ -f "$candidate_home/$jvm_relative_path" ]]; then
    java_home="$candidate_home"
    java_home_count=$((java_home_count + 1))
  fi
done < <(find "$extracted" -path '*/bin/java' -type f -print)

if [[ "$java_home_count" -ne 1 ]]; then
  echo "expected one Java home with $jvm_relative_path, found $java_home_count" >&2
  exit 1
fi

cp -R -L "$java_home"/. "$assembled/runtime/"
cp -R -L "$distribution_dir"/. "$assembled/debezium/"
cp -- "$sbom_file" "$assembled/runtime-sbom.json"
cp -- "$crate_dir/runtime-assets/TEMURIN-NOTICE.md" "$assembled/TEMURIN-NOTICE.md"
if [[ -n "$executable" ]]; then
  mkdir -p -- "$assembled/bin"
  cp -- "$executable" "$assembled/bin/$executable_name"
  chmod 755 "$assembled/bin/$executable_name"
fi

required_files=(
  "runtime/NOTICE"
  "runtime/release"
  "runtime/bin/java"
  "runtime/lib/modules"
  "runtime/lib/security/cacerts"
  "runtime/lib/tzdb.dat"
  "runtime/legal/java.base/LICENSE"
  "runtime/$jvm_relative_path"
  "debezium/MANIFEST"
  "debezium/SHA256SUMS"
  "debezium/bom.json"
  "debezium/THIRD-PARTY-NOTICES.md"
  "runtime-sbom.json"
  "TEMURIN-NOTICE.md"
)
for relative in "${required_files[@]}"; do
  if [[ ! -f "$assembled/$relative" || ! -s "$assembled/$relative" ]]; then
    echo "self-contained bundle is missing required non-empty file: $relative" >&2
    exit 1
  fi
done
if [[ "$(sha256_file "$assembled/runtime-sbom.json")" != "$sbom_sha256" ]]; then
  echo 'copied Temurin SBOM does not match the pinned SHA-256' >&2
  exit 1
fi

python3 - "$target" "$JAVA_RUNTIME_VERSION" "$assembled/runtime/release" <<'PY'
import sys
from pathlib import Path

target, version, release_path = sys.argv[1:]
values = {}
for line in Path(release_path).read_text(encoding="utf-8").splitlines():
    if "=" not in line:
        raise SystemExit(f"invalid Temurin release metadata line: {line!r}")
    key, value = line.split("=", 1)
    if not (len(value) >= 2 and value.startswith('"') and value.endswith('"')):
        raise SystemExit(f"invalid Temurin release metadata value: {key}")
    if key in values:
        raise SystemExit(f"duplicate Temurin release metadata key: {key}")
    values[key] = value[1:-1]

if target == "x86_64-unknown-linux-gnu":
    platform = {"OS_NAME": "Linux", "OS_ARCH": "x86_64", "LIBC": "gnu"}
elif target == "aarch64-unknown-linux-gnu":
    platform = {"OS_NAME": "Linux", "OS_ARCH": "aarch64", "LIBC": "gnu"}
elif target == "x86_64-apple-darwin":
    platform = {"OS_NAME": "Darwin", "OS_ARCH": "x86_64", "LIBC": "default"}
elif target == "aarch64-apple-darwin":
    platform = {"OS_NAME": "Darwin", "OS_ARCH": "aarch64", "LIBC": "default"}
else:
    raise SystemExit(f"unsupported target in Temurin metadata validator: {target}")

expected = {
    "IMPLEMENTOR": "Eclipse Adoptium",
    "SEMANTIC_VERSION": version,
    "IMAGE_TYPE": "JRE",
    "JVM_VARIANT": "Hotspot",
    **platform,
}
for key, expected_value in expected.items():
    actual = values.get(key)
    if actual != expected_value:
        raise SystemExit(
            f"Temurin release metadata does not match {target}: "
            f"{key} expected {expected_value!r}, got {actual!r}"
        )
PY

python3 - "$JAVA_RUNTIME_VERSION" "$assembled/runtime-sbom.json" <<'PY'
import json
import sys
from pathlib import Path

version, sbom_path = sys.argv[1:]
try:
    document = json.loads(Path(sbom_path).read_bytes())
    component = document["metadata"]["component"]
except (OSError, KeyError, TypeError, ValueError) as error:
    raise SystemExit("Temurin SBOM is not a valid CycloneDX component document") from error
if document.get("bomFormat") != "CycloneDX":
    raise SystemExit("Temurin SBOM does not identify itself as CycloneDX")
if component.get("name") != "Eclipse Temurin":
    raise SystemExit("Temurin SBOM has an unexpected root component")
if component.get("version") != f"{version}-LTS":
    raise SystemExit("Temurin SBOM has an unexpected runtime version")
PY

if find "$assembled" -type l -print | grep . >/dev/null; then
  echo 'normalized bundle must not contain symbolic links' >&2
  exit 1
fi
if find "$assembled" ! -type d ! -type f -print | grep . >/dev/null; then
  echo 'normalized bundle must contain only directories and regular files' >&2
  exit 1
fi

cat >"$assembled/MANIFEST" <<EOF
dogpaddle.debezium.bundle=1
target=$target
java.runtime.vendor=$JAVA_RUNTIME_VENDOR
java.runtime.version=$JAVA_RUNTIME_VERSION
EOF

(
  cd -- "$assembled"
  while IFS= read -r path; do
    printf '%s  %s\n' "$(sha256_file "$path")" "$path"
  done < <(find . -type f ! -path './SHA256SUMS' -print | sed 's#^\./##' | LC_ALL=C sort)
) >"$assembled/SHA256SUMS"

"$assembled/runtime/bin/java" -version

rm -rf -- "$bundle_root"
rm -f -- "$archive" "$archive.sha256"
mv -- "$assembled" "$bundle_root"
export COPYFILE_DISABLE=1
tar -czf "$archive" -C "$bundle_parent" "$bundle_name"
printf '%s  %s\n' "$(sha256_file "$archive")" "$(basename -- "$archive")" >"$archive.sha256"

echo "PASS self-contained Debezium runtime bundle: $bundle_root"
echo "PASS archive: $archive"
