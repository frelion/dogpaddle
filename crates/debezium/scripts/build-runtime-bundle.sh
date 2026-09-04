#!/usr/bin/env bash
set -euo pipefail

readonly JAVA_RUNTIME_VENDOR="Eclipse Temurin"
readonly JAVA_RUNTIME_VERSION="21.0.12.1+1"

if [[ "$#" -ne 1 ]]; then
  echo 'usage: build-runtime-bundle.sh TARGET' >&2
  exit 1
fi

target="$1"
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

for command in awk basename cat cp curl dirname find grep mkdir mktemp mv python3 rm rmdir tar; do
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
distribution_dir="$bridge_dir/target/distribution"
bundle_parent="$bridge_dir/target/bundles"
download_dir="$repo_dir/target/debezium-runtime-downloads"
bundle_name="dogpaddle-debezium-runtime-$target"
bundle_root="$bundle_parent/$bundle_name"
archive="$bundle_parent/$bundle_name.tar.gz"

if [[ ! -f "$asset_lock" ]]; then
  echo "missing runtime asset lock: $asset_lock" >&2
  exit 1
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
import shutil
import stat
import sys
import tarfile
import tempfile
from pathlib import Path

archive_path, expected_digest, destination_path = sys.argv[1:]
if not hasattr(tarfile, "data_filter"):
    raise SystemExit(
        "Python tarfile data extraction filters are required; use Python 3.12 "
        "or a version with the security filter backport"
    )

with open(archive_path, "rb") as source:
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
    actual_digest = digest.hexdigest()
    if actual_digest != expected_digest:
        raise SystemExit(
            "pinned Temurin archive changed before extraction: "
            f"expected {expected_digest}, got {actual_digest}"
        )
    source.seek(0)

    with tarfile.open(fileobj=source, mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) > 100_000:
            raise SystemExit("pinned Temurin archive contains too many entries")
        names = set()
        total_size = 0
        for member in members:
            name = member.name.rstrip("/")
            if not name or "\\" in name:
                raise SystemExit(f"non-portable path in pinned Temurin archive: {member.name}")
            if name in names:
                raise SystemExit(f"duplicate path in pinned Temurin archive: {member.name}")
            names.add(name)
            if member.isfile():
                if member.size > 512 * 1024 * 1024:
                    raise SystemExit(f"oversized file in pinned Temurin archive: {member.name}")
                total_size += member.size
                if total_size > 2 * 1024 * 1024 * 1024:
                    raise SystemExit("pinned Temurin archive expands beyond the bundle size limit")
        archive.extractall(destination_path, members=members, filter="data")

root = Path(destination_path).resolve()
for path in [entry for entry in root.rglob("*") if entry.is_symlink()]:
    try:
        target = path.resolve(strict=True)
        target.relative_to(root)
    except (OSError, ValueError) as error:
        raise SystemExit(f"unsafe link in pinned Temurin archive: {path}") from error
    if not target.is_file():
        raise SystemExit(f"Temurin link target is not a regular file: {path}")
    mode = stat.S_IMODE(target.stat().st_mode)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
    shutil.copyfile(target, temporary)
    os.chmod(temporary, mode)
    path.unlink()
    temporary.replace(path)
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

rm -rf -- "$bundle_root"
rm -f -- "$archive" "$archive.sha256"
mv -- "$assembled" "$bundle_root"
export COPYFILE_DISABLE=1
tar -czf "$archive" -C "$bundle_parent" "$bundle_name"
printf '%s  %s\n' "$(sha256_file "$archive")" "$(basename -- "$archive")" >"$archive.sha256"

echo "PASS self-contained Debezium runtime bundle: $bundle_root"
echo "PASS archive: $archive"
