#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != "5" ]]; then
  echo 'usage: verify-bundle.sh TARGET ARCHIVE RUST_PROBE PROBE_JAR SCRATCH_DIR' >&2
  exit 2
fi

target="$1"
archive="$2"
probe="$3"
probe_connector="$4"
scratch_dir="$5"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
bundle_name="dogpaddle-debezium-runtime-$target"
checksum="$archive.sha256"

for path in "$archive" "$checksum" "$probe" "$probe_connector"; do
  if [[ ! -f "$path" ]]; then
    echo "missing bundle verification input: $path" >&2
    exit 1
  fi
done

(
  cd -- "$(dirname -- "$archive")"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check "$(basename -- "$checksum")"
  else
    shasum -a 256 --check "$(basename -- "$checksum")"
  fi
)

relocated_parent="$scratch_dir/DogPaddle runtime smoke $target"
if [[ -e "$relocated_parent" ]]; then
  echo "refusing to replace existing relocation directory: $relocated_parent" >&2
  exit 1
fi
mkdir -p -- "$relocated_parent"
tar -xzf "$archive" -C "$relocated_parent"
relocated_bundle="$relocated_parent/$bundle_name"

"$script_dir/install.sh" \
  "$relocated_bundle" \
  "$probe_connector"

empty_path="$scratch_dir/dogpaddle-empty-path-$target"
missing_java="$scratch_dir/dogpaddle-missing-java-$target"
mkdir -p -- "$empty_path"
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
PATH="$empty_path" \
JAVA_HOME="$missing_java" \
JDK_HOME="$missing_java" \
LD_LIBRARY_PATH= \
DYLD_LIBRARY_PATH= \
DYLD_FALLBACK_LIBRARY_PATH= \
  "$probe" "$relocated_bundle"
