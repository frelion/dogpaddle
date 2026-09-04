#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo 'usage: install-lifecycle-probe.sh BUNDLE_ROOT PROBE_JAR' >&2
  exit 1
fi

bundle_root="$1"
probe_jar="$2"
library="$bundle_root/debezium/lib"
checksums="$bundle_root/debezium/SHA256SUMS"
probe_name="dogpaddle-debezium-lifecycle-probe.jar"

for command in awk cp mktemp mv rm; do
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

if [[ ! -d "$library" || ! -f "$checksums" ]]; then
  echo "not a Debezium runtime bundle: $bundle_root" >&2
  exit 1
fi
if [[ ! -f "$probe_jar" || ! -s "$probe_jar" ]]; then
  echo "missing non-empty lifecycle probe JAR: $probe_jar" >&2
  exit 1
fi

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

cp -- "$probe_jar" "$library/$probe_name"
temporary="$(mktemp "$bundle_root/debezium/.SHA256SUMS.XXXXXX")"
cleanup() {
  rm -f -- "$temporary"
}
trap cleanup EXIT

(
  cd -- "$bundle_root/debezium"
  export LC_ALL=C
  for jar in lib/*.jar; do
    printf '%s  %s\n' "$(sha256_file "$jar")" "$jar"
  done
) >"$temporary"
mv -- "$temporary" "$checksums"
trap - EXIT

echo "PASS installed test-only lifecycle probe into relocated bundle: $bundle_root"
