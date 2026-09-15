#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo 'usage: sign-macos-release.sh TARGET SIGNING_IDENTITY' >&2
  exit 2
fi

target="$1"
identity="$2"
case "$target" in
  x86_64-apple-darwin|aarch64-apple-darwin) ;;
  *)
    echo "macOS signing does not support target: $target" >&2
    exit 2
    ;;
esac

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
entitlements="$repo_dir/crates/dogpaddle/release/macos-entitlements.plist"

for binary in bundled_runtime_probe dogpaddle; do
  path="$repo_dir/target/$target/release/$binary"
  if [[ ! -x "$path" ]]; then
    echo "missing macOS release executable: $path" >&2
    exit 1
  fi
  codesign --force --sign "$identity" --timestamp --options runtime \
    --entitlements "$entitlements" "$path"
  codesign --verify --strict --verbose=2 "$path"
done

echo "PASS signed macOS release executables: $target"
