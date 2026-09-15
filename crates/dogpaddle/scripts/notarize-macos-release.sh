#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo 'usage: notarize-macos-release.sh ARCHIVE API_KEY KEY_ID ISSUER_ID' >&2
  exit 2
fi

archive="$1"
api_key="$2"
key_id="$3"
issuer_id="$4"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
archive_name="$(basename -- "$archive")"
release_name="${archive_name%.tar.gz}"
staging="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-notarization.XXXXXX")"

cleanup() {
  rm -rf -- "$staging"
}
trap cleanup EXIT

if [[ ! -f "$archive" || ! -f "$api_key" ]]; then
  echo 'missing archive or App Store Connect API key for notarization' >&2
  exit 1
fi

release_root="$(python3 "$script_dir/release_archive.py" "$archive" "$staging")"
if [[ "$release_root" != "$staging/$release_name" ]]; then
  echo "release archive has an unexpected root: $release_root" >&2
  exit 1
fi
submission="$release_root.zip"
ditto -c -k --keepParent "$release_root" "$submission"
xcrun notarytool submit "$submission" \
  --key "$api_key" \
  --key-id "$key_id" \
  --issuer "$issuer_id" \
  --wait
spctl --assess --type execute --verbose=2 "$release_root/bin/dogpaddle"

echo "PASS notarized macOS release: $archive"
