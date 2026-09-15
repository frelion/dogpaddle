#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo 'usage: build-release-archive.sh TARGET RELEASE_ID' >&2
  exit 2
fi

target="$1"
release_id="$2"
if [[ ! "$release_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "release id is not portable: $release_id" >&2
  exit 2
fi

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
binary="$repo_dir/target/$target/release/dogpaddle"
runtime_root="$repo_dir/crates/debezium/bridge/target/bundles/dogpaddle-debezium-runtime-$target"
package_dir="$repo_dir/target/release-packages"
archive_name="dogpaddle-$release_id-$target.tar.gz"
release_name="${archive_name%.tar.gz}"
staging="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-release.XXXXXX")"

cleanup() {
  rm -rf -- "$staging"
}
trap cleanup EXIT

if [[ ! -x "$binary" ]]; then
  echo "missing release executable: $binary" >&2
  exit 1
fi
if [[ ! -d "$runtime_root" ]]; then
  echo "missing runtime bundle: $runtime_root" >&2
  exit 1
fi

release_root="$staging/$release_name"
mkdir -p -- "$release_root/bin" "$release_root/libexec/dogpaddle/debezium" "$package_dir"
install -m 0755 "$binary" "$release_root/bin/dogpaddle"
cp -R "$runtime_root/." "$release_root/libexec/dogpaddle/debezium/"
cp "$repo_dir/README.md" "$release_root/README.md"

archive="$package_dir/$archive_name"
rm -f -- "$archive" "$archive.sha256"
export COPYFILE_DISABLE=1
tar -czf "$archive" -C "$staging" "$release_name"

if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$archive" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$archive" | awk '{print $1}')"
fi
printf '%s  %s\n' "$digest" "$archive_name" >"$archive.sha256"

echo "PASS DogPaddle release archive: $archive"
