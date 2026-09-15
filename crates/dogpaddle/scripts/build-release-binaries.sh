#!/usr/bin/env bash
set -euo pipefail

readonly LINUX_GLIBC_BASELINE="2.28"
readonly MACOS_DEPLOYMENT_TARGET="11.0"
static_runtime_dir=""

cleanup() {
  if [[ -n "$static_runtime_dir" ]]; then
    rm -rf -- "$static_runtime_dir"
  fi
}
trap cleanup EXIT

if [[ "$#" -ne 1 ]]; then
  echo 'usage: build-release-binaries.sh TARGET' >&2
  exit 2
fi

target="$1"
case "$target" in
  x86_64-unknown-linux-gnu|aarch64-unknown-linux-gnu)
    actual_glibc="$(getconf GNU_LIBC_VERSION 2>/dev/null || true)"
    if [[ "$actual_glibc" != "glibc $LINUX_GLIBC_BASELINE" ]]; then
      echo "Linux releases must be built against glibc $LINUX_GLIBC_BASELINE, got ${actual_glibc:-unknown}" >&2
      exit 1
    fi
    static_runtime_dir="${TMPDIR:-/tmp}/dogpaddle-static-runtime-$target"
    mkdir -- "$static_runtime_dir"
    libstdcxx_archive="$(gcc -print-file-name=libstdc++.a)"
    libgcc_archive="$(gcc -print-libgcc-file-name)"
    libgcc_eh_archive="$(gcc -print-file-name=libgcc_eh.a)"
    for archive in "$libstdcxx_archive" "$libgcc_archive" "$libgcc_eh_archive"; do
      if [[ ! -f "$archive" ]]; then
        echo "Linux release compiler is missing static runtime archive: $archive" >&2
        exit 1
      fi
    done
    # librocksdb-sys and rustc request dynamic runtime names explicitly. Put
    # static archives first in that lookup; the final archive audit enforces it.
    ln -s "$libstdcxx_archive" "$static_runtime_dir/libstdc++.so"
    ln -s "$libgcc_archive" "$static_runtime_dir/libgcc_s.so"
    export RUSTFLAGS="-L native=$static_runtime_dir -C link-arg=-Wl,--start-group -C link-arg=$libgcc_eh_archive -C link-arg=$libgcc_archive -C link-arg=-Wl,--end-group"
    ;;
  x86_64-apple-darwin|aarch64-apple-darwin)
    export MACOSX_DEPLOYMENT_TARGET="$MACOS_DEPLOYMENT_TARGET"
    ;;
  *)
    echo "unsupported DogPaddle release target: $target" >&2
    exit 2
    ;;
esac

rustc --print target-libdir --target "$target" >/dev/null
cargo build --locked --release --target "$target" \
  -p dogpaddle-debezium-runtime-host --bin bundled_runtime_probe
cargo build --locked --release --target "$target" \
  -p dogpaddle --bin dogpaddle

for binary in bundled_runtime_probe dogpaddle; do
  path="target/$target/release/$binary"
  if [[ ! -x "$path" ]]; then
    echo "release build did not produce executable $path" >&2
    exit 1
  fi
done

echo "PASS release binaries: $target"
