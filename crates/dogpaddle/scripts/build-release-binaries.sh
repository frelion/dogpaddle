#!/usr/bin/env bash
set -euo pipefail

readonly LINUX_GLIBC_BASELINE="2.28"
readonly MACOS_DEPLOYMENT_TARGET="11.0"

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
    export RUSTFLAGS="-C link-arg=-static-libstdc++ -C link-arg=-static-libgcc"
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
