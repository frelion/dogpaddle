#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo 'usage: build-linux-release.sh TARGET BUILD_IMAGE' >&2
  exit 2
fi

target="$1"
build_image="$2"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
workspace="$(cd -- "$script_dir/../../.." && pwd)"
cargo_home="$(dirname -- "$(dirname -- "$(command -v cargo)")")"
rustup_home="$(rustup show home)"
toolchain_bin="$(rustup run 1.96.0 rustc --print sysroot)/bin"
scratch="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/dogpaddle-linux-release.XXXXXX")"

cleanup() {
  rm -rf -- "$scratch"
}
trap cleanup EXIT

case "$target" in
  x86_64-unknown-linux-gnu)
    libclang_url="https://files.pythonhosted.org/packages/1d/fc/716c1e62e512ef1c160e7984a73a5fc7df45166f2ff3f254e71c58076f7c/libclang-18.1.1-py2.py3-none-manylinux2010_x86_64.whl"
    libclang_sha256="c533091d8a3bbf7460a00cb6c1a71da93bffe148f172c7d03b1c31fbf8aa2a0b"
    ;;
  aarch64-unknown-linux-gnu)
    libclang_url="https://files.pythonhosted.org/packages/3c/3d/f0ac1150280d8d20d059608cf2d5ff61b7c3b7f7bcf9c0f425ab92df769a/libclang-18.1.1-py2.py3-none-manylinux2014_aarch64.whl"
    libclang_sha256="54dda940a4a0491a9d1532bf071ea3ef26e6dbaf03b5000ed94dd7174e8f9592"
    ;;
  *)
    echo "unsupported Linux release target: $target" >&2
    exit 2
    ;;
esac

libclang_wheel="$scratch/libclang.whl"
libclang_root="$scratch/libclang"
curl --fail --location --retry 3 \
  --connect-timeout 15 --max-time 300 --max-filesize 67108864 \
  --output "$libclang_wheel" "$libclang_url"
printf '%s  %s\n' "$libclang_sha256" "$libclang_wheel" | sha256sum --check -
mkdir -p -- "$libclang_root"
python3 -m zipfile -e "$libclang_wheel" "$libclang_root"
libclang_file="$(find "$libclang_root" -path '*/clang/native/libclang.so' -type f -print -quit)"
if [[ -z "$libclang_file" ]]; then
  echo 'verified libclang wheel did not contain clang/native/libclang.so' >&2
  exit 1
fi
libclang_path="$(dirname -- "$libclang_file")"

docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env CARGO_HOME="$cargo_home" \
  --env LIBCLANG_PATH=/opt/dogpaddle-libclang \
  --env TARGET="$target" \
  --env TOOLCHAIN_BIN="$toolchain_bin" \
  --volume "$cargo_home:$cargo_home" \
  --volume "$libclang_path:/opt/dogpaddle-libclang:ro" \
  --volume "$rustup_home:$rustup_home:ro" \
  --volume "$workspace:/workspace" \
  --workdir /workspace \
  --entrypoint /bin/bash \
  "$build_image" \
  -c 'set -euo pipefail
  export PATH="$TOOLCHAIN_BIN:$PATH"
  export RUSTC="$TOOLCHAIN_BIN/rustc"
  compiler_include_dir="$(gcc -print-file-name=include)"
  test -f "$compiler_include_dir/stdbool.h"
  export BINDGEN_EXTRA_CLANG_ARGS="-isystem $compiler_include_dir"
  rustc --version --verbose
  cargo --version --verbose
  crates/dogpaddle/scripts/build-release-binaries.sh "$TARGET"'
