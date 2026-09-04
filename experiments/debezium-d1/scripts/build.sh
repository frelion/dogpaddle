#!/usr/bin/env bash
set -euo pipefail

readonly MAVEN_IMAGE="docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
experiment_dir="$(cd -- "$script_dir/.." && pwd)"

cargo fmt \
  --manifest-path "$experiment_dir/host/Cargo.toml" \
  -- \
  --check
cargo test \
  --locked \
  --manifest-path "$experiment_dir/host/Cargo.toml"
cargo clippy \
  --locked \
  --all-targets \
  --manifest-path "$experiment_dir/host/Cargo.toml" \
  -- \
  -D warnings
cargo build \
  --locked \
  --release \
  --manifest-path "$experiment_dir/host/Cargo.toml"

podman run --rm \
  --volume "$experiment_dir/bridge:/workspace:Z" \
  --workdir /workspace \
  "$MAVEN_IMAGE" \
  mvn --batch-mode --no-transfer-progress clean package

if unzip -Z1 "$experiment_dir/bridge/target/debezium-d1-bridge-0.1.0.jar" \
  | rg '^io/debezium/' >/dev/null; then
  echo 'bridge artifact must not contain copied or shadowed io.debezium classes' >&2
  exit 1
fi

test -x "$experiment_dir/host/target/release/dogpaddle-debezium-d1-host"
test -f "$experiment_dir/bridge/target/debezium-d1-bridge-0.1.0.jar"
test -d "$experiment_dir/bridge/target/dependency"

echo 'PASS bridge artifact contains no io.debezium classes'
echo 'PASS Rust host format, tests, Clippy, and release build'
