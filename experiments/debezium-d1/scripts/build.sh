#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
experiment_dir="$(cd -- "$script_dir/.." && pwd)"
repo_dir="$(cd -- "$experiment_dir/../.." && pwd)"
product_crate_dir="$repo_dir/crates/debezium"
runtime_distribution="$product_crate_dir/bridge/target/distribution"
runtime_bundle="$product_crate_dir/bridge/target/bundles/dogpaddle-debezium-runtime-x86_64-unknown-linux-gnu"
host_binary="$experiment_dir/host/target/release/dogpaddle-debezium-d1-host"

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

"$product_crate_dir/scripts/build-distribution.sh"
"$product_crate_dir/scripts/build-runtime-bundle.sh" \
  x86_64-unknown-linux-gnu \
  "$host_binary" \
  dogpaddle-debezium-d1-host

test -x "$host_binary"
test -f "$runtime_distribution/MANIFEST"
test -f "$runtime_distribution/SHA256SUMS"
test -f "$runtime_distribution/bom.json"
test -f "$runtime_distribution/lib/dogpaddle-debezium-bridge.jar"
test -f "$runtime_distribution/lib/debezium-connector-postgres-3.6.2.Final.jar"
test -x "$runtime_bundle/bin/dogpaddle-debezium-d1-host"
test -f "$runtime_bundle/runtime/lib/server/libjvm.so"
test -f "$runtime_bundle/runtime-sbom.json"
test -f "$runtime_bundle/debezium/lib/debezium-connector-postgres-3.6.2.Final.jar"

echo "PASS D1 consumes the product-built PostgreSQL distribution unchanged"
echo "PASS D1 host is packaged with the pinned self-contained Java runtime"
echo "PASS Rust host format, tests, Clippy, and release build"
