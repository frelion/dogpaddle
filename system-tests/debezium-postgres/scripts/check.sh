#!/usr/bin/env bash
set -euo pipefail

readonly MAVEN_IMAGE="docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"
readonly POSTGRES_IMAGE="docker.io/library/postgres:16.15@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94"

if [[ "$#" -ne 0 ]]; then
  echo 'usage: check.sh' >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
gate_dir="$(cd -- "$script_dir/.." && pwd)"
repo_dir="$(cd -- "$gate_dir/../.." && pwd)"
product_crate_dir="$repo_dir/crates/debezium"
runtime_distribution="$product_crate_dir/bridge/target/distribution"
runtime_bundle="$product_crate_dir/bridge/target/bundles/dogpaddle-debezium-runtime-x86_64-unknown-linux-gnu"
host_binary="$gate_dir/host/target/release/dogpaddle-debezium-d1-host"
maven_cache="$repo_dir/target/debezium-maven-cache"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo 'the Debezium PostgreSQL gate is supported only on Linux x86_64' >&2
  exit 1
fi

for command in cargo id mkdir podman test uname; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if ! cargo clippy --version >/dev/null 2>&1; then
  echo 'the Rust Clippy component is unavailable' >&2
  exit 1
fi
if ! podman compose version >/dev/null 2>&1; then
  echo 'podman compose provider is unavailable' >&2
  exit 1
fi

podman pull "$MAVEN_IMAGE"
podman pull "$POSTGRES_IMAGE"
mkdir -p -- "$maven_cache"
podman run --rm \
  --pull never \
  --userns=keep-id \
  --user "$(id -u):$(id -g)" \
  --env HOME=/tmp/dogpaddle-maven \
  --env MAVEN_CONFIG=/tmp/dogpaddle-maven/.m2 \
  --env MAVEN_OPTS=-Duser.home=/tmp/dogpaddle-maven \
  --volume "$repo_dir:/workspace:Z" \
  --volume "$maven_cache:/tmp/dogpaddle-maven/.m2:Z" \
  --workdir /workspace \
  --entrypoint /bin/bash \
  "$MAVEN_IMAGE" \
  crates/debezium/scripts/build-distribution.sh

cargo fmt \
  --manifest-path "$gate_dir/host/Cargo.toml" \
  -- \
  --check
cargo test \
  --locked \
  --manifest-path "$gate_dir/host/Cargo.toml"
cargo clippy \
  --locked \
  --all-targets \
  --manifest-path "$gate_dir/host/Cargo.toml" \
  -- \
  -D warnings
cargo build \
  --locked \
  --release \
  --manifest-path "$gate_dir/host/Cargo.toml"

"$product_crate_dir/scripts/build-runtime-bundle.sh" x86_64-unknown-linux-gnu

test -x "$host_binary"
test -f "$runtime_distribution/MANIFEST"
test -f "$runtime_distribution/SHA256SUMS"
test -f "$runtime_distribution/bom.json"
test -f "$runtime_distribution/lib/dogpaddle-debezium-bridge.jar"
test -f "$runtime_distribution/lib/debezium-connector-postgres-3.6.2.Final.jar"
test -f "$runtime_bundle/runtime/lib/server/libjvm.so"
test -f "$runtime_bundle/runtime-sbom.json"
test -f "$runtime_bundle/debezium/lib/debezium-connector-postgres-3.6.2.Final.jar"

"$script_dir/run.sh" \
  --bundle "$runtime_bundle" \
  --host "$host_binary"
