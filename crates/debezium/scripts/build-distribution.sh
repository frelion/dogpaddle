#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
crate_dir="$(cd -- "$script_dir/.." && pwd)"
bridge_dir="$crate_dir/bridge"
distribution_dir="$bridge_dir/target/distribution"
probe_dir="$bridge_dir/target/lifecycle-probe"

for command in awk cp dirname find grep jar java mkdir mvn rm; do
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

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mvn --version
(
  cd -- "$bridge_dir"
  mvn --batch-mode --no-transfer-progress clean package
)
(
  cd -- "$bridge_dir/probe"
  mvn --batch-mode --no-transfer-progress clean package
)

rm -rf -- "$distribution_dir"
mkdir -p -- "$distribution_dir/lib"
cp -- \
  "$bridge_dir/target/dogpaddle-debezium-bridge-0.1.0.jar" \
  "$distribution_dir/lib/dogpaddle-debezium-bridge.jar"
cp -- "$bridge_dir"/target/dependency/*.jar "$distribution_dir/lib/"
cp -- "$bridge_dir/distribution/MANIFEST" "$distribution_dir/MANIFEST"
cp -- \
  "$bridge_dir/distribution/THIRD-PARTY-NOTICES.md" \
  "$distribution_dir/THIRD-PARTY-NOTICES.md"
cp -- "$bridge_dir/target/bom.json" "$distribution_dir/bom.json"

rm -rf -- "$probe_dir"
mkdir -p -- "$probe_dir"
cp -- \
  "$bridge_dir/probe/target/dogpaddle-debezium-lifecycle-probe.jar" \
  "$probe_dir/dogpaddle-debezium-lifecycle-probe.jar"

(
  cd -- "$distribution_dir"
  export LC_ALL=C
  for jar in lib/*.jar; do
    printf '%s  %s\n' "$(sha256_file "$jar")" "$jar"
  done
) >"$distribution_dir/SHA256SUMS"

if jar tf "$distribution_dir/lib/dogpaddle-debezium-bridge.jar" \
  | grep '^io/debezium/' >/dev/null; then
  echo 'bridge artifact must not contain copied or shadowed io.debezium classes' >&2
  exit 1
fi
if jar tf "$distribution_dir/lib/dogpaddle-debezium-bridge.jar" \
  | grep '^dev/dogpaddle/debezium/probe/' >/dev/null; then
  echo 'bridge artifact must not contain the lifecycle probe connector' >&2
  exit 1
fi
if ! jar tf "$probe_dir/dogpaddle-debezium-lifecycle-probe.jar" \
  | grep '^dev/dogpaddle/debezium/probe/LifecycleProbeConnector.class$' >/dev/null; then
  echo 'lifecycle probe artifact is missing its connector class' >&2
  exit 1
fi

test -s "$distribution_dir/SHA256SUMS"
test -s "$distribution_dir/bom.json"
test -f "$distribution_dir/lib/connect-api-4.3.0.jar"
test -f "$distribution_dir/lib/connect-json-4.3.0.jar"
test -f "$distribution_dir/lib/connect-runtime-4.3.0.jar"
test -f "$distribution_dir/lib/debezium-embedded-3.6.2.Final.jar"
test -f "$distribution_dir/lib/debezium-connector-postgres-3.6.2.Final.jar"
test -f "$distribution_dir/lib/slf4j-simple-1.7.36.jar"
test -s "$probe_dir/dogpaddle-debezium-lifecycle-probe.jar"
if find "$distribution_dir" -name '*lifecycle-probe*' -print | grep . >/dev/null; then
  echo 'lifecycle probe connector must remain outside the product distribution' >&2
  exit 1
fi

echo "PASS Java bridge tests and pinned PostgreSQL distribution: $distribution_dir"
echo "PASS separate lifecycle probe connector: $probe_dir"
