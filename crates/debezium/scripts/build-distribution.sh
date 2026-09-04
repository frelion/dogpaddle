#!/usr/bin/env bash
set -euo pipefail

readonly MAVEN_IMAGE="docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
crate_dir="$(cd -- "$script_dir/.." && pwd)"
bridge_dir="$crate_dir/bridge"
distribution_dir="$bridge_dir/target/distribution"

podman run --rm \
  --volume "$bridge_dir:/workspace:Z" \
  --workdir /workspace \
  "$MAVEN_IMAGE" \
  mvn --batch-mode --no-transfer-progress clean package

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

(
  cd -- "$distribution_dir"
  while IFS= read -r jar; do
    sha256sum "$jar"
  done < <(find lib -maxdepth 1 -type f -name '*.jar' -print | LC_ALL=C sort)
) >"$distribution_dir/SHA256SUMS"

if unzip -Z1 "$distribution_dir/lib/dogpaddle-debezium-bridge.jar" \
  | rg '^io/debezium/' >/dev/null; then
  echo 'bridge artifact must not contain copied or shadowed io.debezium classes' >&2
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

echo "PASS Java bridge tests and pinned PostgreSQL distribution: $distribution_dir"
