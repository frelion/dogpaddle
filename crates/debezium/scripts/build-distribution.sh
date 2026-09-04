#!/usr/bin/env bash
set -euo pipefail

readonly MAVEN_IMAGE="docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
crate_dir="$(cd -- "$script_dir/.." && pwd)"
bridge_dir="$crate_dir/bridge"
distribution_dir="$bridge_dir/target/distribution"

for command in awk cp dirname grep id mkdir rm unzip; do
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

run_local_maven() {
  local executable="${DOGPADDLE_MAVEN_EXECUTABLE:-mvn}"
  if ! command -v "$executable" >/dev/null 2>&1; then
    echo "missing Maven executable: $executable" >&2
    exit 1
  fi
  if ! command -v java >/dev/null 2>&1; then
    echo 'a JDK is required to build the Java bridge locally' >&2
    exit 1
  fi
  "$executable" --version
  (
    cd -- "$bridge_dir"
    "$executable" --batch-mode --no-transfer-progress clean package
  )
}

run_container_maven() {
  local engine="$1"
  local volume="$bridge_dir:/workspace:Z"
  local host_uid
  local host_gid
  local -a ownership_args
  host_uid="$(id -u)"
  host_gid="$(id -g)"
  ownership_args=(--user "$host_uid:$host_gid")
  if [[ "$engine" == "podman" ]]; then
    if [[ "$(podman info --format '{{.Host.Security.Rootless}}')" == "true" ]]; then
      ownership_args=(--userns=keep-id --user "$host_uid:$host_gid")
    fi
  fi
  "$engine" run --rm \
    "${ownership_args[@]}" \
    --env HOME=/tmp/dogpaddle-maven-home \
    --env MAVEN_CONFIG=/tmp/dogpaddle-maven-home/.m2 \
    --volume "$volume" \
    --workdir /workspace \
    "$MAVEN_IMAGE" \
    sh -eu -c \
    'mkdir -p "$HOME/.m2/repository" && exec mvn --batch-mode --no-transfer-progress -Dmaven.repo.local="$HOME/.m2/repository" clean package'
}

maven_mode="${DOGPADDLE_MAVEN_MODE:-auto}"
case "$maven_mode" in
  local)
    run_local_maven
    ;;
  container)
    if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
      run_container_maven podman
    elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      run_container_maven docker
    else
      echo 'DOGPADDLE_MAVEN_MODE=container requires a running Podman or Docker engine' >&2
      exit 1
    fi
    ;;
  auto)
    if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
      run_container_maven podman
    elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      run_container_maven docker
    else
      run_local_maven
    fi
    ;;
  *)
    echo "unsupported DOGPADDLE_MAVEN_MODE: $maven_mode" >&2
    exit 1
    ;;
esac

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
  for jar in lib/*.jar; do
    printf '%s  %s\n' "$(sha256_file "$jar")" "$jar"
  done
) >"$distribution_dir/SHA256SUMS"

if unzip -Z1 "$distribution_dir/lib/dogpaddle-debezium-bridge.jar" \
  | grep '^io/debezium/' >/dev/null; then
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
