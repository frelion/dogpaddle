#!/usr/bin/env bash
set -euo pipefail

readonly COMPOSE_PROJECT="dogpaddle-debezium-d1"
readonly COMPOSE_NETWORK="dogpaddle-debezium-d1_default"
readonly MAVEN_IMAGE="docker.io/library/maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
experiment_dir="$(cd -- "$script_dir/.." && pwd)"
repo_dir="$(cd -- "$experiment_dir/../.." && pwd)"
runtime_bundle="$repo_dir/crates/debezium/bridge/target/bundles/dogpaddle-debezium-runtime-x86_64-unknown-linux-gnu"
state_dir="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-debezium-d1-state.XXXXXX")"
artifacts_dir="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-debezium-d1-artifacts.XXXXXX")"
owns_postgres=0

cleanup() {
  local run_status=$?
  local cleanup_status=0
  trap - EXIT
  if [[ "${D1_KEEP_POSTGRES:-0}" != "1" ]]; then
    if [[ "$owns_postgres" == "1" ]]; then
      if ! podman compose \
        --project-name "$COMPOSE_PROJECT" \
        --file "$experiment_dir/compose.yaml" \
        down --volumes --remove-orphans >/dev/null 2>&1; then
        cleanup_status=1
        echo 'failed to remove the D1 PostgreSQL fixture' >&2
      fi
      if podman network exists "$COMPOSE_NETWORK" \
        && ! podman network rm "$COMPOSE_NETWORK" >/dev/null 2>&1; then
        cleanup_status=1
        echo 'failed to remove the D1 PostgreSQL fixture network' >&2
      fi
    fi
  elif [[ "$owns_postgres" == "1" ]]; then
    echo "D1 PostgreSQL retained as Compose project $COMPOSE_PROJECT" >&2
  fi
  if [[ "${D1_KEEP_ARTIFACTS:-0}" == "1" ]]; then
    echo "D1 state retained at $state_dir" >&2
    echo "D1 logs retained at $artifacts_dir" >&2
  else
    if ! rm -rf -- "$state_dir" "$artifacts_dir"; then
      cleanup_status=1
      echo 'failed to remove D1 temporary artifacts' >&2
    fi
  fi
  if [[ "$run_status" == "0" && "$cleanup_status" != "0" ]]; then
    exit "$cleanup_status"
  fi
  exit "$run_status"
}
trap cleanup EXIT

for command in podman psql python3 cargo git rg unzip flock; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done

if ! podman compose version >/dev/null 2>&1; then
  echo 'podman compose provider is unavailable' >&2
  exit 1
fi
if ! cargo clippy --version >/dev/null 2>&1; then
  echo 'the Rust Clippy component is unavailable' >&2
  exit 1
fi

exec 9>"${TMPDIR:-/tmp}/dogpaddle-debezium-d1.lock"
if ! flock --nonblock 9; then
  echo 'another D1 fixture command is already running' >&2
  exit 1
fi

existing_containers="$(podman ps \
  --all \
  --quiet \
  --filter "label=com.docker.compose.project=$COMPOSE_PROJECT")"
existing_volumes="$(podman volume ls \
  --quiet \
  --filter "label=com.docker.compose.project=$COMPOSE_PROJECT")"
existing_networks="$(podman network ls \
  --quiet \
  --filter "label=com.docker.compose.project=$COMPOSE_PROJECT")"
if [[ -n "$existing_containers" || -n "$existing_volumes" || -n "$existing_networks" ]]; then
  echo "refusing to overwrite existing Compose project $COMPOSE_PROJECT" >&2
  echo 'inspect it or remove it explicitly with experiments/debezium-d1/scripts/clean.sh' >&2
  exit 1
fi

"$script_dir/audit-debezium-source.sh"
"$script_dir/build.sh"

owns_postgres=1
podman compose \
  --project-name "$COMPOSE_PROJECT" \
  --file "$experiment_dir/compose.yaml" \
  up --detach postgres

for _ in $(seq 1 30); do
  if PGPASSWORD=dogpaddle_d1 psql \
    -X \
    --host 127.0.0.1 \
    --port 55432 \
    --username dogpaddle_d1 \
    --dbname dogpaddle_d1 \
    --quiet \
    --command 'SELECT 1' >/dev/null 2>&1; then
    postgres_ready=1
    break
  fi
  sleep 1
done
if [[ "${postgres_ready:-0}" != "1" ]]; then
  echo "PostgreSQL did not become ready" >&2
  exit 1
fi

python3 "$experiment_dir/tests/d1_blackbox.py" \
  --pg-port 55432 \
  --flush-interval-ms 500 \
  --flush-intervals 4 \
  --state-dir "$state_dir" \
  --artifacts-dir "$artifacts_dir" \
  --connector-fixture "$experiment_dir/tests/fixtures/connector.json" \
  -- \
  podman run --rm --interactive \
    --network host \
    --env PATH=/nonexistent \
    --env JAVA_HOME=/nonexistent \
    --env JDK_HOME=/nonexistent \
    --env JAVA_TOOL_OPTIONS= \
    --env JDK_JAVA_OPTIONS= \
    --env _JAVA_OPTIONS= \
    --env LD_LIBRARY_PATH= \
    --env DYLD_LIBRARY_PATH= \
    --env DYLD_FALLBACK_LIBRARY_PATH= \
    --volume "$runtime_bundle:/opt/d1/bundle:ro,Z" \
    --volume "$experiment_dir/tests/fixtures/connector.json:/opt/d1/connector.json:ro,Z" \
    --volume "$state_dir:/state:Z" \
    --entrypoint /opt/d1/bundle/bin/dogpaddle-debezium-d1-host \
    "$MAVEN_IMAGE" \
    --bundle /opt/d1/bundle \
    --config /opt/d1/connector.json \
    --checkpoint /state/checkpoint.bin
