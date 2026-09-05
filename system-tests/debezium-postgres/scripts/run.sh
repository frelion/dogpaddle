#!/usr/bin/env bash
set -euo pipefail

readonly COMPOSE_PROJECT="dogpaddle-debezium-d1"
readonly COMPOSE_NETWORK="dogpaddle-debezium-d1_default"
readonly POSTGRES_IMAGE="quay.io/debezium/postgres:16@sha256:114cbe1e4f38055e83c9b567a7e0988fb80837b8eb500203b25c0f784a075b92"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
gate_dir="$(cd -- "$script_dir/.." && pwd)"
runtime_bundle=""
host_binary=""
artifacts_dir=""
temporary_root=""
owns_postgres=0

usage() {
  echo 'usage: run.sh --bundle ABSOLUTE_PATH --host ABSOLUTE_PATH [--artifacts-dir ABSOLUTE_PATH]' >&2
  exit 2
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --bundle)
      [[ -z "$runtime_bundle" && "$#" -ge 2 ]] || usage
      runtime_bundle="$2"
      shift 2
      ;;
    --host)
      [[ -z "$host_binary" && "$#" -ge 2 ]] || usage
      host_binary="$2"
      shift 2
      ;;
    --artifacts-dir)
      [[ -z "$artifacts_dir" && "$#" -ge 2 ]] || usage
      artifacts_dir="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[[ -n "$runtime_bundle" && -n "$host_binary" ]] || usage
if [[ "$runtime_bundle" != /* || "$host_binary" != /* ]]; then
  echo '--bundle and --host must be absolute paths' >&2
  exit 2
fi
if [[ -n "$artifacts_dir" && "$artifacts_dir" != /* ]]; then
  echo '--artifacts-dir must be an absolute path' >&2
  exit 2
fi
if [[ ! -d "$runtime_bundle" || ! -f "$runtime_bundle/runtime/lib/server/libjvm.so" ]]; then
  echo "not a built Linux x86_64 Debezium runtime bundle: $runtime_bundle" >&2
  exit 1
fi
if [[ ! -f "$host_binary" || ! -x "$host_binary" ]]; then
  echo "not an executable D1 host: $host_binary" >&2
  exit 1
fi

for command in env flock mkdir mktemp podman psql python3 seq sleep tee; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if ! podman compose version >/dev/null 2>&1; then
  echo 'podman compose provider is unavailable' >&2
  exit 1
fi
if ! podman image exists "$POSTGRES_IMAGE"; then
  echo "missing preloaded PostgreSQL fixture image: $POSTGRES_IMAGE" >&2
  echo 'run scripts/check.sh or pull the pinned image before scripts/run.sh' >&2
  exit 1
fi

if [[ -z "$artifacts_dir" ]]; then
  temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-debezium-postgres.XXXXXX")"
  artifacts_dir="$temporary_root/artifacts"
fi
mkdir -p -- "$artifacts_dir"
state_dir="$artifacts_dir/state"
if [[ -e "$state_dir" || -e "$artifacts_dir/host.stderr.log" ]]; then
  echo "refusing to overwrite an existing D1 scenario: $artifacts_dir" >&2
  exit 1
fi
mkdir -p -- "$state_dir"

cleanup() {
  local run_status=$?
  local cleanup_status=0
  trap - EXIT
  if [[ "$owns_postgres" == "1" ]]; then
    podman compose \
      --project-name "$COMPOSE_PROJECT" \
      --file "$gate_dir/compose.yaml" \
      ps --all >"$artifacts_dir/postgres-status.log" 2>&1 || true
    podman compose \
      --project-name "$COMPOSE_PROJECT" \
      --file "$gate_dir/compose.yaml" \
      logs --no-color postgres >"$artifacts_dir/postgres.log" 2>&1 || true
    if ! podman compose \
      --project-name "$COMPOSE_PROJECT" \
      --file "$gate_dir/compose.yaml" \
      down --volumes --remove-orphans >"$artifacts_dir/cleanup.log" 2>&1; then
      cleanup_status=1
      echo 'failed to remove the D1 PostgreSQL fixture' >&2
    fi
    if podman network exists "$COMPOSE_NETWORK" \
      && ! podman network rm "$COMPOSE_NETWORK" >>"$artifacts_dir/cleanup.log" 2>&1; then
      cleanup_status=1
      echo 'failed to remove the D1 PostgreSQL fixture network' >&2
    fi
  fi
  if [[ -n "$temporary_root" ]]; then
    rm -rf -- "$temporary_root"
  else
    echo "D1 artifacts: $artifacts_dir" >&2
  fi
  if [[ "$run_status" == "0" && "$cleanup_status" != "0" ]]; then
    exit "$cleanup_status"
  fi
  exit "$run_status"
}
trap cleanup EXIT

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
  echo 'inspect it or remove it explicitly with system-tests/debezium-postgres/scripts/clean.sh' >&2
  exit 1
fi

owns_postgres=1
podman compose \
  --project-name "$COMPOSE_PROJECT" \
  --file "$gate_dir/compose.yaml" \
  up --detach --pull never postgres

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

env_command="$(command -v env)"
set -o pipefail
python3 "$gate_dir/gate.py" \
  --pg-port 55432 \
  --flush-interval-ms 500 \
  --flush-intervals 4 \
  --state-dir "$state_dir" \
  --artifacts-dir "$artifacts_dir" \
  --connector-fixture "$gate_dir/fixtures/connector.json" \
  -- \
  "$env_command" \
    PATH=/nonexistent \
    JAVA_HOME=/nonexistent \
    JDK_HOME=/nonexistent \
    JAVA_TOOL_OPTIONS= \
    JDK_JAVA_OPTIONS= \
    _JAVA_OPTIONS= \
    LD_LIBRARY_PATH= \
    DYLD_LIBRARY_PATH= \
    DYLD_FALLBACK_LIBRARY_PATH= \
    "$host_binary" \
    --bundle "$runtime_bundle" \
    --config "$gate_dir/fixtures/connector.json" \
    --checkpoint "$state_dir/checkpoint.bin" \
  2>&1 | tee "$artifacts_dir/gate.log"
