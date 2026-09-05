#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo 'usage: clean.sh' >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
gate_dir="$(cd -- "$script_dir/.." && pwd)"
readonly COMPOSE_NETWORK="dogpaddle-debezium-d1_default"

for command in flock podman; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if ! podman compose version >/dev/null 2>&1; then
  echo 'podman compose provider is unavailable' >&2
  exit 1
fi

exec 9>"${TMPDIR:-/tmp}/dogpaddle-debezium-d1.lock"
if ! flock --nonblock 9; then
  echo 'refusing to clean while the D1 fixture gate is running' >&2
  exit 1
fi

podman compose \
  --project-name dogpaddle-debezium-d1 \
  --file "$gate_dir/compose.yaml" \
  down --volumes --remove-orphans

if podman network exists "$COMPOSE_NETWORK"; then
  podman network rm "$COMPOSE_NETWORK"
fi
