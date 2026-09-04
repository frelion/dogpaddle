#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
experiment_dir="$(cd -- "$script_dir/.." && pwd)"
readonly COMPOSE_NETWORK="dogpaddle-debezium-d1_default"

exec 9>"${TMPDIR:-/tmp}/dogpaddle-debezium-d1.lock"
if ! flock --nonblock 9; then
  echo 'refusing to clean while the D1 fixture gate is running' >&2
  exit 1
fi

podman compose \
  --project-name dogpaddle-debezium-d1 \
  --file "$experiment_dir/compose.yaml" \
  down --volumes --remove-orphans

if podman network exists "$COMPOSE_NETWORK"; then
  podman network rm "$COMPOSE_NETWORK"
fi
