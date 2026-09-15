#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
COMPOSE_FILE="$ROOT/system-tests/warehouse-sinks/compose.yaml"
ENGINE=${CONTAINER_ENGINE:-docker}
PROJECT=dogpaddle-warehouse-sinks

compose() {
  "$ENGINE" compose --project-name "$PROJECT" --file "$COMPOSE_FILE" "$@"
}

cleanup() {
  compose down --volumes --remove-orphans
}
trap cleanup EXIT

compose up --detach

for _ in $(seq 1 180); do
  if compose exec --no-TTY clickhouse clickhouse-client \
      --user dogpaddle --password dogpaddle --query 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
compose exec --no-TTY clickhouse clickhouse-client \
  --user dogpaddle --password dogpaddle --query 'SELECT 1' >/dev/null

for _ in $(seq 1 300); do
  if compose exec --no-TTY doris mysql -h127.0.0.1 -P9030 -uroot \
      -e 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
compose exec --no-TTY doris mysql -h127.0.0.1 -P9030 -uroot \
  -e 'CREATE DATABASE IF NOT EXISTS dogpaddle' >/dev/null

cd "$ROOT"
cargo test -p dogpaddle-operation --lib \
  operation::sink::clickhouse::target::live_tests::adapter_replay_is_convergent_and_rejects_id_rebinding \
  -- --ignored --exact
cargo test -p dogpaddle-operation --lib \
  operation::sink::clickhouse::target::live_tests::adapter_batches_maximum_distinct_lookup_over_historical_state \
  -- --ignored --exact
cargo test -p dogpaddle-operation --lib \
  operation::sink::doris::target::live_tests::adapter_replay_is_convergent_and_rejects_id_rebinding \
  -- --ignored --exact
cargo test -p dogpaddle-operation --lib \
  operation::sink::doris::target::live_tests::wide_batch_is_split_inside_one_explicit_transaction \
  -- --ignored --exact
