#!/usr/bin/env bash
set -euo pipefail

readonly DEBEZIUM_TAG="v3.6.2.Final"
readonly DEBEZIUM_COMMIT="02810e25b19c04e5095b2b6fbbdcbae549a69f19"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
experiment_dir="$(cd -- "$script_dir/.." && pwd)"
audit_root="$(mktemp -d "${TMPDIR:-/tmp}/dogpaddle-debezium-d1-audit.XXXXXX")"
trap 'rm -rf -- "$audit_root"' EXIT

git clone \
  --depth 1 \
  --branch "$DEBEZIUM_TAG" \
  --filter=blob:none \
  --sparse \
  https://github.com/debezium/debezium.git \
  "$audit_root/debezium"

actual_commit="$(git -C "$audit_root/debezium" rev-parse HEAD)"
if [[ "$actual_commit" != "$DEBEZIUM_COMMIT" ]]; then
  echo "FAIL source revision: expected $DEBEZIUM_COMMIT, found $actual_commit" >&2
  exit 1
fi

git -C "$audit_root/debezium" sparse-checkout set \
  debezium-api \
  debezium-embedded \
  debezium-connector-common \
  debezium-connector-postgres

readonly async_engine="$audit_root/debezium/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java"
readonly pg_config="$audit_root/debezium/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java"
readonly pg_stream="$audit_root/debezium/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/PostgresReplicationConnection.java"
readonly pg_source="$audit_root/debezium/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresStreamingChangeEventSource.java"

require_source() {
  local pattern="$1"
  local path="$2"
  local label="$3"
  if ! rg --fixed-strings --quiet -- "$pattern" "$path"; then
    echo "FAIL $label: expected source pattern was not found" >&2
    exit 1
  fi
  echo "PASS $label"
}

require_source \
  'offsetWriter.offset(record.sourcePartition(), record.sourceOffset());' \
  "$async_engine" \
  'markProcessed records the complete partition and offset'
require_source \
  'if (offsetCommitPolicy.performCommit(recordsSinceLastCommit, durationSinceLastCommit))' \
  "$async_engine" \
  'markBatchFinished gates offset-store flush through the public commit policy'
require_source \
  'task.commit();' \
  "$async_engine" \
  'a successful offset-store flush requests the connector commit callback'
require_source \
  'CONNECTOR_AND_DRIVER("connector_and_driver")' \
  "$pg_config" \
  'the driver-managed LSN mode exists and must be excluded by D1'
require_source \
  'mode = LsnFlushMode.CONNECTOR; // Use default' \
  "$pg_config" \
  'connector-only LSN flushing is the PostgreSQL connector default'
require_source \
  'boolean enableDriverKeepaliveFlush = (connectorConfig.getLsnFlushMode() == PostgresConnectorConfig.LsnFlushMode.CONNECTOR_AND_DRIVER);' \
  "$pg_stream" \
  'only connector_and_driver enables pgjdbc automatic LSN flush'
require_source \
  'withAutomaticFlush(enableDriverKeepaliveFlush)' \
  "$pg_stream" \
  'pgjdbc automatic LSN flush is selected by connector configuration'
require_source \
  'replicationStream.flushLsn(lsn);' \
  "$pg_source" \
  'the PostgreSQL commit callback advances the replication stream LSN'

if find "$experiment_dir/bridge/src" -type f -path '*/io/debezium/*' -print -quit | rg --quiet .; then
  echo 'FAIL local Debezium class shadowing: found source below io/debezium' >&2
  exit 1
fi
echo 'PASS the experiment does not copy or shadow io.debezium classes'

if rg --glob '*.java' --quiet \
  '^[[:space:]]*package[[:space:]]+io[.]debezium([.;])' \
  "$experiment_dir/bridge/src"; then
  echo 'FAIL local Debezium class shadowing: found an io.debezium package declaration' >&2
  exit 1
fi
echo 'PASS no Java source declares an io.debezium package'

if rg --glob '*.java' --quiet \
  'System[.]load(Library)?[(]|RegisterNatives|[[:space:]]native[[:space:]]+[A-Za-z_$][A-Za-z0-9_.$<>?, \[\]]*[[:space:]]+[A-Za-z_$][A-Za-z0-9_$]*[[:space:]]*[(]' \
  "$experiment_dir/bridge/src"; then
  echo 'FAIL Java-to-Rust callback boundary: found native loading or a native method' >&2
  exit 1
fi
echo 'PASS the Java bridge declares no native callback into Rust'

if ! rg --fixed-strings --line-regexp --quiet \
  'unsafe_code = "forbid"' \
  "$experiment_dir/host/Cargo.toml"; then
  echo 'FAIL Rust host does not forbid unsafe code at the crate level' >&2
  exit 1
fi
echo 'PASS the Rust host crate forbids unsafe code'

echo "PASS Debezium source audit $DEBEZIUM_TAG $DEBEZIUM_COMMIT"
