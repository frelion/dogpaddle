#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo 'usage: verify-release.sh TARGET ARCHIVE RUST_PROBE PROBE_JAR SCRATCH_DIR' >&2
  exit 2
fi

target="$1"
archive="$2"
probe="$3"
probe_connector="$4"
scratch_dir="$5"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
archive_name="$(basename -- "$archive")"
release_name="${archive_name%.tar.gz}"
checksum="$archive.sha256"

for path in "$archive" "$checksum" "$probe" "$probe_connector"; do
  if [[ ! -f "$path" ]]; then
    echo "missing release verification input: $path" >&2
    exit 1
  fi
done

(
  cd -- "$(dirname -- "$archive")"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check "$(basename -- "$checksum")"
  else
    shasum -a 256 --check "$(basename -- "$checksum")"
  fi
)

relocated_parent="$scratch_dir/DogPaddle release smoke $target"
if [[ -e "$relocated_parent" ]]; then
  echo "refusing to replace existing release directory: $relocated_parent" >&2
  exit 1
fi
mkdir -p -- "$relocated_parent"
tar -xzf "$archive" -C "$relocated_parent"
release_root="$relocated_parent/$release_name"
runtime="$release_root/libexec/dogpaddle/debezium"
dogpaddle="$release_root/bin/dogpaddle"

if [[ ! -x "$dogpaddle" || ! -d "$runtime" ]]; then
  echo "invalid DogPaddle release layout: $release_root" >&2
  exit 1
fi

"$script_dir/install.sh" "$runtime" "$probe_connector"
chmod -R a-w -- "$release_root"

empty_path="$scratch_dir/dogpaddle-empty-path-$target"
missing_java="$scratch_dir/dogpaddle-missing-java-$target"
work="$scratch_dir/dogpaddle-product-smoke-$target"
mkdir -p -- "$empty_path" "$work"
unset DOGPADDLE_DEBEZIUM_RUNTIME JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
PATH="$empty_path" \
JAVA_HOME="$missing_java" \
JDK_HOME="$missing_java" \
LD_LIBRARY_PATH= \
DYLD_LIBRARY_PATH= \
DYLD_FALLBACK_LIBRARY_PATH= \
  "$probe" "$runtime"

resolver_sql="$work/runtime-path.sql"
resolver_state="$work/runtime-path-state"
resolver_log="$work/runtime-path.log"
printf '%s\n' \
  "INSERT INTO discard() SELECT * FROM postgres_cdc(connection => 'postgresql://user:pass@127.0.0.1:1/db', table => 'public.items', publication => 'dogpaddle_probe', connect_timeout_ms => 100, query_timeout_ms => 100, retry_limit => 0);" \
  >"$resolver_sql"
if PATH="$empty_path" \
  JAVA_HOME="$missing_java" \
  JDK_HOME="$missing_java" \
  LD_LIBRARY_PATH= \
  DYLD_LIBRARY_PATH= \
  DYLD_FALLBACK_LIBRARY_PATH= \
    "$dogpaddle" run "$resolver_sql" --state "$resolver_state" \
    >"$resolver_log" 2>&1; then
  echo 'runtime path smoke unexpectedly connected to the unreachable PostgreSQL endpoint' >&2
  exit 1
fi
if ! grep -Fq 'PostgreSQL connect failed' "$resolver_log"; then
  echo 'DogPaddle did not reach PostgreSQL discovery through its default bundled runtime path' >&2
  cat "$resolver_log" >&2
  exit 1
fi
if [[ -e "$resolver_state" ]]; then
  echo 'runtime path smoke created state before endpoint discovery succeeded' >&2
  exit 1
fi

sql="$work/pipeline.sql"
state="$work/state"
log="$work/dogpaddle.log"
touch "$log"
printf '%s\n' \
  "INSERT INTO discard() SELECT value FROM sequence(start => 0);" >"$sql"

product_pid=""
cleanup_product() {
  if [[ -n "$product_pid" ]] && kill -0 "$product_pid" 2>/dev/null; then
    kill -TERM "$product_pid" 2>/dev/null || true
    for _ in {1..50}; do
      if ! kill -0 "$product_pid" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "$product_pid" 2>/dev/null; then
      kill -KILL "$product_pid" 2>/dev/null || true
    fi
  fi
  if [[ -n "$product_pid" ]]; then
    wait "$product_pid" 2>/dev/null || true
    product_pid=""
  fi
}
trap cleanup_product EXIT

run_product() {
  prior_starts="$(awk '/^state: / { count += 1 } END { print count + 0 }' "$log")"
  expected_starts="$((prior_starts + 1))"
  PATH="$empty_path" \
  JAVA_HOME="$missing_java" \
  JDK_HOME="$missing_java" \
  LD_LIBRARY_PATH= \
  DYLD_LIBRARY_PATH= \
  DYLD_FALLBACK_LIBRARY_PATH= \
    "$dogpaddle" run "$sql" --state "$state" >>"$log" 2>&1 &
  product_pid=$!
  for _ in {1..300}; do
    starts="$(awk '/^state: / { count += 1 } END { print count + 0 }' "$log")"
    if [[ "$starts" -ge "$expected_starts" ]]; then
      break
    fi
    if ! kill -0 "$product_pid" 2>/dev/null; then
      wait "$product_pid" || true
      product_pid=""
      return 1
    fi
    sleep 0.1
  done
  if [[ "$starts" -lt "$expected_starts" ]]; then
    cleanup_product
    echo 'DogPaddle did not create state within 30 seconds' >&2
    return 1
  fi
  kill -INT "$product_pid"
  for _ in {1..300}; do
    if ! kill -0 "$product_pid" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  if kill -0 "$product_pid" 2>/dev/null; then
    echo 'DogPaddle did not stop within 30 seconds after Ctrl-C' >&2
    cleanup_product
    return 1
  fi
  if ! wait "$product_pid"; then
    product_pid=""
    echo 'DogPaddle exited unsuccessfully after Ctrl-C' >&2
    return 1
  fi
  product_pid=""
}

run_product
run_product
if [[ "$(awk '/^state: / { count += 1 } END { print count + 0 }' "$log")" -ne 2 ]]; then
  echo 'DogPaddle did not build and reopen the product smoke state' >&2
  exit 1
fi

echo "PASS relocated, read-only DogPaddle release build and reopen: $archive"
