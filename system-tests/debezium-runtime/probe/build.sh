#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/../../.." && pwd)"
project_dir="$script_dir"
output_dir="$repo_dir/target/debezium-runtime-probe"
artifact_name="dogpaddle-debezium-lifecycle-probe.jar"

for command in cp grep jar mkdir mv mvn rm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done

(
  cd -- "$project_dir"
  mvn --batch-mode --no-transfer-progress clean package
)

staging_dir="$repo_dir/target/debezium-runtime-probe.new"
rm -rf -- "$staging_dir"
mkdir -p -- "$staging_dir"
cp -- "$project_dir/target/$artifact_name" "$staging_dir/$artifact_name"

if ! jar tf "$staging_dir/$artifact_name" \
  | grep '^dev/dogpaddle/debezium/probe/LifecycleProbeConnector.class$' >/dev/null; then
  echo 'lifecycle probe artifact is missing its connector class' >&2
  exit 1
fi

rm -rf -- "$output_dir"
mv -- "$staging_dir" "$output_dir"
echo "PASS separate lifecycle probe connector: $output_dir/$artifact_name"
