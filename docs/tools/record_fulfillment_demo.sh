#!/usr/bin/env bash

set -euo pipefail

script_path="${BASH_SOURCE[0]}"
script_directory="$(cd -- "$(dirname -- "$script_path")" && pwd -P)"
repository="$(cd "$script_directory/../.." && pwd -P)"

usage() {
    echo "usage: $(basename -- "$script_path") --bundle ABSOLUTE_RUNTIME_BUNDLE --postgres-bin ABSOLUTE_POSTGRES_BIN" >&2
}

bundle=""
postgres_bin=""
while (( $# > 0 )); do
    case "$1" in
        --bundle)
            (( $# >= 2 )) || { usage; exit 2; }
            bundle="${2:-}"
            shift 2
            ;;
        --postgres-bin)
            (( $# >= 2 )) || { usage; exit 2; }
            postgres_bin="${2:-}"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$bundle" || -z "$postgres_bin" || "$bundle" != /* || "$postgres_bin" != /* ]]; then
    usage
    exit 2
fi

if [[ ! -d "$bundle" ]]; then
    echo "runtime bundle is missing: $bundle" >&2
    exit 1
fi
if [[ ! -d "$postgres_bin" ]]; then
    echo "PostgreSQL bin directory is missing: $postgres_bin" >&2
    exit 1
fi
bundle="$(cd -- "$bundle" && pwd -P)"
postgres_bin="$(cd -- "$postgres_bin" && pwd -P)"
for executable in initdb pg_ctl postgres psql; do
    if [[ ! -x "$postgres_bin/$executable" ]]; then
        echo "PostgreSQL executable is missing: $postgres_bin/$executable" >&2
        exit 1
    fi
done
for command in cargo python3 uv; do
    if ! command -v "$command" >/dev/null; then
        echo "missing command: $command" >&2
        exit 1
    fi
done

staging="$(mktemp -d /tmp/dogpaddle-fulfillment-demo.XXXXXX)"
trap 'rm -rf -- "$staging"' EXIT HUP INT TERM
trace="$staging/trace.json"
poster="$repository/docs/assets/fulfillment-hero.png"
video="$repository/target/demo/fulfillment-demo.mp4"

cd "$repository"

# The renderer never invents business rows. The real PostgreSQL gate must pass
# and atomically publish all eight source/target snapshots first.
python3 system-tests/postgres/check_sql.py \
    --bundle "$bundle" \
    --postgres-bin "$postgres_bin" \
    --trace-output "$trace"

uv run --quiet --with 'playwright==1.60.0' playwright install chromium
uv run --quiet --script docs/tools/render_fulfillment_demo.py \
    --trace "$trace" \
    --html docs/demo/fulfillment.html \
    --poster "$poster" \
    --video "$video"

echo "README poster: $poster"
echo "upload-ready video: $video"
