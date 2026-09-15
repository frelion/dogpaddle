#!/usr/bin/env bash

set -euo pipefail

script_path="${BASH_SOURCE[0]}"
script_directory="$(cd -- "$(dirname -- "$script_path")" && pwd -P)"
repository="$(cd "$script_directory/../.." && pwd -P)"

usage() {
    echo "usage: $(basename -- "$script_path") --dogpaddle ABSOLUTE_BINARY --bundle ABSOLUTE_RUNTIME_BUNDLE --postgres-bin ABSOLUTE_POSTGRES_BIN" >&2
}

dogpaddle=""
bundle=""
postgres_bin=""
while (( $# > 0 )); do
    case "$1" in
        --dogpaddle)
            (( $# >= 2 )) || { usage; exit 2; }
            dogpaddle="${2:-}"
            shift 2
            ;;
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

if [[ -z "$dogpaddle" || -z "$bundle" || -z "$postgres_bin" || "$dogpaddle" != /* || "$bundle" != /* || "$postgres_bin" != /* ]]; then
    usage
    exit 2
fi

cd "$repository"

for command in python3 asciinema agg tmux; do
    if ! command -v "$command" >/dev/null; then
        echo "missing command: $command" >&2
        exit 1
    fi
done

python3 docs/tools/record_readme_examples.py \
    --bundle "$bundle" \
    --postgres-bin "$postgres_bin" \
    --dogpaddle "$dogpaddle" \
    --asciinema "$(command -v asciinema)" \
    --agg "$(command -v agg)" \
    --tmux "$(command -v tmux)" \
    --output-dir "$repository/docs/assets"
