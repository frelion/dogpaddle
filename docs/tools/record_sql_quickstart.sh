#!/bin/zsh

set -eu

script_path="${0:A}"
script_directory="${script_path:h}"
repository="$(cd "$script_directory/../.." && pwd -P)"

if [[ "${1:-}" != "--session" ]]; then
    raw_cast="$(mktemp /tmp/dogpaddle-sql-quickstart.XXXXXX.cast)"
    clean_cast="$(mktemp /tmp/dogpaddle-sql-quickstart-clean.XXXXXX.cast)"
    output="$repository/docs/assets/sql-quickstart.gif"

    cleanup_recording() {
        rm -f -- "$raw_cast" "$clean_cast"
    }
    trap cleanup_recording EXIT

    for command in asciinema agg tmux sqlite3 cargo; do
        if ! command -v "$command" >/dev/null; then
            echo "missing command: $command" >&2
            echo "requires asciinema, agg, tmux, sqlite3 and cargo" >&2
            echo "on macOS: brew install asciinema agg tmux sqlite" >&2
            exit 1
        fi
    done

    cd "$repository"
    cargo build -q -p dogpaddle-sql --example quickstart --locked
    record_command="/bin/zsh ${(q)script_path} --session"
    TERM=xterm-256color asciinema record -q --headless --overwrite \
        --window-size 126x32 --command "$record_command" "$raw_cast"

    # tmux restores the alternate screen when it exits. Without this trim,
    # the GIF would end on the blank screen that preceded the recording.
    awk 'index($0, "?1049l") { exit } { print }' "$raw_cast" > "$clean_cast"
    agg --quiet --font-size 18 --line-height 1.1 --theme github-dark \
        --fps-cap 12 --idle-time-limit 1.5 --last-frame-duration 2 \
        "$clean_cast" "$output"
    echo "recorded $output"
    exit 0
fi

session="dogpaddle-sql-readme-$$"
demo_root="$(mktemp -d /tmp/dogpaddle-sql-live.XXXXXX)"
demo_root="$(cd "$demo_root" && pwd -P)"
database="$demo_root/results.sqlite"
state="$demo_root/flow"
left="$session:0.0"
right="$session:0.1"
query='SELECT "$dogpaddle.id" id, number, square, size FROM even_squares ORDER BY id'
build_command='cargo run --locked -q -p dogpaddle-sql --example quickstart -- build crates/sql/examples/quickstart.sql "$DOGPADDLE_QUICKSTART_STATE" 12 350'
open_command='cargo run --locked -q -p dogpaddle-sql --example quickstart -- open crates/sql/examples/quickstart.sql "$DOGPADDLE_QUICKSTART_STATE" 6 350'

run_query() {
    tmux send-keys -t "$right" -l "$query"
    tmux send-keys -t "$right" -H 3b
    tmux send-keys -t "$right" Enter
}

wait_for_left_shell() {
    sleep 0.2
    while [[ "$(tmux display-message -p -t "$left" '#{pane_current_command}')" != "bash" ]]; do
        sleep 0.05
    done
}

cleanup() {
    tmux kill-session -t "$session" 2>/dev/null || true
    rm -rf -- "$demo_root"
}
trap cleanup EXIT

shell="env BASH_SILENCE_DEPRECATION_WARNING=1 \
DOGPADDLE_QUICKSTART_SQLITE='$database' \
DOGPADDLE_QUICKSTART_STATE='$state' \
PS1='$ ' /bin/bash --noprofile --norc"

tmux new-session -d -s "$session" -x 126 -y 32 -c "$repository" "$shell"
tmux set-option -t "$session" status off
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux set-option -t "$session" pane-border-format \
    '#[fg=colour39,bold] #{pane_title} #[default]'
tmux split-window -h -p 42 -t "$left" -c "$repository" "$shell"
tmux select-pane -t "$left" -T 'ONE SQL FILE / build + open'
tmux select-pane -t "$right" -T 'SQLITE / committed result'
tmux select-pane -t "$left"

(
    sleep 0.5
    tmux send-keys -t "$right" -l "echo 'waiting for the first committed row...'"
    tmux send-keys -t "$right" Enter
    tmux send-keys -t "$left" -l "sed -n '1,18p' crates/sql/examples/quickstart.sql"
    tmux send-keys -t "$left" Enter
    sleep 2

    tmux send-keys -t "$left" C-l
    tmux send-keys -t "$left" -l "$build_command"
    tmux send-keys -t "$left" Enter
    while [ ! -f "$database" ]; do
        sleep 0.05
    done
    while [[ "$(sqlite3 -readonly "$database" \
        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='even_squares' LIMIT 1;" \
        2>/dev/null)" != "1" ]]; do
        sleep 0.05
    done
    tmux send-keys -t "$right" C-l
    tmux send-keys -t "$right" -l "sqlite3 -readonly -box '$database'"
    tmux send-keys -t "$right" Enter
    sleep 0.15
    for _ in {1..5}; do
        tmux send-keys -t "$right" C-l
        run_query
        sleep 0.8
    done
    wait_for_left_shell

    tmux send-keys -t "$left" C-l
    tmux send-keys -t "$left" -l \
        "echo 'build process exited; reopening the same durable Flow...'"
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$left" -l "$open_command"
    tmux send-keys -t "$left" Enter
    for _ in {1..3}; do
        tmux send-keys -t "$right" C-l
        run_query
        sleep 0.8
    done
    wait_for_left_shell
    tmux send-keys -t "$right" C-l
    run_query
    sleep 2.5
    tmux kill-session -t "$session" 2>/dev/null || true
) &

tmux attach-session -t "$session"
