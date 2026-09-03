#!/bin/zsh

set -eu

script_path="${0:A}"
script_directory="${script_path:h}"
repository="$(cd "$script_directory/.." && pwd -P)"

if [[ "${1:-}" != "--session" ]]; then
    raw_cast="$(mktemp /tmp/dogpaddle-sqlite-live.XXXXXX.cast)"
    clean_cast="$(mktemp /tmp/dogpaddle-sqlite-live-clean.XXXXXX.cast)"
    output="$repository/docs/assets/sqlite-sink-live.gif"

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
    cargo build -q -p dogpaddle-flow --example sqlite_sink_live --locked
    record_command="/bin/zsh ${(q)script_path} --session"
    TERM=xterm-256color asciinema record -q --headless --overwrite \
        --window-size 116x30 --command "$record_command" "$raw_cast"

    # tmux restores the alternate screen when it exits. That final escape
    # sequence would otherwise replace the useful last frame with a blank one.
    awk 'index($0, "?1049l") { exit } { print }' "$raw_cast" > "$clean_cast"
    agg --quiet --font-size 18 --line-height 1.1 --theme github-dark \
        --fps-cap 12 --idle-time-limit 1 --last-frame-duration 2 \
        "$clean_cast" "$output"
    echo "recorded $output"
    exit 0
fi

session="dogpaddle-readme-$$"
demo_root="$(mktemp -d /tmp/dogpaddle-live.XXXXXX)"
demo_root="$(cd "$demo_root" && pwd -P)"
database="$demo_root/events.sqlite"
left="$session:0.0"
right="$session:0.1"
query='SELECT "$dogpaddle.id" id, number, square FROM even_squares ORDER BY id'

run_query() {
    tmux send-keys -t "$right" -l "$query"
    tmux send-keys -t "$right" -H 3b
    tmux send-keys -t "$right" Enter
}

cleanup() {
    tmux kill-session -t "$session" 2>/dev/null || true
    rm -rf -- "$demo_root"
}
trap cleanup EXIT

tmux new-session -d -s "$session" -x 116 -y 30 -c "$repository" \
    "env BASH_SILENCE_DEPRECATION_WARNING=1 PS1='$ ' /bin/bash --noprofile --norc"
tmux set-option -t "$session" status off
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux set-option -t "$session" pane-border-format \
    '#[fg=colour39,bold] #{pane_title} #[default]'
tmux split-window -h -p 43 -t "$left" -c "$repository" \
    "env BASH_SILENCE_DEPRECATION_WARNING=1 PS1='$ ' /bin/bash --noprofile --norc"
tmux select-pane -t "$left" -T 'FLOW / cargo run'
tmux select-pane -t "$right" -T 'SQLITE / read-only live query'
tmux select-pane -t "$left"

(
    sleep 0.5
    tmux send-keys -t "$right" -l \
        "test ! -f '$database' && echo 'waiting: sink has not created SQLite yet'"
    tmux send-keys -t "$right" Enter
    sleep 0.4
    tmux send-keys -t "$left" -l \
        "cargo run -q -p dogpaddle-flow --example sqlite_sink_live -- '$demo_root' 18 450"
    tmux send-keys -t "$left" Enter
    while [ ! -f "$database" ]; do
        sleep 0.05
    done
    tmux send-keys -t "$right" -l \
        "echo 'SQLite file created; waiting for target table...'"
    tmux send-keys -t "$right" Enter
    while [[ "$(sqlite3 -readonly "$database" \
        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='even_squares' LIMIT 1;" \
        2>/dev/null)" != "1" ]]; do
        sleep 0.05
    done
    tmux send-keys -t "$right" -l "sqlite3 -readonly -box '$database'"
    tmux send-keys -t "$right" Enter
    sleep 0.15
    run_query
    sleep 0.75
    for _ in {1..10}; do
        tmux send-keys -t "$right" C-l
        run_query
        sleep 0.9
    done
    sleep 2.0
    tmux kill-session -t "$session" 2>/dev/null || true
) &

tmux attach-session -t "$session"
