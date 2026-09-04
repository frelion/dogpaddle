#!/bin/zsh

set -eu

script_path="${0:A}"
script_directory="${script_path:h}"
repository="$(cd "$script_directory/.." && pwd -P)"

usage() {
    echo "usage: ${script_path:t} --bundle ABSOLUTE_RUNTIME_BUNDLE --postgres-bin ABSOLUTE_POSTGRES_BIN" >&2
}

if [[ "${1:-}" != "--session" ]]; then
    bundle=""
    postgres_bin=""
    while (( $# > 0 )); do
        case "$1" in
            --bundle)
                if (( $# < 2 )); then
                    usage
                    exit 2
                fi
                bundle="${2:-}"
                shift 2
                ;;
            --postgres-bin)
                if (( $# < 2 )); then
                    usage
                    exit 2
                fi
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
    bundle="${bundle:A}"
    postgres_bin="${postgres_bin:A}"
    if [[ ! -d "$bundle" || ! -x "$postgres_bin/initdb" || ! -x "$postgres_bin/pg_ctl" \
        || ! -x "$postgres_bin/postgres" || ! -x "$postgres_bin/psql" ]]; then
        echo "runtime bundle or PostgreSQL binaries are missing" >&2
        exit 1
    fi
    for command in asciinema agg tmux sqlite3 cargo python3; do
        if ! command -v "$command" >/dev/null; then
            echo "missing command: $command" >&2
            echo "requires asciinema, agg, tmux, sqlite3, PostgreSQL, Python 3 and cargo" >&2
            exit 1
        fi
    done

    recording_root=""
    asset_staging=""
    data=""
    output="$repository/docs/assets/postgres-cdc-live.gif"
    started=false
    recorder_pid=""

    stop_postgres() {
        "$postgres_bin/pg_ctl" -D "$data" -m immediate -t 10 -w stop >/dev/null 2>&1
    }
    cleanup_recording() {
        local preserve_root=false
        if [[ -n "${recorder_pid:-}" ]] && kill -0 "$recorder_pid" 2>/dev/null; then
            kill "$recorder_pid" 2>/dev/null || true
            wait "$recorder_pid" 2>/dev/null || true
        fi
        if [[ -n "${data:-}" && ( "$started" == true || -f "$data/postmaster.pid" ) ]]; then
            if ! stop_postgres; then
                preserve_root=true
                echo "could not stop temporary PostgreSQL; preserving $recording_root" >&2
            fi
        fi
        if [[ -n "${asset_staging:-}" ]]; then
            rm -rf -- "$asset_staging"
        fi
        if [[ -n "${recording_root:-}" && "$preserve_root" == false ]]; then
            rm -rf -- "$recording_root"
        fi
    }
    abort_recording() {
        exit 1
    }
    trap cleanup_recording EXIT
    trap abort_recording HUP INT TERM

    recording_root="$(mktemp -d /tmp/dogpaddle-postgres-live.XXXXXX)"
    asset_staging="$(mktemp -d "$repository/docs/assets/.postgres-cdc-live.XXXXXX")"
    raw_cast="$recording_root/raw.cast"
    clean_cast="$recording_root/clean.cast"
    data="$recording_root/data"
    staged_output="$asset_staging/postgres-cdc-live.gif"
    port="$(python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
    )"

    cd "$repository"
    cargo build -q --locked -p dogpaddle-flow --example postgres_cdc
    "$postgres_bin/initdb" -D "$data" -U dogpaddle_gate --auth=trust \
        --no-instructions --locale=C -E UTF8 >/dev/null
    pg_options="-h 127.0.0.1 -p $port -k $recording_root -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4"
    "$postgres_bin/pg_ctl" -D "$data" -l "$recording_root/postgres.log" -t 10 \
        -o "$pg_options" -w start >/dev/null
    started=true
    export PGCONNECT_TIMEOUT=3
    export PGOPTIONS="-c statement_timeout=3000"
    "$postgres_bin/psql" -X -w -h 127.0.0.1 -p "$port" -U dogpaddle_gate \
        -d postgres -v ON_ERROR_STOP=1 -q -c \
        "CREATE TABLE public.orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL); ALTER TABLE public.orders REPLICA IDENTITY FULL; CREATE PUBLICATION orders_pub FOR TABLE public.orders;" \
        >/dev/null
    "$postgres_bin/psql" -X -w -h 127.0.0.1 -p "$port" -U dogpaddle_gate \
        -d postgres -v ON_ERROR_STOP=1 -q -c \
        "SELECT pg_create_logical_replication_slot('orders_slot', 'pgoutput');" \
        >/dev/null

    export DOGPADDLE_RECORD_ROOT="$recording_root"
    export DOGPADDLE_RECORD_BUNDLE="$bundle"
    export DOGPADDLE_RECORD_POSTGRES_BIN="$postgres_bin"
    export DOGPADDLE_RECORD_PORT="$port"
    record_command="/bin/zsh ${(q)script_path} --session"
    TERM=xterm-256color asciinema record -q --headless --overwrite --return \
        --window-size 132x30 --command "$record_command" "$raw_cast" &
    recorder_pid=$!
    if ! wait "$recorder_pid"; then
        recorder_pid=""
        echo "recording session failed" >&2
        exit 1
    fi
    recorder_pid=""

    # End at the final SQLite prompt, before tmux clears its alternate screen.
    python3 - "$raw_cast" "$clean_cast" <<'PY'
import json
import sys

source, destination = sys.argv[1:]
after_commit = False
finished = False
tail = ""
commit_marker = "ACK sent after commit"
prompt_marker = "sqlite> "
with open(source, encoding="utf-8") as input_file, open(
    destination, "w", encoding="utf-8"
) as output_file:
    output_file.write(input_file.readline())
    for line in input_file:
        event = json.loads(line)
        output_file.write(line)
        if len(event) < 3 or event[1] != "o":
            continue
        tail += event[2]
        if not after_commit and commit_marker in tail:
            after_commit = True
            tail = tail.split(commit_marker, 1)[1]
        if after_commit and prompt_marker in tail:
            finished = True
            break
        marker = prompt_marker if after_commit else commit_marker
        tail = tail[-len(marker) :]

if not finished:
    raise SystemExit("recording ended before the final SQLite result")
PY
    agg --quiet --font-size 16 --line-height 1.1 --theme github-dark \
        --fps-cap 10 --idle-time-limit 0.8 --last-frame-duration 2 \
        --select 0.9..100% \
        "$clean_cast" "$staged_output"
    if ! stop_postgres; then
        echo "could not stop temporary PostgreSQL; recording was not replaced" >&2
        exit 1
    fi
    started=false
    mv -f -- "$staged_output" "$output"
    echo "recorded $output"
    exit 0
fi

recording_root="${DOGPADDLE_RECORD_ROOT:?missing recording root}"
bundle="${DOGPADDLE_RECORD_BUNDLE:?missing runtime bundle}"
postgres_bin="${DOGPADDLE_RECORD_POSTGRES_BIN:?missing PostgreSQL binaries}"
port="${DOGPADDLE_RECORD_PORT:?missing PostgreSQL port}"
binary="$repository/target/debug/examples/postgres_cdc"
flow_root="$recording_root/flow-demo"
database="$flow_root/sink.sqlite"
host_input="$recording_root/host.in"
host_output="$recording_root/host.out"
status_input="$recording_root/status.in"
session="dogpaddle-postgres-readme-$$"
driver_pid=""

mkfifo "$host_input" "$host_output" "$status_input"
exec 3<>"$host_input"
exec 4<>"$host_output"
exec 5<>"$status_input"
mkdir -p "$flow_root"

DOGPADDLE_GATE_PASSWORD=recording-secret-not-persisted \
    "$binary" flow "$flow_root" "$bundle" "$port" orders orders_slot orders_pub \
    <"$host_input" >"$host_output" 2>"$recording_root/flow.log" &
host_pid=$!

cleanup_session() {
    if [[ -n "${driver_pid:-}" ]] && kill -0 "$driver_pid" 2>/dev/null; then
        kill "$driver_pid" 2>/dev/null || true
        wait "$driver_pid" 2>/dev/null || true
    fi
    tmux kill-session -t "$session" 2>/dev/null || true
    kill "$host_pid" 2>/dev/null || true
    wait "$host_pid" 2>/dev/null || true
}
abort_session() {
    exit 1
}
trap cleanup_session EXIT
trap abort_session HUP INT TERM

host_response=""
read_host_response() {
    local context="$1"
    if ! IFS= read -r -t 30 host_response <&4; then
        if kill -0 "$host_pid" 2>/dev/null; then
            echo "timed out waiting for Flow host: $context" >&2
        else
            echo "Flow host exited while waiting for: $context" >&2
        fi
        return 1
    fi
}

read_host_response startup || exit 1
ready="$host_response"
if [[ "$ready" != '{"kind":"ready"}' ]]; then
    echo "unexpected Flow host startup: $ready" >&2
    exit 1
fi

psql_command=("$postgres_bin/psql" -X -w -h 127.0.0.1 -p "$port" -U dogpaddle_gate -d postgres)
psql_check=("$postgres_bin/psql" -X -w -h 127.0.0.1 -p "$port" -U dogpaddle_gate -d postgres -Atq)

host_request() {
    print -r -- "$1" >&3
    read_host_response "response to $1" || exit 1
    if [[ "$host_response" == *'"kind":"error"'* ]]; then
        echo "$host_response" >&2
        exit 1
    fi
}

slot_is_active() {
    [[ "$("${psql_check[@]}" -c "SELECT active FROM pg_replication_slots WHERE slot_name='orders_slot'")" == t ]]
}

wait_for_slot() {
    local started_at=$SECONDS
    while ! slot_is_active; do
        host_request advance
        sleep 0.02
        if (( SECONDS - started_at >= 30 )); then
            echo "timed out waiting for PostgreSQL replication slot" >&2
            exit 1
        fi
    done
}

sqlite_state() {
    if [[ ! -f "$database" ]]; then
        return 1
    fi
    sqlite3 -readonly "$database" \
        "SELECT group_concat(id || ':' || status, ',') FROM (SELECT id,status FROM events ORDER BY id);" \
        2>/dev/null
}

drive_until() {
    local expected="$1"
    local started_at=$SECONDS
    while [[ "$(sqlite_state || true)" != "$expected" ]]; do
        host_request advance
        sleep 0.02
        if (( SECONDS - started_at >= 60 )); then
            echo "timed out waiting for SQLite state: $expected" >&2
            exit 1
        fi
    done
}

status() {
    print -r -- "$1" >&5
}

left="$(tmux new-session -d -s "$session" -x 132 -y 30 -P -F '#{pane_id}' \
    -c "$repository" "${(q)psql_command[@]}")"
middle="$(tmux split-window -h -p 66 -t "$left" -P -F '#{pane_id}' -c "$repository" \
    "/bin/bash -c 'while IFS= read -r line; do printf \"%s\\n\" \"\$line\"; done < ${(q)status_input}'")"
right="$(tmux split-window -h -p 50 -t "$middle" -P -F '#{pane_id}' -c "$repository" \
    "/bin/bash -c 'printf \"waiting for SqliteSink...\\n\"; while [ ! -f ${(q)database} ]; do sleep 0.05; done; exec sqlite3 -readonly -header -box ${(q)database}'")"
tmux set-option -t "$session" status off
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux set-option -t "$session" pane-border-format \
    '#[fg=colour39,bold] #{pane_title} #[default]'
tmux select-pane -t "$left" -T 'POSTGRESQL / writes'
tmux select-pane -t "$middle" -T 'DOGPADDLE / embedded Flow'
tmux select-pane -t "$right" -T 'SQLITE / read-only sink'
tmux select-pane -t "$left"

run_query() {
    tmux send-keys -t "$right" -l 'SELECT id,status FROM events'
    tmux send-keys -t "$right" -H 3b
    tmux send-keys -t "$right" Enter
}

run_postgres() {
    tmux send-keys -t "$left" -l "$1"
    # A literal semicolon is a tmux command separator, so send its byte value.
    tmux send-keys -t "$left" -H 3b
    tmux send-keys -t "$left" Enter
}

(
    trap 'tmux kill-session -t "$session" 2>/dev/null || true' EXIT
    trap 'exit 1' HUP INT TERM
    sleep 0.5
    status 'PostgresSource -> Arrow Change'
    status '                 -> SqliteSink'
    status ''
    status 'opening durable Flow state...'
    host_request advance
    wait_for_slot
    status 'connected: orders_slot'
    sleep 0.7

    run_postgres "INSERT INTO orders VALUES(1,'new')"
    drive_until '1:new'
    status 'advance -> Progressed  [INSERT]'
    sleep 0.5
    run_query
    sleep 1.0

    run_postgres "UPDATE orders SET status='sent' WHERE id=1"
    drive_until '1:sent'
    status 'advance -> Progressed  [UPDATE]'
    sleep 0.5
    run_query
    sleep 1.0

    run_postgres "INSERT INTO orders VALUES(2,'queued')"
    drive_until '1:sent,2:queued'
    status 'advance -> Progressed  [INSERT]'
    sleep 0.5
    run_query
    sleep 1.0

    run_postgres 'DELETE FROM orders WHERE id=1'
    drive_until '2:queued'
    status 'advance -> Progressed  [DELETE]'
    status ''
    status 'checkpoint + output committed'
    status 'ACK sent after commit'
    sleep 0.5
    run_query
    sleep 2.0
) &
driver_pid=$!

if ! tmux attach-session -t "$session"; then
    echo "tmux recording session failed" >&2
    exit 1
fi
if ! wait "$driver_pid"; then
    driver_pid=""
    echo "recording driver failed" >&2
    exit 1
fi
driver_pid=""
