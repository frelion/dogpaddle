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
    for command in asciinema agg tmux cargo python3; do
        if ! command -v "$command" >/dev/null; then
            echo "missing command: $command" >&2
            echo "requires asciinema, agg, tmux, PostgreSQL, Python 3 and cargo" >&2
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
        "$postgres_bin/pg_ctl" -D "$data" -m fast -t 10 -w stop >/dev/null 2>&1
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

    recording_root="$(mktemp -d /tmp/dogpaddle-postgres-sync.XXXXXX)"
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
    cargo build -q --locked -p dogpaddle-flow --example postgres_sync_live
    "$postgres_bin/initdb" -D "$data" -U dogpaddle_demo --auth=trust \
        --no-instructions --locale=C -E UTF8 >/dev/null
    pg_options="-h 127.0.0.1 -p $port -k $recording_root -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4 -c fsync=on -c synchronous_commit=on"
    "$postgres_bin/pg_ctl" -D "$data" -l "$recording_root/postgres.log" -t 10 \
        -o "$pg_options" -w start >/dev/null
    started=true
    export PGCONNECT_TIMEOUT=3
    export PGOPTIONS="-c statement_timeout=5000"
    psql_setup=("$postgres_bin/psql" -X -w -q -h 127.0.0.1 -p "$port" \
        -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1)
    "${psql_setup[@]}" -c \
        "CREATE SCHEMA source; CREATE SCHEMA target; CREATE TABLE source.orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL); ALTER TABLE source.orders REPLICA IDENTITY FULL; CREATE PUBLICATION orders_publication FOR TABLE source.orders;" \
        >/dev/null
    "${psql_setup[@]}" -c \
        "SELECT pg_create_logical_replication_slot('orders_slot', 'pgoutput');" \
        >/dev/null

    export DOGPADDLE_RECORD_ROOT="$recording_root"
    export DOGPADDLE_RECORD_BUNDLE="$bundle"
    export DOGPADDLE_RECORD_POSTGRES_BIN="$postgres_bin"
    export DOGPADDLE_RECORD_PORT="$port"
    record_command="/bin/zsh ${(q)script_path} --session"
    TERM=xterm-256color asciinema record -q --headless --overwrite --return \
        --window-size 144x30 --command "$record_command" "$raw_cast" &
    recorder_pid=$!
    if ! wait "$recorder_pid"; then
        recorder_pid=""
        echo "recording session failed" >&2
        exit 1
    fi
    recorder_pid=""

    # Stop at the final target prompt, before tmux clears its alternate screen.
    python3 - "$raw_cast" "$clean_cast" <<'PY'
import json
import sys

source, destination = sys.argv[1:]
after_success = False
finished = False
tail = ""
success_marker = "SAME PG SYNC RESUMED"
prompt_marker = "dst> "
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
        if not after_success and success_marker in tail:
            after_success = True
            tail = tail.split(success_marker, 1)[1]
        if after_success and prompt_marker in tail:
            finished = True
            break
        marker = prompt_marker if after_success else success_marker
        tail = tail[-max(4096, len(marker)) :]

if not finished:
    raise SystemExit("recording ended before the final PostgreSQL result")
PY
    agg --quiet --font-size 15 --line-height 1.15 --theme github-dark \
        --fps-cap 10 --idle-time-limit 0.8 --last-frame-duration 2 \
        --select 1.5..100% \
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
binary="$repository/target/debug/examples/postgres_sync_live"
flow_path="$recording_root/flow"
host_input="$recording_root/host.in"
host_output="$recording_root/host.out"
status_input="$recording_root/status.in"
session="dogpaddle-postgres-sync-$$"
driver_pid=""

mkfifo "$host_input" "$host_output" "$status_input"
exec 3<>"$host_input"
exec 4<>"$host_output"
exec 5<>"$status_input"

cleanup_session() {
    if [[ -n "${driver_pid:-}" ]] && kill -0 "$driver_pid" 2>/dev/null; then
        kill "$driver_pid" 2>/dev/null || true
        wait "$driver_pid" 2>/dev/null || true
    fi
    tmux kill-session -t "$session" 2>/dev/null || true
}
abort_session() {
    exit 1
}
trap cleanup_session EXIT
trap abort_session HUP INT TERM

origin_psql=("$postgres_bin/psql" -X -w -q -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1 -P border=2)
target_psql=("$postgres_bin/psql" -X -w -q -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1 -P border=2)
source_check=("$postgres_bin/psql" -X -w -Atq -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1)
target_check=("$postgres_bin/psql" -X -w -Atq -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1)

left="$(tmux new-session -d -s "$session" -x 144 -y 30 -P -F '#{pane_id}' \
    -c "$repository" "${(q)origin_psql[@]}")"
middle="$(tmux split-window -h -p 66 -t "$left" -P -F '#{pane_id}' -c "$repository" \
    "/bin/bash -c 'while IFS= read -r line; do printf \"%s\\n\" \"\$line\"; done < ${(q)status_input}'")"
right="$(tmux split-window -h -p 50 -t "$middle" -P -F '#{pane_id}' -c "$repository" \
    "${(q)target_psql[@]}")"
tmux select-layout -t "$session" even-horizontal >/dev/null
tmux set-option -t "$session" status off
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux set-option -t "$session" pane-border-format \
    '#[fg=colour39,bold] #{pane_title} #[default]'
tmux select-pane -t "$left" -T 'POSTGRESQL / source.orders'
tmux select-pane -t "$middle" -T 'DOGPADDLE / durable Flow'
tmux select-pane -t "$right" -T 'SAME PG / target.orders'
tmux select-pane -t "$left"

(
    host_pid=""
    host_response=""

    cleanup_driver() {
        if [[ -n "${host_pid:-}" ]] && kill -0 "$host_pid" 2>/dev/null; then
            kill "$host_pid" 2>/dev/null || true
            wait "$host_pid" 2>/dev/null || true
        fi
        tmux kill-session -t "$session" 2>/dev/null || true
    }
    trap cleanup_driver EXIT
    trap 'exit 1' HUP INT TERM

    status() {
        print -r -- "$1" >&5
    }
    success() {
        status $'\033[38;5;78m✓\033[0m '"$1"
    }
    read_host_response() {
        local context="$1"
        local deadline=$(( SECONDS + 60 ))
        while (( SECONDS < deadline )); do
            if IFS= read -r -t 1 host_response <&4; then
                return 0
            fi
            if [[ -n "${host_pid:-}" ]] && ! kill -0 "$host_pid" 2>/dev/null; then
                echo "Flow host exited while waiting for: $context" >&2
                return 1
            fi
        done
        echo "timed out waiting for Flow host: $context" >&2
        return 1
    }
    start_host() {
        local mode="$1"
        local number="$2"
        DOGPADDLE_SOURCE_PASSWORD=recording-secret-not-persisted \
        DOGPADDLE_TARGET_PASSWORD=recording-secret-not-persisted \
            "$binary" "$mode" "$flow_path" "$bundle" "$port" \
            <"$host_input" >"$host_output" \
            2>"$recording_root/flow-$number.log" 3>&- 4>&- 5>&- &
        host_pid=$!
        read_host_response "$mode startup" || exit 1
        if [[ "$host_response" != "{\"kind\":\"ready\",\"mode\":\"$mode\"}" ]]; then
            echo "unexpected Flow host startup: $host_response" >&2
            exit 1
        fi
    }
    host_request() {
        print -r -- "$1" >&3
        read_host_response "response to $1" || exit 1
        if [[ "$host_response" != '{"kind":"advance","outcome":'* ]]; then
            echo "unexpected Flow host response: $host_response" >&2
            exit 1
        fi
    }
    slot_is_active() {
        [[ "$("${source_check[@]}" -c "SELECT active FROM pg_replication_slots WHERE slot_name='orders_slot'")" == t ]]
    }
    wait_for_slot() {
        local deadline=$(( SECONDS + 60 ))
        while ! slot_is_active; do
            host_request advance
            sleep 0.03
            if (( SECONDS >= deadline )); then
                echo "timed out waiting for PostgreSQL replication slot" >&2
                exit 1
            fi
        done
    }
    wait_for_slot_inactive() {
        local deadline=$(( SECONDS + 15 ))
        while slot_is_active; do
            sleep 0.05
            if (( SECONDS >= deadline )); then
                echo "timed out waiting for PostgreSQL replication slot release" >&2
                exit 1
            fi
        done
    }
    source_state() {
        "${source_check[@]}" -c \
            "SELECT COALESCE(string_agg(id::text || ':' || status, ',' ORDER BY id), '') FROM source.orders;"
    }
    target_state() {
        if [[ "$("${target_check[@]}" -c "SELECT to_regclass('target.orders') IS NOT NULL")" != t ]]; then
            return 0
        fi
        "${target_check[@]}" -c \
            "SELECT COALESCE(string_agg(id::text || ':' || convert_from(status, 'UTF8'), ',' ORDER BY id, \"\$dogpaddle.id\"), '') FROM target.orders;"
    }
    receipt_count() {
        "${target_check[@]}" -c \
            'SELECT count(*) FROM target."$dogpaddle.receipt.readme_sink";'
    }
    wait_for_source() {
        local expected="$1"
        local deadline=$(( SECONDS + 15 ))
        while [[ "$(source_state)" != "$expected" ]]; do
            sleep 0.03
            if (( SECONDS >= deadline )); then
                echo "timed out waiting for source state: $expected" >&2
                exit 1
            fi
        done
    }
    drive_until() {
        local expected="$1"
        local deadline=$(( SECONDS + 60 ))
        while [[ "$(target_state)" != "$expected" ]]; do
            host_request advance
            sleep 0.03
            if (( SECONDS >= deadline )); then
                echo "timed out waiting for target state: $expected" >&2
                exit 1
            fi
        done
    }
    run_source() {
        tmux send-keys -t "$left" C-l
        tmux send-keys -t "$left" -l "$1"
        # A literal semicolon is a tmux command separator, so send its byte value.
        tmux send-keys -t "$left" -H 3b
        tmux send-keys -t "$left" Enter
    }
    run_initial_insert() {
        tmux send-keys -t "$left" C-l
        tmux send-keys -t "$left" -l 'INSERT INTO orders VALUES'
        tmux send-keys -t "$left" Enter
        tmux send-keys -t "$left" -l "  (1,'new'), (2,'queued')"
        tmux send-keys -t "$left" -H 3b
        tmux send-keys -t "$left" Enter
    }
    show_target() {
        tmux send-keys -t "$right" C-l
        tmux send-keys -t "$right" -l "SELECT id,convert_from(status,'UTF8')"
        tmux send-keys -t "$right" Enter
        tmux send-keys -t "$right" -l '  AS status FROM orders ORDER BY id'
        tmux send-keys -t "$right" -H 3b
        tmux send-keys -t "$right" Enter
    }

    sleep 0.8
    tmux send-keys -t "$left" -l "\\set PROMPT1 'src> '"
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$left" -l "\\set PROMPT2 '...> '"
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$right" -l "\\set PROMPT1 'dst> '"
    tmux send-keys -t "$right" Enter
    tmux send-keys -t "$right" -l "\\set PROMPT2 '...> '"
    tmux send-keys -t "$right" Enter
    tmux send-keys -t "$left" -l 'SET search_path=source'
    tmux send-keys -t "$left" -H 3b
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$right" -l 'SET search_path=target'
    tmux send-keys -t "$right" -H 3b
    tmux send-keys -t "$right" Enter
    sleep 0.2
    tmux send-keys -t "$left" C-l
    tmux send-keys -t "$right" C-l
    status $'\033[38;5;245mone PostgreSQL · WAL-only · no snapshot\033[0m'
    status 'source.orders'
    status '    │ WAL / pgoutput'
    status '    ▼ Arrow Change'
    status 'PostgresSource → PostgresSink'
    status '    │ receipt + mutations'
    status '    ▼'
    status 'target.orders'
    status ''
    status 'opening durable Flow...'
    start_host build 1
    wait_for_slot
    success 'WAL stream connected'
    sleep 0.7

    run_initial_insert
    wait_for_source '1:new,2:queued'
    drive_until '1:new,2:queued'
    success 'INSERT ×2 synced'
    show_target
    sleep 1.1

    run_source "UPDATE orders SET status='paid' WHERE id=1"
    wait_for_source '1:paid,2:queued'
    drive_until '1:paid,2:queued'
    success 'UPDATE synced'
    show_target
    sleep 1.1

    run_source 'DELETE FROM orders WHERE id=2'
    wait_for_source '1:paid'
    drive_until '1:paid'
    success 'DELETE synced'
    show_target
    sleep 1.0

    receipts_before="$(receipt_count)"
    if [[ -z "$receipts_before" ]] || (( receipts_before < 1 )); then
        echo "target receipt was not committed before restart" >&2
        exit 1
    fi
    status ''
    status $'\033[38;5;214m● target committed · local completion pending\033[0m'
    kill -KILL "$host_pid"
    wait "$host_pid" 2>/dev/null || true
    host_pid=""
    wait_for_slot_inactive
    status $'\033[38;5;214m● Flow host stopped (SIGKILL)\033[0m'
    sleep 0.8

    start_host open 2
    status $'\033[38;5;45m↻ reopened from durable Flow state\033[0m'
    host_request advance
    host_request advance
    wait_for_slot
    if [[ "$(target_state)" != '1:paid' || "$(receipt_count)" != "$receipts_before" ]]; then
        echo "target changed while replaying the prepared delivery" >&2
        exit 1
    fi
    success 'committed batch recovered without duplicates'
    sleep 0.9

    run_source "INSERT INTO orders VALUES(3,'live')"
    wait_for_source '1:paid,3:live'
    drive_until '1:paid,3:live'
    host_request advance
    if [[ "$(target_state)" != '1:paid,3:live' ]] \
        || (( $(receipt_count) <= receipts_before )); then
        echo "post-restart witness did not converge" >&2
        exit 1
    fi
    success 'new WAL change synced after reopen'
    show_target
    sleep 1.0
    status ''
    status $'\033[1;38;5;78mSAME PG SYNC RESUMED\033[0m'
    tmux send-keys -t "$right" Enter
    sleep 1.5
) &
driver_pid=$!

if ! tmux attach-session -t "$session"; then
    echo "tmux recording session failed" >&2
    exit 1
fi
if ! wait "$driver_pid"; then
    driver_pid=""
    echo "recording driver failed" >&2
    for log in "$recording_root"/flow-*.log(N); do
        if [[ -f "$log" ]]; then
            echo "--- ${log:t} ---" >&2
            tail -n 80 "$log" >&2
        fi
    done
    exit 1
fi
driver_pid=""
