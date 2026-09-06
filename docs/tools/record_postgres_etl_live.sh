#!/bin/zsh

set -eu

script_path="${0:A}"
script_directory="${script_path:h}"
repository="$(cd "$script_directory/../.." && pwd -P)"

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
    output="$repository/docs/assets/postgres-etl-live.gif"
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

    recording_root="$(mktemp -d /tmp/dogpaddle-postgres-etl.XXXXXX)"
    asset_staging="$(mktemp -d "$repository/docs/assets/.postgres-etl-live.XXXXXX")"
    raw_cast="$recording_root/raw.cast"
    clean_cast="$recording_root/clean.cast"
    data="$recording_root/data"
    staged_output="$asset_staging/postgres-etl-live.gif"
    port="$(python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
    )"

    cd "$repository"
    cargo build -q --locked -p dogpaddle-sql --example postgres_etl_live
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
        "CREATE SCHEMA sales; CREATE SCHEMA analytics; CREATE TABLE sales.orders (order_id BIGINT PRIMARY KEY, customer TEXT NOT NULL, region TEXT NOT NULL, status TEXT NOT NULL, quantity INTEGER NOT NULL, unit_price_cents BIGINT NOT NULL, discount_pct INTEGER NOT NULL); ALTER TABLE sales.orders REPLICA IDENTITY FULL; CREATE PUBLICATION orders_publication FOR TABLE sales.orders;" \
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
        --window-size 120x32 --command "$record_command" "$raw_cast" &
    recorder_pid=$!
    if ! wait "$recorder_pid"; then
        recorder_pid=""
        echo "recording session failed" >&2
        exit 1
    fi
    recorder_pid=""

    # Stop on the completed scene, before tmux clears its alternate screen.
    python3 - "$raw_cast" "$clean_cast" <<'PY'
import json
import sys

source, destination = sys.argv[1:]
finished = False
tail = ""
success_marker = "SQL ETL RESUMED"
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
        if success_marker in tail:
            finished = True
            break
        tail = tail[-max(4096, len(success_marker)) :]

if not finished:
    raise SystemExit("recording ended before the final PostgreSQL result")
PY
    agg --quiet --font-size 18 --line-height 1.15 --theme github-dark \
        --fps-cap 10 --idle-time-limit 1 --last-frame-duration 2 \
        --select 2..100% \
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
binary="$repository/target/debug/examples/postgres_etl_live"
sql_path="$repository/crates/sql/examples/postgres_etl.sql"
flow_path="$recording_root/flow"
host_input="$recording_root/host.in"
host_output="$recording_root/host.out"
status_input="$recording_root/status.in"
target_input="$recording_root/target.in"
session="dogpaddle-postgres-etl-$$"
driver_pid=""

mkfifo "$host_input" "$host_output" "$status_input" "$target_input"
exec 3<>"$host_input"
exec 4<>"$host_output"
exec 5<>"$status_input"
exec 6<>"$target_input"

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
target_query='SELECT "$dogpaddle.id" AS rid, order_id AS id,'
target_query+=" convert_from(market,'UTF8') AS market, net_cents AS net,"
target_query+=" convert_from(lane,'UTF8') AS lane,"
target_query+=" CASE WHEN attention IS NULL THEN '-' ELSE convert_from(attention,'UTF8') END AS attention"
target_query+=' FROM analytics.order_insights ORDER BY id'
target_display=("$postgres_bin/psql" -X -w -q -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1 -P border=2 -P footer=off)
source_check=("$postgres_bin/psql" -X -w -Atq -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1)
target_check=("$postgres_bin/psql" -X -w -Atq -h 127.0.0.1 -p "$port" \
    -U dogpaddle_demo -d postgres -v ON_ERROR_STOP=1)

left="$(tmux new-session -d -s "$session" -x 120 -y 32 -P -F '#{pane_id}' \
    -c "$repository" "${(q)origin_psql[@]}")"
right="$(tmux split-window -h -p 50 -t "$left" -P -F '#{pane_id}' -c "$repository" \
    "/bin/bash -c 'while IFS= read -r line; do printf \"%s\\n\" \"\$line\"; done < ${(q)target_input}'")"
bottom="$(tmux split-window -f -v -p 31 -t "$left" -P -F '#{pane_id}' -c "$repository" \
    "/bin/bash -c 'while IFS= read -r line; do printf \"%s\\n\" \"\$line\"; done < ${(q)status_input}'")"
tmux set-option -t "$session" status off
tmux set-option -t "$session" remain-on-exit on
tmux set-option -t "$session" pane-border-status top
tmux set-option -t "$session" pane-border-format \
    '#[fg=colour39,bold] #{pane_title} #[default]'
tmux select-pane -t "$left" -T 'SAME PG / SOURCE · sales.orders'
tmux select-pane -t "$right" -T 'SAME PG / RESULT · analytics.order_insights'
tmux select-pane -t "$bottom" -T 'DOGPADDLE / postgres_etl.sql'
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
    pipeline() {
        status $'\033[2J\033[H\033[1;38;5;45mONE SQL · SAME POSTGRESQL DATABASE\033[0m'
        status 'sales.orders  ── WAL ──▶  postgres_etl.sql  ──▶  analytics.order_insights'
        status 'postgres_cdc · 4 CTEs · arithmetic · CASE/WHERE · UNION ALL'
    }
    stage() {
        pipeline
        status ''
        status $'\033[1;38;5;255m'"$1"$'\033[0m'
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
        DOGPADDLE_POSTGRES_ETL_BUNDLE="$bundle" \
        DOGPADDLE_POSTGRES_ETL_PORT="$port" \
        DOGPADDLE_POSTGRES_ETL_USER=dogpaddle_demo \
        DOGPADDLE_POSTGRES_ETL_PASSWORD=recording-secret-not-persisted \
            "$binary" "$mode" "$sql_path" "$flow_path" \
            <"$host_input" >"$host_output" \
            2>"$recording_root/flow-$number.log" 3>&- 4>&- 5>&- 6>&- &
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
    sink_has_backlog() {
        python3 - "$host_response" <<'PY'
import json
import sys

sink = json.loads(sys.argv[1])["sink"]
raise SystemExit(0 if sink["cursor"] < sink["tail"] else 1)
PY
    }
    sink_is_caught_up() {
        python3 - "$host_response" <<'PY'
import json
import sys

sink = json.loads(sys.argv[1])["sink"]
raise SystemExit(0 if sink["cursor"] == sink["tail"] else 1)
PY
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
            "SELECT COALESCE(string_agg(order_id::text || ':' || customer || ':' || region || ':' || status || ':' || quantity::text || ':' || unit_price_cents::text || ':' || discount_pct::text, ',' ORDER BY order_id), '') FROM sales.orders;"
    }
    target_state() {
        if [[ "$("${target_check[@]}" -c "SELECT to_regclass('analytics.order_insights') IS NOT NULL")" != t ]]; then
            return 0
        fi
        "${target_check[@]}" -c \
            "SELECT COALESCE(string_agg(order_id::text || ':' || convert_from(customer, 'UTF8') || ':' || convert_from(market, 'UTF8') || ':' || gross_cents::text || ':' || net_cents::text || ':' || convert_from(lane, 'UTF8') || ':' || CASE WHEN attention IS NULL THEN '<null>' ELSE convert_from(attention, 'UTF8') END, ',' ORDER BY order_id, \"\$dogpaddle.id\"), '') FROM analytics.order_insights;"
    }
    target_ids() {
        "${target_check[@]}" -c \
            "SELECT COALESCE(string_agg(\"\$dogpaddle.id\"::text, ',' ORDER BY \"\$dogpaddle.id\"), '') FROM analytics.order_insights;"
    }
    target_id() {
        "${target_check[@]}" -c \
            "SELECT \"\$dogpaddle.id\" FROM analytics.order_insights WHERE order_id=$1;"
    }
    attention_is_nullable() {
        local value
        value="$("${target_check[@]}" -c \
            "SELECT is_nullable FROM information_schema.columns WHERE table_schema='analytics' AND table_name='order_insights' AND column_name='attention'")"
        [[ "$value" == YES ]]
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
        local line
        for line in "$@"; do
            tmux send-keys -t "$left" -l "$line"
            tmux send-keys -t "$left" Enter
        done
        tmux send-keys -t "$left" -l 'RETURNING order_id id, status,'
        tmux send-keys -t "$left" Enter
        tmux send-keys -t "$left" -l ' quantity qty, unit_price_cents unit,'
        tmux send-keys -t "$left" Enter
        tmux send-keys -t "$left" -l ' discount_pct discount'
        # A literal semicolon is a tmux command separator, so send its byte value.
        tmux send-keys -t "$left" -H 3b
        tmux send-keys -t "$left" Enter
    }
    show_target() {
        print -r -- $'\033[2J\033[H' >&6
        "${target_display[@]}" -c "$target_query" >&6
    }
    sleep 0.8
    tmux send-keys -t "$left" -l "\\set PROMPT1 'src> '"
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$left" -l "\\set PROMPT2 '...> '"
    tmux send-keys -t "$left" Enter
    tmux send-keys -t "$left" -l 'SET search_path=sales'
    tmux send-keys -t "$left" -H 3b
    tmux send-keys -t "$left" Enter
    sleep 0.2
    tmux send-keys -t "$left" C-l
    print -r -- $'\033[2J\033[Hwaiting for transformed rows…' >&6
    stage 'BUILD · compile postgres_etl.sql and connect to WAL'
    start_host build 1
    wait_for_slot
    success 'SQL compiled into a durable Flow'
    success 'WAL stream connected'
    sleep 0.9

    initial_source='101:Acme:cn-east:new:3:5000:10,102:Orbit:eu-west:paid:2:4000:0,103:Nova:cn-south:paid:4:8000:25'
    initial_target='103:Nova:China:32000:24000:standard:regional'
    stage 'INSERT · filter unpaid/low-value rows, then price and classify'
    run_source \
        'INSERT INTO orders VALUES' \
        " (101,'Acme','cn-east','new',3,5000,10)," \
        " (102,'Orbit','eu-west','paid',2,4000,0)," \
        " (103,'Nova','cn-south','paid',4,8000,25)"
    wait_for_source "$initial_source"
    drive_until "$initial_target"
    if ! attention_is_nullable; then
        echo "UNION ALL did not preserve the nullable attention Schema" >&2
        exit 1
    fi
    success 'INSERT: filtered, priced, classified'
    show_target
    sleep 1.2

    paid_source='101:Acme:cn-east:paid:3:5000:10,102:Orbit:eu-west:paid:2:4000:0,103:Nova:cn-south:paid:4:8000:25'
    paid_target='101:Acme:China:15000:13500:standard:regional,103:Nova:China:32000:24000:standard:regional'
    stage 'UPDATE STATUS · a previously filtered order enters the result'
    run_source \
        'UPDATE orders' \
        "SET status='paid'" \
        'WHERE order_id=101'
    wait_for_source "$paid_source"
    drive_until "$paid_target"
    success 'UPDATE: pending order now qualifies'
    show_target
    sleep 1.2

    repriced_source='101:Acme:cn-east:paid:3:5000:10,102:Orbit:eu-west:paid:2:4000:0,103:Nova:cn-south:paid:6:8000:0'
    repriced_target='101:Acme:China:15000:13500:standard:regional,103:Nova:China:48000:48000:priority:regional'
    stage 'UPDATE VALUES · recompute net amount and priority lane'
    run_source \
        'UPDATE orders' \
        'SET quantity=6, discount_pct=0' \
        'WHERE order_id=103'
    wait_for_source "$repriced_source"
    drive_until "$repriced_target"
    success 'UPDATE: net amount and lane recalculated'
    show_target
    sleep 1.2

    deleted_source='102:Orbit:eu-west:paid:2:4000:0,103:Nova:cn-south:paid:6:8000:0'
    deleted_target='103:Nova:China:48000:48000:priority:regional'
    stage 'DELETE · retract the transformed result row'
    run_source \
        'DELETE FROM orders' \
        'WHERE order_id=101'
    wait_for_source "$deleted_source"
    drive_until "$deleted_target"
    success 'DELETE: transformed result retracted'
    show_target
    sleep 1.1

    ids_before="$(target_ids)"
    id_before="$(target_id 103)"
    if [[ -z "$ids_before" || -z "$id_before" ]] || ! sink_has_backlog; then
        echo "target row was not committed before restart" >&2
        exit 1
    fi
    stage 'CRASH TEST · stop after target commit, before local completion'
    status $'\033[38;5;214m● target committed · local completion pending\033[0m'
    kill -KILL "$host_pid"
    wait "$host_pid" 2>/dev/null || true
    host_pid=""
    wait_for_slot_inactive
    status $'\033[38;5;214m● Flow host stopped (SIGKILL)\033[0m'
    sleep 0.8

    stage 'OPEN SAME STATE · replay the pending commit'
    start_host open 2
    status $'\033[38;5;45m↻ durable Flow reopened\033[0m'
    host_request advance
    if ! sink_has_backlog; then
        echo "reopen did not restore the unsettled sink input" >&2
        exit 1
    fi
    host_request advance
    if ! sink_is_caught_up; then
        echo "reopen did not settle the replayed sink input" >&2
        exit 1
    fi
    wait_for_slot
    if [[ "$(target_state)" != "$deleted_target" || "$(target_ids)" != "$ids_before" \
        || "$(target_id 103)" != "$id_before" ]]; then
        echo "target data or stable IDs changed while replaying the prepared batch" >&2
        exit 1
    fi
    success 'committed result recovered without duplicates'
    sleep 1.1

    resumed_source='102:Orbit:eu-west:paid:2:4000:0,103:Nova:cn-south:paid:6:8000:0,104:Kestrel:us-west:paid:5:10000:20'
    resumed_target='103:Nova:China:48000:48000:priority:regional,104:Kestrel:Global:50000:40000:priority:review'
    stage 'AFTER RECOVERY · process the next INSERT'
    success 'previous result kept the same durable row ID'
    run_source \
        'INSERT INTO orders VALUES' \
        " (104,'Kestrel','us-west','paid',5,10000,20)"
    wait_for_source "$resumed_source"
    drive_until "$resumed_target"
    host_request advance
    if [[ "$(target_state)" != "$resumed_target" ]] \
        || [[ "$(target_ids)" == "$ids_before" ]] \
        || [[ "$(target_id 103)" != "$id_before" ]]; then
        echo "post-restart witness did not converge" >&2
        exit 1
    fi
    success 'new row transformed after reopen'
    show_target
    sleep 1.2
    status $'\033[1;38;5;78mSQL ETL RESUMED\033[0m'
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
