#!/usr/bin/env python3
"""D1 process-level acceptance test against a real PostgreSQL logical slot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


class GateFailure(RuntimeError):
    """A D1 exit gate failed."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateFailure(message)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Pg:
    def __init__(self, port: int) -> None:
        self._base = [
            "psql",
            "-X",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--username",
            "dogpaddle_d1",
            "--dbname",
            "dogpaddle_d1",
        ]
        self._env = dict(os.environ, PGPASSWORD="dogpaddle_d1")

    def execute(self, sql: str) -> None:
        subprocess.run(
            [*self._base, "--quiet", "--command", sql],
            env=self._env,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def scalar(self, sql: str) -> str:
        completed = subprocess.run(
            [*self._base, "--tuples-only", "--no-align", "--command", sql],
            env=self._env,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        require(len(rows) == 1, f"expected one SQL scalar row, got {rows!r}")
        return rows[0]

    def reset(self) -> None:
        active = self.scalar(
            "SELECT count(*) FROM pg_replication_slots "
            "WHERE slot_name = 'dogpaddle_d1_slot' AND active"
        )
        require(active == "0", "cannot reset while the D1 replication slot is active")
        self.execute(
            "SELECT pg_drop_replication_slot('dogpaddle_d1_slot') "
            "WHERE EXISTS (SELECT 1 FROM pg_replication_slots "
            "WHERE slot_name = 'dogpaddle_d1_slot'); "
            "TRUNCATE TABLE public.d1_events;"
        )

    def slot(self) -> "Slot":
        row = self.scalar(
            "SELECT count(*) || '|' || "
            "count(*) FILTER (WHERE active) || '|' || "
            "COALESCE(max(active_pid), 0) || '|' || "
            "COALESCE(max(pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::numeric), 0) "
            "FROM pg_replication_slots WHERE slot_name = 'dogpaddle_d1_slot'"
        )
        count, active, pid, confirmed = row.split("|")
        return Slot(int(count), int(active), int(pid), int(confirmed))


@dataclass(frozen=True)
class Slot:
    count: int
    active_count: int
    active_pid: int
    confirmed_flush: int


@dataclass(frozen=True)
class Response:
    raw: str
    body: dict[str, Any]


class Host:
    def __init__(self, command: list[str], stderr_path: Path) -> None:
        self._stderr = stderr_path.open("w", encoding="utf-8")
        self._process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        require(self._process.stdin is not None, "host stdin is unavailable")
        require(self._process.stdout is not None, "host stdout is unavailable")
        self._responses: queue.Queue[Response | BaseException] = queue.Queue()
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()

    def _read_stdout(self) -> None:
        assert self._process.stdout is not None
        try:
            for line in self._process.stdout:
                raw = line.rstrip("\n")
                if not raw:
                    continue
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError as error:
                    raise GateFailure(f"host stdout is not JSONL: {raw!r}") from error
                if not isinstance(body, dict):
                    raise GateFailure(f"host response is not a JSON object: {body!r}")
                self._responses.put(Response(raw, body))
        except BaseException as error:  # surfaced on the controller thread
            self._responses.put(error)

    def request(
        self,
        command: str,
        accept: Callable[[dict[str, Any]], bool],
        timeout: float = 30.0,
    ) -> Response:
        require(self._process.poll() is None, f"host exited with {self._process.returncode}")
        assert self._process.stdin is not None
        self._process.stdin.write(command + "\n")
        self._process.stdin.flush()
        deadline = time.monotonic() + timeout
        seen: list[dict[str, Any]] = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GateFailure(
                    f"timeout waiting for response to {command!r}; observed {seen!r}"
                )
            try:
                item = self._responses.get(timeout=remaining)
            except queue.Empty as error:
                raise GateFailure(f"timeout waiting for response to {command!r}") from error
            if isinstance(item, BaseException):
                raise item
            seen.append(item.body)
            if item.body.get("kind") == "error" and not accept(item.body):
                raise GateFailure(f"host rejected {command!r}: {item.body!r}")
            if accept(item.body):
                return item

    def error(self, command: str, message_fragment: str) -> Response:
        response = self.request(
            command,
            lambda body: body.get("kind") == "error",
            timeout=30.0,
        )
        message = response.body.get("message")
        require(
            isinstance(message, str) and message_fragment in message,
            f"error for {command!r} did not contain {message_fragment!r}: {message!r}",
        )
        return response

    def start(self) -> Response:
        return self.request(
            "start",
            lambda body: body.get("kind") in {"state", "status"}
            and str(body.get("state", "")).upper() in {"READY", "RUNNING"},
            timeout=60.0,
        )

    def status(self) -> Response:
        return self.request(
            "status",
            lambda body: body.get("kind") == "status",
            timeout=10.0,
        )

    def poll(self) -> Response:
        return self.request(
            "poll",
            lambda body: body.get("kind") in {"delivery", "idle"},
            timeout=30.0,
        )

    def delivery(self, timeout: float = 30.0) -> Response:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.poll()
            if response.body.get("kind") == "delivery":
                return response
        raise GateFailure("timeout waiting for a delivery")

    def ack(self, token: int) -> Response:
        return self.request(
            f"ack {token}",
            lambda body: body.get("kind") == "ack" and body.get("token") == token,
            timeout=30.0,
        )

    def stop(self, timeout_ms: int | None = None) -> Response:
        command = "stop" if timeout_ms is None else f"stop {timeout_ms}"
        return self.request(
            command,
            lambda body: body.get("kind") in {"state", "status"}
            and str(body.get("state", "")).upper() == "STOPPED",
            timeout=60.0,
        )

    def close(self) -> None:
        if self._process.poll() is None:
            try:
                self.request("quit", lambda body: body.get("kind") == "bye", timeout=10.0)
            except (BrokenPipeError, GateFailure):
                self._process.terminate()
        try:
            self._process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=10.0)
        self._stderr.close()


def wait_until(
    description: str,
    observation: Callable[[], Any],
    predicate: Callable[[Any], bool],
    timeout: float = 30.0,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = observation()
        if predicate(last):
            return last
        time.sleep(0.1)
    raise GateFailure(f"timeout waiting for {description}; last observation was {last!r}")


def wait_slot(pg: Pg, active: bool) -> Slot:
    return wait_until(
        f"slot active={active}",
        pg.slot,
        lambda slot: slot.count == 1
        and slot.active_count == (1 if active else 0)
        and ((slot.active_pid > 0) if active else (slot.active_pid == 0)),
    )


def stable_confirmed_flush(pg: Pg, hold_seconds: float) -> Slot:
    """Require the slot's acknowledged LSN to stay unchanged for a full hold."""
    first = pg.slot()
    time.sleep(hold_seconds)
    second = pg.slot()
    require(
        first.active_count == 1 and second.active_count == 1,
        "replication slot became inactive while establishing the LSN baseline",
    )
    require(
        first.confirmed_flush == second.confirmed_flush,
        "confirmed_flush_lsn was not stable before the unacknowledged test",
    )
    return second


def read_offset_file(path: Path) -> bytes:
    return path.read_bytes() if path.is_file() else b""


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require_process_identity(
    response: Response,
    expected_jvm_id: str | None = None,
) -> str:
    body = response.body
    jvm_id = body.get("jvm_id")
    java_process_id = body.get("java_process_id")
    rust_process_id = body.get("rust_process_id")
    require(isinstance(jvm_id, str) and jvm_id, "status has no JVM identity")
    require(
        isinstance(java_process_id, int) and java_process_id > 0,
        "status has no Java process ID",
    )
    require(
        isinstance(rust_process_id, int) and rust_process_id > 0,
        "status has no Rust process ID",
    )
    require(
        java_process_id == rust_process_id,
        "Java and Rust do not report the same OS process",
    )
    if expected_jvm_id is not None:
        require(jvm_id == expected_jvm_id, "a fresh JVM was created during Engine restart")
    return jvm_id


def require_connector_fixture(path: Path) -> dict[str, Any]:
    try:
        configuration = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateFailure(f"cannot read connector fixture {path}: {error}") from error
    require(isinstance(configuration, dict), "connector fixture is not a JSON object")
    require(
        configuration.get("connector.class")
        == "io.debezium.connector.postgresql.PostgresConnector",
        "D1 runner requires the stock PostgreSQL connector",
    )
    require(
        configuration.get("lsn.flush.mode") == "connector",
        "D1 runner requires lsn.flush.mode=connector",
    )
    require(
        configuration.get("offset.storage")
        == "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "D1 runner requires FileOffsetBackingStore",
    )
    require(
        configuration.get("offset.storage.file.filename") == "/state/offsets.dat",
        "D1 runner and host disagree about the offset file path",
    )
    return configuration


def business_events(delivery: dict[str, Any]) -> list[dict[str, Any]]:
    events = delivery.get("events")
    require(isinstance(events, list), "delivery.events is not an array")
    require(delivery.get("event_count") == len(events), "event_count does not match events")
    selected: list[dict[str, Any]] = []
    for event in events:
        require(isinstance(event, dict), "delivery contains a non-object event")
        value = event.get("value")
        if not isinstance(value, dict):
            continue
        payload = value.get("payload")
        if not isinstance(payload, dict):
            continue
        source = payload.get("source")
        if isinstance(source, dict) and source.get("table") == "d1_events":
            selected.append(event)
    return selected


def event_ids(delivery: dict[str, Any]) -> list[int]:
    result: list[int] = []
    for event in business_events(delivery):
        payload = event["value"]["payload"]
        row = payload.get("after") or payload.get("before")
        require(isinstance(row, dict), "business event has neither before nor after row")
        result.append(int(row["id"]))
    return result


def postgres_committable_lsn(delivery: dict[str, Any]) -> int:
    """Extract the connector's PG commit frontier only for this black-box oracle."""
    offset = delivery.get("offset")
    require(isinstance(offset, dict), "delivery offset is not an object")
    lsn = offset.get("lsn_commit", offset.get("lsn"))
    require(
        isinstance(lsn, (int, str)) and str(lsn).isdigit(),
        f"delivery offset has no numeric PostgreSQL commit frontier: {lsn!r}",
    )
    return int(lsn)


def replay_semantics(delivery: dict[str, Any]) -> str:
    """Compare source identity and row meaning, excluding run-local metadata."""
    projected_events: list[dict[str, Any]] = []
    for event in delivery["events"]:
        headers = event.get("headers")
        if isinstance(headers, list):
            headers = [
                header
                for header in headers
                if not (
                    isinstance(header, dict)
                    and header.get("key") == "__debezium.context.runId"
                )
            ]
        projected = {
            "topic": event.get("topic"),
            "kafka_partition": event.get("kafka_partition"),
            # SourceRecord.timestamp is connector-supplied, not part of the
            # source partition/offset identity, and may be regenerated.
            "partition": event.get("partition"),
            "offset": event.get("offset"),
            "key": event.get("key"),
            # runId deliberately identifies one Engine run, so it changes on
            # replay. Every other Connect header remains part of the oracle.
            "headers": headers,
        }
        value = event.get("value")
        if isinstance(value, dict) and isinstance(value.get("payload"), dict):
            payload = dict(value["payload"])
            # Debezium regenerates the envelope processing timestamps when the
            # same WAL event is decoded by a fresh Engine. They are not source
            # identity; source.ts_* and the complete source offset remain.
            payload.pop("ts_ms", None)
            payload.pop("ts_us", None)
            payload.pop("ts_ns", None)
            projected["value"] = {"schema": value.get("schema"), "payload": payload}
        else:
            projected["value"] = value
        projected_events.append(projected)
    return canonical_json(
        {
            "partition": delivery["partition"],
            "offset": delivery["offset"],
            "events": projected_events,
        }
    )


def assert_complete_position(delivery: dict[str, Any]) -> None:
    partition = delivery.get("partition")
    offset = delivery.get("offset")
    require(isinstance(partition, dict) and partition, "delivery has no source partition")
    require(isinstance(offset, dict) and offset, "delivery has no source offset")
    events = delivery["events"]
    require(partition == events[0].get("partition"), "envelope partition is not the first event partition")
    require(offset == events[-1].get("offset"), "envelope offset is not the final event offset")
    for event in events:
        require(isinstance(event.get("partition"), dict) and event["partition"], "event partition is incomplete")
        require(isinstance(event.get("offset"), dict) and event["offset"], "event offset is incomplete")


def report(gate: str, **evidence: Any) -> None:
    print(canonical_json({"result": "PASS", "gate": gate, **evidence}), flush=True)


def run(args: argparse.Namespace) -> None:
    connector_configuration = require_connector_fixture(args.connector_fixture)
    pg = Pg(args.pg_port)
    pg.reset()
    offset_file = args.state_dir / "offsets.dat"
    if offset_file.exists():
        offset_file.unlink()

    host = Host(args.host_command, args.artifacts_dir / "host.stderr.log")
    try:
        unsafe_configuration = dict(connector_configuration)
        unsafe_configuration["lsn.flush.mode"] = "connector_and_driver"
        unsafe_path = args.state_dir / "unsafe-lsn.json"
        unsafe_path.write_text(canonical_json(unsafe_configuration), encoding="utf-8")
        unsafe_error = host.error(
            "create /state/unsafe-lsn.json",
            "requires lsn.flush.mode=connector",
        )
        created_after_error = host.status()
        require(
            str(created_after_error.body.get("state", "")).upper() == "CREATED",
            "a rejected configuration poisoned the original created handle",
        )
        report(
            "unsafe PostgreSQL configuration fails through JNI without poisoning the host",
            error=unsafe_error.body.get("message"),
        )

        first_start = host.start()
        jvm_id = require_process_identity(first_start)
        report(
            "Rust and the embedded JVM share one OS process",
            java_process_id=first_start.body["java_process_id"],
            rust_process_id=first_start.body["rust_process_id"],
            jvm_id=jvm_id,
        )
        initial_slot = wait_slot(pg, active=True)
        idle = host.poll()
        require(idle.body.get("kind") == "idle", "poll timeout was not reported as idle")
        report("poll timeout is an idle result")
        hold_seconds = args.flush_interval_ms * args.flush_intervals / 1000.0
        stable_slot = stable_confirmed_flush(pg, hold_seconds)
        baseline = stable_slot.confirmed_flush
        report("single active consumer after start", slot=initial_slot.__dict__)

        pg.execute(
            "BEGIN; "
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES "
            "(101, 1, 'first'), (102, 2, 'second'), (103, 3, 'third'); "
            "COMMIT;"
        )
        first = host.delivery()
        assert_complete_position(first.body)
        require(
            postgres_committable_lsn(first.body) > baseline,
            "first delivery did not move beyond the slot baseline",
        )
        require(event_ids(first.body) == [101, 102, 103], "multi-row transaction order changed")
        report(
            "multi-row transaction order and complete position",
            ids=event_ids(first.body),
            partition=first.body["partition"],
            offset=first.body["offset"],
        )

        first_token = int(first.body["token"])
        offset_before_first_ack = read_offset_file(offset_file)
        host.error("poll 1 1", "exceeding poll maxBytes=1")
        host.error(f"ack {first_token + 1}", "does not match outstanding token")
        status_after_wrong_token = host.status()
        require(
            status_after_wrong_token.body.get("outstanding") is True,
            "wrong ACK hid outstanding state",
        )
        require(
            status_after_wrong_token.body.get("token") == first_token,
            "wrong ACK changed the token",
        )
        after_rejections = host.delivery()
        require(
            after_rejections.raw == first.raw,
            "max-bytes or wrong-token rejection changed the outstanding delivery",
        )
        report(
            "bounded poll and wrong-token errors preserve the outstanding delivery",
            token=first_token,
        )

        pg.execute(
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES (201, 1, 'blocked');"
        )
        time.sleep(hold_seconds)
        held_slot = pg.slot()
        require(
            held_slot.confirmed_flush == baseline,
            "confirmed_flush_lsn advanced while the first delivery was unacknowledged",
        )
        require(
            read_offset_file(offset_file) == offset_before_first_ack,
            "file offset store changed while the first delivery was unacknowledged",
        )
        repeats = [host.delivery(), host.delivery(), host.delivery()]
        for repeated in repeats:
            require(repeated.raw == first.raw, "outstanding delivery was not byte-for-byte stable")
        require(pg.slot().confirmed_flush == baseline, "repeated polls advanced confirmed_flush_lsn")
        report(
            "unacknowledged delivery survives multiple configured observation intervals",
            observation_intervals=args.flush_intervals,
            interval_ms=args.flush_interval_ms,
            confirmed_flush=baseline,
            offset_file_sha256=digest(offset_before_first_ack),
        )
        report("single outstanding delivery backpressure", token=first.body["token"])

        host.ack(first_token)
        after_first_ack = wait_until(
            "confirmed_flush_lsn to advance after ACK",
            pg.slot,
            lambda slot: slot.confirmed_flush > baseline,
        )
        report(
            "ACK advances confirmed_flush_lsn",
            before=baseline,
            after=after_first_ack.confirmed_flush,
        )
        offset_after_first_ack = wait_until(
            "file offset store to change after the first ACK",
            lambda: read_offset_file(offset_file),
            lambda value: value != offset_before_first_ack,
        )
        report(
            "ACK advances the standard file offset store",
            before_sha256=digest(offset_before_first_ack),
            after_sha256=digest(offset_after_first_ack),
        )

        second = host.delivery()
        second_token = int(second.body["token"])
        require(second_token != first_token, "a later delivery reused the first token")
        require(event_ids(second.body) == [201], "later batch was lost or reordered after ACK")
        assert_complete_position(second.body)
        require(
            after_first_ack.confirmed_flush < postgres_committable_lsn(second.body),
            "the first ACK advanced PostgreSQL through the still-unacknowledged second delivery",
        )
        require(
            pg.slot().confirmed_flush == after_first_ack.confirmed_flush,
            "the second delivery advanced PostgreSQL before its ACK",
        )
        host.error(f"ack {first_token}", "does not match outstanding token")
        status_after_stale_ack = host.status()
        require(
            status_after_stale_ack.body.get("outstanding") is True,
            "stale ACK hid outstanding state",
        )
        require(
            status_after_stale_ack.body.get("token") == second_token,
            "stale ACK changed the delivery",
        )
        require(
            host.delivery().raw == second.raw,
            "stale ACK changed the outstanding delivery bytes",
        )
        offset_before_second_ack = read_offset_file(offset_file)
        host.ack(second_token)
        after_second_ack = wait_until(
            "second ACK to reach PostgreSQL",
            pg.slot,
            lambda slot: slot.confirmed_flush > after_first_ack.confirmed_flush,
        )
        offset_after_second_ack = wait_until(
            "file offset store to change after the second ACK",
            lambda: read_offset_file(offset_file),
            lambda value: value != offset_before_second_ack,
        )
        host.error(f"ack {second_token}", "no outstanding delivery")
        status_after_repeated_ack = host.status()
        require(
            status_after_repeated_ack.body.get("outstanding") is False,
            "repeated ACK reported a phantom outstanding delivery",
        )
        report(
            "stale and repeated ACK tokens fail closed",
            stale_token=first_token,
            accepted_token=second_token,
            confirmed_flush=after_second_ack.confirmed_flush,
            offset_file_sha256=digest(offset_after_second_ack),
        )

        saved_partition = second.body["partition"]
        saved_offset = second.body["offset"]
        old_pid = pg.slot().active_pid
        host.stop()
        wait_slot(pg, active=False)
        pg.execute(
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES (301, 1, 'after restart');"
        )
        second_start = host.start()
        require_process_identity(second_start, jvm_id)
        restarted_slot = wait_slot(pg, active=True)
        require(restarted_slot.active_count == 1, "restart created more than one active slot consumer")
        recovered = host.delivery()
        require(
            event_ids(recovered.body) == [301],
            "combined file-offset/slot restart replayed an ACKed batch",
        )
        require(recovered.body["partition"] == saved_partition, "source partition changed after recovery")
        require(recovered.body["offset"] != saved_offset, "source offset did not advance after recovery")
        assert_complete_position(recovered.body)
        report(
            "stock FileOffsetBackingStore and persistent slot restart witness",
            partition=saved_partition,
            previous_offset=saved_offset,
            recovered_offset=recovered.body["offset"],
        )
        report(
            "stop/start single active consumer",
            previous_pid=old_pid,
            restarted_pid=restarted_slot.active_pid,
            active_count=restarted_slot.active_count,
            jvm_id=jvm_id,
        )

        before_unacked_restart = pg.slot().confirmed_flush
        require(
            before_unacked_restart < postgres_committable_lsn(recovered.body),
            "the recovered delivery was already acknowledged before restart testing",
        )
        recovered_token = int(recovered.body["token"])
        unacked_semantics = replay_semantics(recovered.body)
        offset_before_unacked_stop = read_offset_file(offset_file)
        stop_started = time.monotonic()
        stop_result = host.request(
            "stop 0",
            lambda body: body.get("kind") == "error"
            or (
                body.get("kind") in {"state", "status"}
                and str(body.get("state", "")).upper() == "STOPPED"
            ),
            timeout=5.0,
        )
        if stop_result.body.get("kind") == "error":
            message = stop_result.body.get("message")
            require(
                isinstance(message, str) and "did not stop within PT0S" in message,
                f"zero-deadline stop returned an unexpected error: {message!r}",
            )
        stop_error_elapsed_ms = round((time.monotonic() - stop_started) * 1000)
        require(
            stop_error_elapsed_ms < 5_000,
            "zero-deadline stop did not return a bounded error",
        )
        stopped_status = wait_until(
            "asynchronous stop to finish after its deadline",
            host.status,
            lambda response: str(response.body.get("state", "")).upper() == "STOPPED",
            timeout=30.0,
        )
        require_process_identity(stopped_status, jvm_id)
        wait_slot(pg, active=False)
        require(
            pg.slot().confirmed_flush == before_unacked_restart,
            "graceful stop ACKed an outstanding delivery",
        )
        require(
            read_offset_file(offset_file) == offset_before_unacked_stop,
            "stopping persisted the unacknowledged delivery offset",
        )
        report(
            "stop deadline is bounded and shutdown completes asynchronously",
            elapsed_ms=stop_error_elapsed_ms,
            immediate_outcome=(
                "timeout" if stop_result.body.get("kind") == "error" else "stopped"
            ),
        )
        third_start = host.start()
        require_process_identity(third_start, jvm_id)
        wait_slot(pg, active=True)
        replayed = host.delivery()
        replayed_token = int(replayed.body["token"])
        require(
            replayed_token != recovered_token,
            "a fresh Engine handle reused an earlier delivery token",
        )
        replayed_semantics = replay_semantics(replayed.body)
        (args.artifacts_dir / "unacknowledged-before-restart.json").write_text(
            unacked_semantics + "\n", encoding="utf-8"
        )
        (args.artifacts_dir / "unacknowledged-after-restart.json").write_text(
            replayed_semantics + "\n", encoding="utf-8"
        )
        require(replayed_semantics == unacked_semantics, "unacknowledged delivery changed across restart")
        host.error(f"ack {recovered_token}", "does not match outstanding token")
        status_after_restart_stale_ack = host.status()
        require(
            status_after_restart_stale_ack.body.get("token") == replayed_token,
            "cross-handle stale ACK changed the new outstanding token",
        )
        require(
            host.delivery().raw == replayed.raw,
            "cross-handle stale ACK changed the replayed delivery",
        )
        require(
            read_offset_file(offset_file) == offset_before_unacked_stop,
            "fresh Engine persisted the replay before ACK",
        )
        host.ack(replayed_token)
        wait_until(
            "replayed delivery ACK to advance PostgreSQL",
            pg.slot,
            lambda slot: slot.confirmed_flush > before_unacked_restart,
        )
        offset_after_replay_ack = wait_until(
            "file offset store to change after replay ACK",
            lambda: read_offset_file(offset_file),
            lambda value: value != offset_before_unacked_stop,
        )
        report(
            "unacknowledged stop/start replay",
            offset=replayed.body["offset"],
            previous_token=recovered_token,
            replay_token=replayed_token,
            offset_file_sha256=digest(offset_after_replay_ack),
        )

        final_stop = host.stop()
        require_process_identity(final_stop, jvm_id)
        final_slot = wait_slot(pg, active=False)
        report("clean stop", slot=final_slot.__dict__)

        missing_connector = dict(connector_configuration)
        missing_connector["name"] = "dogpaddle-debezium-d1-missing-connector"
        missing_connector["connector.class"] = (
            "dev.dogpaddle.experiments.MissingD1Connector"
        )
        missing_path = args.state_dir / "missing-connector.json"
        missing_path.write_text(canonical_json(missing_connector), encoding="utf-8")
        create_result = host.request(
            "create /state/missing-connector.json",
            lambda body: body.get("kind") in {"state", "error"},
            timeout=30.0,
        )
        if create_result.body.get("kind") == "error":
            connector_failure = create_result
        else:
            connector_failure = host.error("start 10000", "engine did not start")
        require(
            "MissingD1Connector" in str(connector_failure.body.get("message", "")),
            "missing connector failure did not retain its cause",
        )
        status_after_connector_failure = host.status()
        require_process_identity(status_after_connector_failure, jvm_id)
        require(
            str(status_after_connector_failure.body.get("state", "")).upper()
            in {"STOPPED", "FAILED"},
            "connector failure left the handle in an unstable state",
        )
        report(
            "connector class-loading failure is structured and contained",
            state=status_after_connector_failure.body.get("state"),
            error=connector_failure.body.get("message"),
        )
    finally:
        host.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-port", type=int, default=55432)
    parser.add_argument("--flush-interval-ms", type=int, default=500)
    parser.add_argument("--flush-intervals", type=int, default=4)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--connector-fixture", type=Path, required=True)
    parser.add_argument("host_command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    require(args.flush_intervals >= 3, "the unacknowledged hold must span at least three intervals")
    require(args.flush_interval_ms > 0, "flush interval must be positive")
    require(bool(args.host_command), "host command is required after --")
    if args.host_command[0] == "--":
        args.host_command = args.host_command[1:]
    args.state_dir = args.state_dir.resolve()
    args.artifacts_dir = args.artifacts_dir.resolve()
    args.connector_fixture = args.connector_fixture.resolve()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    args.artifacts_dir.mkdir(parents=True, exist_ok=True)
    return args


def main() -> int:
    try:
        run(parse_args())
    except (GateFailure, subprocess.CalledProcessError) as error:
        print(canonical_json({"result": "FAIL", "error": str(error)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
