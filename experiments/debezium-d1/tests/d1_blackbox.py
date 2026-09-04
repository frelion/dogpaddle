#!/usr/bin/env python3
"""Black-box recovery gate for the product Debezium runtime and PostgreSQL."""

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
        response = self.request(command, lambda body: body.get("kind") == "error")
        message = response.body.get("message")
        require(
            isinstance(message, str) and message_fragment in message,
            f"error for {command!r} did not contain {message_fragment!r}: {message!r}",
        )
        return response

    def start(self) -> Response:
        return self.request(
            "start",
            lambda body: body.get("kind") == "state"
            and body.get("state") == "running",
            timeout=60.0,
        )

    def status(self) -> Response:
        return self.request("status", lambda body: body.get("kind") == "status")

    def poll(self, timeout_ms: int = 1_000) -> Response:
        return self.request(
            f"poll {timeout_ms}",
            lambda body: body.get("kind") in {"delivery", "idle"},
            timeout=max(30.0, timeout_ms / 1_000 + 10.0),
        )

    def delivery(self, timeout: float = 30.0) -> Response:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.poll()
            if response.body.get("kind") == "delivery":
                return response
        raise GateFailure("timeout waiting for a delivery")

    def save(self) -> Response:
        return self.request("save", lambda body: body.get("kind") == "saved")

    def ack(self, token: int) -> Response:
        return self.request(
            f"ack {token}",
            lambda body: body.get("kind") == "ack" and body.get("token") == token,
            timeout=60.0,
        )

    def stop(self) -> Response:
        return self.request(
            "stop",
            lambda body: body.get("kind") == "state" and body.get("state") == "stopped",
            timeout=60.0,
        )

    def close(self) -> None:
        if self._process.poll() is None:
            try:
                self.request("quit", lambda body: body.get("kind") == "bye", timeout=40.0)
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
    first = pg.slot()
    time.sleep(hold_seconds)
    second = pg.slot()
    require(
        first.active_count == 1 and second.active_count == 1,
        "replication slot became inactive during the observation window",
    )
    require(
        first.confirmed_flush == second.confirmed_flush,
        "confirmed_flush_lsn changed without ACK",
    )
    return second


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
    offset_keys = sorted(
        key for key in configuration if key == "offset" or key.startswith("offset.")
    )
    require(not offset_keys, f"Java offset-store properties remain in the fixture: {offset_keys!r}")
    return configuration


def checkpoint_bytes(delivery: dict[str, Any]) -> bytes:
    encoded = delivery.get("checkpoint")
    require(isinstance(encoded, str) and encoded, "delivery has no opaque checkpoint")
    try:
        decoded = bytes.fromhex(encoded)
    except ValueError as error:
        raise GateFailure("delivery checkpoint is not lowercase hexadecimal") from error
    require(encoded == decoded.hex(), "delivery checkpoint is not canonical lowercase hexadecimal")
    require(
        delivery.get("checkpoint_bytes") == len(decoded),
        "checkpoint_bytes does not match the opaque checkpoint",
    )
    return decoded


def business_events(delivery: dict[str, Any]) -> list[dict[str, Any]]:
    events = delivery.get("events")
    require(isinstance(events, list), "delivery.events is not an array")
    require(delivery.get("record_count") == len(events), "record_count does not match events")
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


def replay_semantics(delivery: dict[str, Any]) -> str:
    """Compare decoded records while excluding only Engine-run metadata."""
    projected_events: list[dict[str, Any]] = []
    events = delivery.get("events")
    require(isinstance(events, list), "delivery.events is not an array")
    for event in events:
        require(isinstance(event, dict), "delivery contains a non-object event")
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
        projected: dict[str, Any] = {
            "topic": event.get("topic"),
            "kafka_partition": event.get("kafka_partition"),
            "key": event.get("key"),
            "headers": headers,
        }
        value = event.get("value")
        if isinstance(value, dict) and isinstance(value.get("payload"), dict):
            payload = dict(value["payload"])
            payload.pop("ts_ms", None)
            payload.pop("ts_us", None)
            payload.pop("ts_ns", None)
            projected["value"] = {"schema": value.get("schema"), "payload": payload}
        else:
            projected["value"] = value
        projected_events.append(projected)
    return canonical_json(projected_events)


def require_delivery(delivery: Response, expected_ids: list[int]) -> bytes:
    require(delivery.body.get("protocol") == 2, "unexpected diagnostic protocol")
    require(event_ids(delivery.body) == expected_ids, f"expected IDs {expected_ids!r}")
    return checkpoint_bytes(delivery.body)


def report(gate: str, **evidence: Any) -> None:
    print(canonical_json({"result": "PASS", "gate": gate, **evidence}), flush=True)


def run(args: argparse.Namespace) -> None:
    require_connector_fixture(args.connector_fixture)
    pg = Pg(args.pg_port)
    pg.reset()
    checkpoint_path = args.state_dir / "checkpoint.bin"
    if checkpoint_path.exists():
        checkpoint_path.unlink()

    host = Host(args.host_command, args.artifacts_dir / "host.stderr.log")
    try:
        first_start = host.start()
        require(
            first_start.body.get("resumed_checkpoint_bytes") == 0,
            "first Engine resumed a checkpoint",
        )
        wait_slot(pg, active=True)
        idle = host.poll()
        require(idle.body.get("kind") == "idle", "ordinary poll timeout did not return idle")

        hold_seconds = args.flush_interval_ms * args.flush_intervals / 1_000
        baseline = stable_confirmed_flush(pg, hold_seconds).confirmed_flush
        pg.execute(
            "BEGIN; "
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES "
            "(101, 1, 'first'), (102, 2, 'second'), (103, 3, 'third'); "
            "COMMIT;"
        )
        first = host.delivery()
        first_checkpoint = require_delivery(first, [101, 102, 103])
        report(
            "ordinary timeout leaves the connector usable",
            ids=event_ids(first.body),
            checkpoint_bytes=len(first_checkpoint),
        )

        first_token = int(first.body["token"])
        host.error(f"ack {first_token}", "checkpoint must be saved before ACK")
        repeats = [host.delivery(), host.delivery(), host.delivery()]
        require(
            all(repeated.raw == first.raw for repeated in repeats),
            "dropping Delivery changed its records, checkpoint, or host-local token",
        )
        held = stable_confirmed_flush(pg, hold_seconds)
        require(held.confirmed_flush == baseline, "an unacknowledged Delivery advanced PostgreSQL")
        report(
            "dropped Delivery re-polls with identical bytes and checkpoint",
            token=first_token,
            observations=len(repeats) + 1,
            checkpoint_sha256=hashlib.sha256(first_checkpoint).hexdigest(),
            confirmed_flush=baseline,
        )

        host.stop()
        wait_slot(pg, active=False)
        require(not checkpoint_path.exists(), "unsaved Delivery unexpectedly created a checkpoint")
        require(
            pg.slot().confirmed_flush == baseline,
            "stop acknowledged the outstanding unsaved Delivery",
        )

        replay_start = host.start()
        require(
            replay_start.body.get("resumed_checkpoint_bytes") == 0,
            "fresh Engine unexpectedly resumed a checkpoint",
        )
        wait_slot(pg, active=True)
        replayed_first = host.delivery()
        replayed_first_checkpoint = require_delivery(replayed_first, [101, 102, 103])
        require(
            replayed_first_checkpoint == first_checkpoint,
            "fresh Engine changed the replay candidate checkpoint",
        )
        require(
            replay_semantics(replayed_first.body) == replay_semantics(first.body),
            "fresh Engine did not replay the unsaved, unacknowledged batch",
        )
        report(
            "fresh Engine without a saved checkpoint replays the batch",
            previous_token=first_token,
            replay_token=replayed_first.body["token"],
        )

        saved_first = host.save()
        require(
            checkpoint_path.read_bytes() == checkpoint_bytes(replayed_first.body),
            "saved candidate changed",
        )
        require(
            saved_first.body.get("checkpoint_bytes") == checkpoint_path.stat().st_size,
            "save response has the wrong checkpoint size",
        )
        before_candidate_stop = pg.slot().confirmed_flush
        host.stop()
        wait_slot(pg, active=False)
        require(
            pg.slot().confirmed_flush == before_candidate_stop == baseline,
            "saving a pre-ACK checkpoint or stopping acknowledged its batch",
        )

        pg.execute(
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES (201, 1, 'after candidate');"
        )
        candidate_start = host.start()
        require(
            candidate_start.body.get("resumed_checkpoint_bytes")
            == checkpoint_path.stat().st_size,
            "fresh Engine did not load the opaque checkpoint",
        )
        wait_slot(pg, active=True)
        after_candidate = host.delivery()
        require_delivery(after_candidate, [201])
        confirmed_before_candidate_ack = stable_confirmed_flush(
            pg, hold_seconds
        ).confirmed_flush
        report(
            "saved pre-ACK checkpoint alone skips the taken-over batch",
            skipped_ids=[101, 102, 103],
            next_ids=event_ids(after_candidate.body),
            postgres_confirmed_flush=confirmed_before_candidate_ack,
        )

        host.save()
        after_candidate_token = int(after_candidate.body["token"])
        host.ack(after_candidate_token)
        lsn_at_ack_return = pg.slot().confirmed_flush
        post_ack_poll = host.poll()
        require(
            post_ack_poll.body.get("kind") == "idle",
            "post-ACK source poll unexpectedly produced a delivery",
        )
        host.stop()
        after_candidate_ack = wait_slot(pg, active=False)
        require(
            after_candidate_ack.confirmed_flush > confirmed_before_candidate_ack,
            "the accepted PostgreSQL position was not eventually flushed by the next poll/stop",
        )
        report(
            "durable checkpoint precedes ACK and PostgreSQL flush is eventual",
            token=after_candidate_token,
            confirmed_flush_before=confirmed_before_candidate_ack,
            confirmed_flush_at_ack_return=lsn_at_ack_return,
            confirmed_flush_after=after_candidate_ack.confirmed_flush,
            observation_boundary="next source poll plus graceful stop",
        )
        accepted_restart = host.start()
        require(
            accepted_restart.body.get("resumed_checkpoint_bytes")
            == checkpoint_path.stat().st_size,
            "post-ACK Engine did not resume the accepted checkpoint",
        )
        wait_slot(pg, active=True)

        pg.execute(
            "INSERT INTO public.d1_events(id, tx_seq, payload) VALUES (301, 1, 'stop outstanding');"
        )
        before_stop = host.delivery()
        before_stop_checkpoint = require_delivery(before_stop, [301])
        before_stop_semantics = replay_semantics(before_stop.body)
        saved_checkpoint = checkpoint_path.read_bytes()
        confirmed_before_stop = pg.slot().confirmed_flush
        time.sleep(hold_seconds)
        require(
            pg.slot().confirmed_flush == confirmed_before_stop,
            "outstanding batch advanced before stop",
        )
        host.stop()
        wait_slot(pg, active=False)
        require(
            checkpoint_path.read_bytes() == saved_checkpoint,
            "stop saved an outstanding checkpoint",
        )
        require(
            pg.slot().confirmed_flush == confirmed_before_stop,
            "stop acknowledged the outstanding batch",
        )

        final_start = host.start()
        require(
            final_start.body.get("resumed_checkpoint_bytes") == len(saved_checkpoint),
            "final Engine did not resume the last accepted checkpoint",
        )
        wait_slot(pg, active=True)
        after_stop = host.delivery()
        after_stop_checkpoint = require_delivery(after_stop, [301])
        require(
            after_stop_checkpoint == before_stop_checkpoint,
            "stop/restart changed the replay candidate checkpoint",
        )
        require(
            replay_semantics(after_stop.body) == before_stop_semantics,
            "stop changed the outstanding source semantics",
        )
        report(
            "stop with an outstanding Delivery does not ACK it",
            previous_token=before_stop.body["token"],
            replay_token=after_stop.body["token"],
            checkpoint_sha256=hashlib.sha256(after_stop_checkpoint).hexdigest(),
        )

        host.save()
        host.ack(int(after_stop.body["token"]))
        final_lsn_at_ack_return = pg.slot().confirmed_flush
        host.stop()
        final_slot = wait_slot(pg, active=False)
        require(
            final_slot.confirmed_flush > confirmed_before_stop,
            "graceful stop did not eventually flush the final ACKed position",
        )

        files = sorted(
            str(path.relative_to(args.state_dir))
            for path in args.state_dir.rglob("*")
            if path.is_file()
        )
        require(files == ["checkpoint.bin"], f"unexpected Java or host state files: {files!r}")
        report(
            "opaque Rust checkpoint is the only durable offset truth",
            files=files,
            checkpoint_sha256=hashlib.sha256(checkpoint_path.read_bytes()).hexdigest(),
            confirmed_flush_at_ack_return=final_lsn_at_ack_return,
            final_confirmed_flush=final_slot.confirmed_flush,
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
    except (GateFailure, OSError, subprocess.CalledProcessError) as error:
        print(canonical_json({"result": "FAIL", "error": str(error)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
