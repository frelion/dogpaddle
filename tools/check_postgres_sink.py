#!/usr/bin/env python3
"""Native PostgreSQL sink correctness and crash/reopen gate (Python 3.9+).

The gate initializes and owns a temporary loopback-only PostgreSQL cluster. It
never discovers or contacts an existing server, and ordinary Cargo gates only
compile the Rust host; they do not execute this script.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Optional


PASSWORD = "dogpaddle-postgres-sink-gate"
EXPECTED = [2**64 - 3, 2**64 - 2, 2**64 - 1]
RECEIPT = '"$dogpaddle.receipt.gate_sink"'


def run(command: list[str], *, cwd: Optional[Path] = None,
        env: Optional[dict[str, str]] = None, timeout: int = 60) -> str:
    result = subprocess.run(command, cwd=cwd, env=env, check=True,
                            capture_output=True, text=True, timeout=timeout)
    return result.stdout.strip()


class Host:
    def __init__(self, binary: Path, mode: str, flow: Path, port: int, log: Path,
                 scenario: Optional[str] = None) -> None:
        self.stderr = log.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [str(binary), mode, str(flow), str(port)] + ([scenario] if scenario else []),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                text=True, bufsize=1,
                env=dict(os.environ, DOGPADDLE_GATE_PASSWORD=PASSWORD),
            )
        except BaseException:
            self.stderr.close()
            raise
        self.responses: queue.Queue[Any] = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            ready = self.receive()
            if ready != {"kind": "ready", "mode": mode}:
                raise RuntimeError(f"unexpected host startup: {ready}")
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> Host:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _read(self) -> None:
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                if line.strip():
                    self.responses.put(json.loads(line))
        except (OSError, ValueError) as error:
            self.responses.put(error)
        finally:
            self.responses.put(RuntimeError("host stdout closed"))

    def receive(self) -> dict[str, Any]:
        response = self.responses.get(timeout=60)
        if isinstance(response, BaseException):
            raise response
        return response

    def advance(self) -> str:
        response = self.command("advance")
        if response.get("kind") != "advance":
            raise RuntimeError(f"unexpected host response: {response}")
        return str(response["outcome"])

    def command(self, command: str) -> dict[str, Any]:
        assert self.process.stdin is not None
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        return self.receive()

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=15)

    def close(self) -> None:
        self.kill()
        self.reader.join(timeout=5)
        if self.process.stdin:
            self.process.stdin.close()
        if self.process.stdout:
            self.process.stdout.close()
        self.stderr.close()


class Gate:
    def __init__(self, root: Path, pg_bin: Path, binary: Path) -> None:
        self.root, self.pg_bin, self.binary = root, pg_bin, binary
        self.data = root / "data"
        self.flow = root / "flow"
        self.started = False
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.pg_env = dict(os.environ, PGPASSWORD=PASSWORD)
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1",
                     "-p", str(self.port), "-U", "dogpaddle_gate",
                     "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]

    def start(self) -> None:
        password_file = self.root / "password"
        password_file.write_text(PASSWORD + "\n", encoding="utf-8")
        password_file.chmod(0o600)
        try:
            run([str(self.pg_bin / "initdb"), "-D", str(self.data),
                 "-U", "dogpaddle_gate", "--pwfile", str(password_file),
                 "--auth-local=trust", "--auth-host=scram-sha-256",
                 "--no-instructions", "--locale=C", "-E", "UTF8"])
        finally:
            password_file.unlink(missing_ok=True)
        socket_dir = self.root / "socket"
        socket_dir.mkdir()
        options = (f"-h 127.0.0.1 -p {self.port} -k {socket_dir} "
                   "-c fsync=on -c synchronous_commit=on "
                   "-c log_statement=all -c log_parameter_max_length=0")
        run([str(self.pg_bin / "pg_ctl"), "-D", str(self.data),
             "-l", str(self.root / "postgres.log"), "-o", options,
             "-w", "start"])
        self.started = True

    def stop(self) -> None:
        if self.started or (self.data / "postmaster.pid").exists():
            run([str(self.pg_bin / "pg_ctl"), "-D", str(self.data),
                 "-m", "immediate", "-w", "stop"])
        self.started = False

    def sql(self, statement: str) -> str:
        return run([*self.psql, "-c", statement], env=self.pg_env)

    def exists(self, relation: str) -> bool:
        return self.sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = 'public' AND c.relname = '{relation}')"
        ) == "t"

    def rows(self) -> list[tuple[int, int]]:
        if not self.exists("events"):
            return []
        lines = self.sql(
            'SELECT "$dogpaddle.id", encode("value", \'hex\') '
            'FROM public."events" ORDER BY "$dogpaddle.id"'
        )
        if not lines:
            return []
        result = []
        for line in lines.splitlines():
            raw_id, encoded = line.split("|")
            result.append((int(raw_id), int.from_bytes(bytes.fromhex(encoded), "big")))
        return result

    def receipts(self) -> list[tuple[int, str, int]]:
        if not self.exists("$dogpaddle.receipt.gate_sink"):
            return []
        lines = self.sql(
            'SELECT "$dogpaddle.delivery", encode("$dogpaddle.digest", \'hex\'), '
            f'"$dogpaddle.mutations" FROM public.{RECEIPT} '
            'ORDER BY "$dogpaddle.delivery"'
        )
        if not lines:
            return []
        return [(int(sequence), digest, int(count))
                for sequence, digest, count in
                (line.split("|") for line in lines.splitlines())]

    def host(self, mode: str, session: int) -> Host:
        return Host(self.binary, mode, self.flow, self.port,
                    self.root / f"host-{session}.log")

    def run_gate(self) -> None:
        with self.host("build", 1) as host:
            host.advance()  # initialize the target
            host.advance()  # settle Initialize -> Ready
            # A conflicting lock makes the actual PG transaction fail after its
            # receipt INSERT but before its target INSERT. Releasing it does not
            # make the fail-stopped Flow reusable; only reopen may replay intent.
            with subprocess.Popen(
                [*self.psql, "-c", "BEGIN; LOCK TABLE public.events IN ACCESS EXCLUSIVE MODE; "
                 "SELECT pg_sleep(60); ROLLBACK"],
                env=dict(self.pg_env, PGAPPNAME="dogpaddle_gate_lock"),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ) as locker:
                try:
                    deadline = time.monotonic() + 15
                    while self.sql("SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a "
                                   "ON a.pid=l.pid WHERE a.application_name='dogpaddle_gate_lock' "
                                   "AND l.relation='public.events'::regclass AND l.granted)") != "t":
                        if locker.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("fixture lock was not acquired")
                        time.sleep(0.02)
                    failure = host.command("advance")
                    assert failure.get("requires_reopen") is True, failure
                    retry = host.command("advance")
                    assert retry.get("requires_reopen") is True, retry
                    assert "station must be reopened" in retry["message"], retry
                finally:
                    self.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                             "WHERE application_name='dogpaddle_gate_lock'")
                    locker.wait(timeout=5)
            assert self.rows() == [] and self.receipts() == [], "failed PG transaction partially committed"
            host.kill()

        with self.host("open", 2) as host:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                host.advance()
                rows, receipts = self.rows(), self.receipts()
                if len(rows) == 1:
                    if rows != [(1, EXPECTED[0])] or [item[0] for item in receipts] != [1]:
                        raise RuntimeError(f"invalid first committed delivery: {rows}, {receipts}")
                    # No further Flow round occurs: Store is durably Prepared while
                    # the target receipt and row are already committed.
                    host.kill()
                    break
                if len(rows) > 1:
                    raise RuntimeError("host passed the required first-delivery crash window")
            else:
                raise RuntimeError("timed out waiting for the first target delivery")

        with self.host("open", 3) as host:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                outcome = host.advance()
                if outcome == "Idle" and len(self.rows()) == len(EXPECTED):
                    break
            else:
                raise RuntimeError("timed out draining the reopened Flow")

        rows, receipts = self.rows(), self.receipts()
        expected_rows = list(enumerate(EXPECTED, start=1))
        if rows != expected_rows:
            raise RuntimeError(f"target UInt64 rows differ: {rows}")
        if [item[0] for item in receipts] != [3]:
            raise RuntimeError(f"only the last receipt should remain: {receipts}")
        if any(len(digest) != 64 or count != 1 for _, digest, count in receipts):
            raise RuntimeError(f"receipt payload differs: {receipts}")
        print("PASS target transaction failure rolls back receipt/data and fail-stops Flow; "
              "crash at externally committed/local Prepared boundary; reopen produced "
              "exactly three big-endian UInt64 rows and retained only receipt 3")

    def direct_host(self, binary: Path, mode: str, scenario: str, session: int) -> Host:
        return Host(binary, mode, self.root / scenario, self.port,
                    self.root / f"host-{scenario}-{session}.log", scenario)

    def direct_state(self, scenario: str) -> tuple[int, str]:
        rows = int(self.sql(f'SELECT count(*) FROM public."{scenario}"'))
        receipts = self.sql(
            'SELECT "$dogpaddle.delivery", encode("$dogpaddle.digest", \'hex\'), '
            '"$dogpaddle.mutations" '
            f'FROM public."$dogpaddle.receipt.gate_{scenario}"'
        )
        if len(receipts.splitlines()) > 1:
            raise RuntimeError(f"settled receipts accumulated: {receipts}")
        return rows, receipts

    def drain(self, host: Host, scenario: str, stage: str) -> None:
        for _ in range(100):
            response = host.command(f"advance {stage}")
            if response.get("kind") != "advance":
                raise RuntimeError(f"{scenario}/{stage}: {response}")
            self.direct_state(scenario)
            if response["outcome"] == "Complete":
                return
        raise RuntimeError(f"{scenario}/{stage} did not complete within 100 turns")

    def run_operation_gate(self, binary: Path) -> None:
        started = time.monotonic()
        with self.direct_host(binary, "build", "bulk", 1) as host:
            for _ in range(2):  # initialize, then settle initialization
                assert host.command("advance seed") == {"kind": "advance", "outcome": "Commit"}
            assert host.command("rollback seed") == {"kind": "rollback", "unchanged": True}
            assert self.direct_state("bulk") == (0, "")
            assert host.command("prepare-only seed") == {"kind": "prepared"}
            assert self.direct_state("bulk") == (0, "")
            host.kill()

        with self.direct_host(binary, "open", "bulk", 2) as host:
            assert host.command("advance seed")["outcome"] == "Commit"
            first = self.direct_state("bulk")
            assert first[0] == 1024
            assert host.command("rollback seed") == {"kind": "rollback", "unchanged": True}
            assert self.direct_state("bulk") == first
            host.kill()

        with self.direct_host(binary, "open", "bulk", 3) as host:
            assert host.command("advance seed")["outcome"] == "Commit"
            assert self.direct_state("bulk") == first  # replay cannot insert twice
            self.drain(host, "bulk", "seed")
            seeded = self.direct_state("bulk")
            assert seeded[0] == 16_385
            assert self.sql('SELECT min("$dogpaddle.id"), max("$dogpaddle.id") FROM public.bulk') == "1|16385"
            response = host.command("advance missing")
            assert response["kind"] == "error" and "only 16385 exist" in response["message"], response
            assert self.direct_state("bulk") == seeded
            # The ordinary planning error above must not poison this instance.
            assert host.command("advance withdraw")["outcome"] == "Commit"
            deleted = self.direct_state("bulk")
            assert deleted[0] == 16_385 - 1024
            assert self.sql('SELECT min("$dogpaddle.id") FROM public.bulk') == "1025"
            host.kill()

        with self.direct_host(binary, "open", "bulk", 4) as host:
            assert host.command("advance withdraw")["outcome"] == "Commit"  # replay Prepared
            assert self.direct_state("bulk") == deleted
            assert host.command("rollback withdraw") == {"kind": "rollback", "unchanged": True}
            host.kill()

        with self.direct_host(binary, "open", "bulk", 5) as host:
            assert host.command("advance withdraw")["outcome"] == "Commit"
            assert self.direct_state("bulk") == deleted  # replay cannot delete twice
            self.drain(host, "bulk", "withdraw")
            assert self.direct_state("bulk")[0] == 0
            self.drain(host, "bulk", "mixed")
            empty = self.direct_state("bulk")
            assert empty[0] == 0
            response = host.command("advance invalid-prefix")
            assert response["kind"] == "error" and "only 0 exist" in response["message"], response
            assert self.direct_state("bulk") == empty

        for scenario, rows in (("types", 2), ("wide", 80), ("empty", 2)):
            with self.direct_host(binary, "build", scenario, 1) as host:
                self.drain(host, scenario, "seed")
                assert self.direct_state(scenario)[0] == rows
                if scenario == "types":
                    assert self.sql('SELECT encode(u64, \'hex\'), encode(f32, \'hex\'), '
                                    'encode(f64, \'hex\'), encode(text, \'hex\') '
                                    'FROM public.types WHERE u64 IS NOT NULL') == (
                                        "ffffffffffffffff|7f800123|8000000000000000|6265666f7265006166746572")
            with self.direct_host(binary, "open", scenario, 2) as host:
                self.drain(host, scenario, "withdraw")
                assert self.direct_state(scenario)[0] == 0

        # This is actual server execution evidence, not timing-sensitive unit
        # testing: 16,385 inserts/deletes use 17 statements each, plus 2 mixed
        # runs. Whole-event counts run once per admission, not once per batch.
        log = (self.root / "postgres.log").read_text(encoding="utf-8")
        inserts = log.count('INSERT INTO "public"."bulk" (')
        deletes = log.count('DELETE FROM ONLY "public"."bulk" AS target USING')
        counts = log.count('SELECT count(*) FROM (SELECT 1 FROM ONLY "public"."bulk"')
        assert (inserts, deletes, counts) == (19, 19, 2), (inserts, deletes, counts)
        assert log.count('INSERT INTO "public"."wide" (') == 2
        print(f"PASS 16,385-row insert/retract, both commit crash windows, rollback, "
              f"negative-prefix rejection, ordered mixed events, NULL/bit-exact values, "
              f"empty/1,598-column schemas; {inserts} INSERT/{deletes} DELETE statements, "
              f"{counts} admission counts, at most one receipt ({time.monotonic() - started:.2f}s)")


def main() -> None:
    if not __debug__:
        raise RuntimeError("this correctness gate requires assertions; run Python without -O")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", type=Path,
                        default=Path("/opt/homebrew/bin"))
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    pg_bin = args.postgres_bin.resolve(strict=True)
    for executable in ("initdb", "pg_ctl", "postgres", "psql"):
        (pg_bin / executable).resolve(strict=True)

    print(run([str(pg_bin / "postgres"), "--version"]))
    print(run(["rustc", "--version"]))
    subprocess.run(["cargo", "build", "--locked", "-p", "dogpaddle-flow",
                    "--example", "postgres_sink"], cwd=repo, check=True)
    subprocess.run(["cargo", "build", "--locked", "-p", "dogpaddle-operation",
                    "--example", "postgres_sink_recovery"], cwd=repo, check=True)
    metadata = json.loads(run(["cargo", "metadata", "--no-deps",
                               "--format-version", "1"], cwd=repo))
    binary = Path(metadata["target_directory"]) / "debug" / "examples" / "postgres_sink"
    operation_binary = binary.with_name("postgres_sink_recovery")

    with tempfile.TemporaryDirectory(prefix="dogpaddle-pg-sink-") as directory:
        root = Path(directory)
        gate = Gate(root, pg_bin, binary)
        try:
            gate.start()
            gate.run_gate()
            gate.run_operation_gate(operation_binary)
        except BaseException:
            for log in [root / "postgres.log", *sorted(root.glob("host-*.log"))]:
                if log.exists():
                    print(f"--- {log.name} (last 16 KiB) ---\n{log.read_text(encoding='utf-8')[-16384:]}",
                          file=os.sys.stderr)
            raise
        finally:
            gate.stop()
    print("removed the temporary PostgreSQL cluster and Flow")


if __name__ == "__main__":
    main()
