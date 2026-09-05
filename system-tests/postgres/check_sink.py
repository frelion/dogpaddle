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
import subprocess
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Optional

from support import (
    ArtifactDirectory,
    PostgresCluster,
    absolute_existing,
    print_log_tails,
    require_executables,
    run,
)


PASSWORD = "dogpaddle-postgres-sink-gate"
EXPECTED = [2**64 - 3, 2**64 - 2, 2**64 - 1]


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

    def status(self) -> list[dict[str, Any]]:
        response = self.command("status")
        if response.get("kind") != "status":
            raise RuntimeError(f"unexpected host response: {response}")
        return list(response["stations"])

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
    def __init__(
        self,
        root: Path,
        pg_bin: Path,
        binary: Path,
        cluster: PostgresCluster,
    ) -> None:
        self.root, self.pg_bin, self.binary = root, pg_bin, binary
        self.cluster = cluster
        self.data = cluster.data
        self.flow = root / "flow"
        self.port = cluster.port
        self.pg_env = dict(os.environ, PGPASSWORD=PASSWORD)
        self.psql = [str(pg_bin / "psql"), "-X", "-h", "127.0.0.1",
                     "-p", str(self.port), "-U", "dogpaddle_gate",
                     "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At"]

    def start(self) -> None:
        password_file = self.root / "password"
        password_file.write_text(PASSWORD + "\n", encoding="utf-8")
        password_file.chmod(0o600)
        try:
            self.cluster.start(
                initdb_arguments=(
                    "-U",
                    "dogpaddle_gate",
                    "--pwfile",
                    str(password_file),
                    "--auth-local=trust",
                    "--auth-host=scram-sha-256",
                    "--no-instructions",
                    "--locale=C",
                    "-E",
                    "UTF8",
                ),
                server_options=(
                    "-c",
                    "fsync=on",
                    "-c",
                    "synchronous_commit=on",
                    "-c",
                    "log_statement=all",
                    "-c",
                    "log_parameter_max_length=0",
                ),
            )
        finally:
            password_file.unlink(missing_ok=True)

    def stop(self) -> None:
        self.cluster.stop()

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

    def host(self, mode: str, session: int) -> Host:
        return Host(self.binary, mode, self.flow, self.port,
                    self.root / f"host-{session}.log")

    def run_gate(self) -> None:
        with self.host("build", 1) as host:
            for _ in range(10):
                host.advance()
                if self.exists("events"):
                    break
            else:
                raise RuntimeError("target initialization did not finish")
            host.advance()  # settle the committed initialization
            assert self.rows() == []
            # A conflicting lock makes the actual PG write transaction fail.
            # Releasing it does not make the fail-stopped Flow reusable; only
            # reopen may replay intent.
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
            assert self.rows() == [], "failed PG transaction partially committed"
            host.kill()

        with self.host("open", 2) as host:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                host.advance()
                rows = self.rows()
                if len(rows) == 1:
                    if rows != [(1, EXPECTED[0])]:
                        raise RuntimeError(f"invalid first committed batch: {rows}")
                    # No further Flow round occurs: Store is durably Prepared while
                    # the target row is already committed.
                    host.kill()
                    break
                if len(rows) > 1:
                    raise RuntimeError("host passed the required first-batch crash window")
            else:
                raise RuntimeError("timed out waiting for the first target batch")

        with self.host("open", 3) as host:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                outcome = host.advance()
                if outcome == "Idle" and len(self.rows()) == len(EXPECTED):
                    break
            else:
                raise RuntimeError("timed out draining the reopened Flow")

            drained = {station["id"]: station for station in host.status()}
            assert drained["sequence"]["inputs"] == []
            assert drained["sequence"]["output"] == {
                "head": len(EXPECTED),
                "tail": len(EXPECTED),
                "retained_bytes": 0,
            }, drained
            assert drained["postgres"]["inputs"] == [
                {"cursor": len(EXPECTED), "tail": len(EXPECTED)}
            ], drained
            assert drained["postgres"]["output"] is None

        rows = self.rows()
        expected_rows = list(enumerate(EXPECTED, start=1))
        if rows != expected_rows:
            raise RuntimeError(f"target UInt64 rows differ: {rows}")

        with self.host("open", 4) as host:
            reopened = {station["id"]: station for station in host.status()}
            assert reopened == drained, (reopened, drained)
            assert host.advance() == "Idle"
            assert {station["id"]: station for station in host.status()} == drained
            fourth_rows = self.rows()
            if fourth_rows != expected_rows:
                raise RuntimeError(
                    f"fourth reopen changed target UInt64 rows or IDs: {fourth_rows}"
                )

        print("PASS target transaction failure rolls back data and fail-stops Flow; "
              "crash at externally committed/local Prepared boundary; reopen produced "
              "exactly three big-endian UInt64 rows with unchanged IDs; input "
              "head/tail/cursor=3/3/3 and retained_bytes=0; fourth reopen remained Idle")

    def direct_host(self, binary: Path, mode: str, scenario: str, session: int) -> Host:
        return Host(binary, mode, self.root / scenario, self.port,
                    self.root / f"host-{scenario}-{session}.log", scenario)

    def direct_state(self, scenario: str) -> tuple[int, str]:
        count, ids = self.sql(
            'SELECT count(*), COALESCE(string_agg("$dogpaddle.id"::text, '
            '\',\' ORDER BY "$dogpaddle.id"), \'\') '
            f'FROM public."{scenario}"'
        ).split("|", 1)
        return int(count), ids

    def initialize(self, host: Host, scenario: str) -> None:
        for _ in range(10):
            response = host.command("advance seed")
            assert response == {"kind": "advance", "outcome": "Commit"}, response
            if self.exists(scenario):
                break
        else:
            raise RuntimeError(f"{scenario} initialization did not finish")
        assert host.command("advance seed") == {"kind": "advance", "outcome": "Commit"}
        assert self.direct_state(scenario) == (0, "")

    def drain(self, host: Host, scenario: str, stage: str) -> None:
        for _ in range(100):
            response = host.command(f"advance {stage}")
            if response.get("kind") != "advance":
                raise RuntimeError(f"{scenario}/{stage}: {response}")
            if response["outcome"] == "Complete":
                return
        raise RuntimeError(f"{scenario}/{stage} did not complete within 100 turns")

    def run_operation_gate(self, binary: Path) -> None:
        started = time.monotonic()
        with self.direct_host(binary, "build", "bulk", 1) as host:
            self.initialize(host, "bulk")
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
            assert host.command("advance mixed")["outcome"] == "Commit"
            empty = self.direct_state("bulk")
            assert empty[0] == 0
            # Every inserted ID in this Prepared is deleted in the same target
            # transaction. Replaying both lists must still leave an empty table.
            host.kill()

        with self.direct_host(binary, "open", "bulk", 6) as host:
            assert host.command("advance mixed")["outcome"] == "Commit"
            assert self.direct_state("bulk") == empty
            self.drain(host, "bulk", "mixed")
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

        with self.direct_host(binary, "build", "updates", 1) as host:
            self.drain(host, "updates", "seed")
            expected = "\n".join(f"{value + 1}|{value}" for value in range(1_000))
            assert self.sql('SELECT "$dogpaddle.id", value FROM public.updates '
                            'ORDER BY "$dogpaddle.id"') == expected
            log_start = (self.root / "postgres.log").stat().st_size
            self.drain(host, "updates", "update")
            with (self.root / "postgres.log").open("rb") as stream:
                stream.seek(log_start)
                update_log = stream.read().decode("utf-8")
            expected = "\n".join(f"{value + 1}|{value}" for value in range(1_000, 2_000))
            assert self.sql('SELECT "$dogpaddle.id", value FROM public.updates '
                            'ORDER BY "$dogpaddle.id"') == expected
            update_inserts = update_log.count('INSERT INTO "public"."updates" (')
            update_deletes = update_log.count('DELETE FROM ONLY "public"."updates" ')
            update_lookups = update_log.count('SELECT request.n, CASE WHEN cardinality(selected.ids)')
            assert (update_lookups, update_inserts, update_deletes) == (2, 2, 2), (
                update_lookups, update_inserts, update_deletes)
        with self.direct_host(binary, "open", "updates", 2) as host:
            self.drain(host, "updates", "withdraw")
            assert self.direct_state("updates") == (0, "")

        # Server execution counts are a deterministic batching oracle, not a
        # throughput benchmark. Idempotent recovery deliberately executes the
        # same fixed writes again.
        log = (self.root / "postgres.log").read_text(encoding="utf-8")
        inserts = log.count('INSERT INTO "public"."bulk" (')
        deletes = log.count('DELETE FROM ONLY "public"."bulk" ')
        assert (inserts, deletes) == (20, 21), (inserts, deletes)
        assert log.count('INSERT INTO "public"."wide" (') == 2
        print(f"PASS 16,385-row insert/retract, both commit crash windows, rollback, "
              f"negative-prefix rejection, ordered mixed events, NULL/bit-exact values, "
              f"empty/1,598-column schemas; {inserts} INSERT/{deletes} DELETE statements, "
              f"1,000 distinct updates use {update_lookups} lookup/"
              f"{update_inserts} INSERT/{update_deletes} DELETE; "
              f"stable technical IDs ({time.monotonic() - started:.2f}s)")


def report_stop_failure(root: Path, error: BaseException) -> None:
    print(
        "failed to stop the isolated PostgreSQL cluster; a process may still be "
        f"running and the fixture is retained at {root}",
        file=os.sys.stderr,
    )
    traceback.print_exception(type(error), error, error.__traceback__, file=os.sys.stderr)


def resolve_hosts(
    parser: argparse.ArgumentParser,
    supplied_host: Optional[Path],
    supplied_recovery_host: Optional[Path],
) -> tuple[Path, Path]:
    if (supplied_host is None) != (supplied_recovery_host is None):
        parser.error("--host and --recovery-host must be supplied together")
    if supplied_host is not None and supplied_recovery_host is not None:
        try:
            binary = absolute_existing(supplied_host, "--host")
            recovery_binary = absolute_existing(
                supplied_recovery_host, "--recovery-host"
            )
        except (OSError, ValueError) as error:
            parser.error(str(error))
    else:
        repository = Path(__file__).resolve().parents[2]
        print("building dogpaddle-postgres-system-hosts release binaries", flush=True)
        run(
            [
                "cargo",
                "build",
                "--locked",
                "--release",
                "-p",
                "dogpaddle-postgres-system-hosts",
                "--bins",
            ],
            cwd=repository,
            timeout=1_200,
        )
        metadata = json.loads(
            run(
                [
                    "cargo",
                    "metadata",
                    "--locked",
                    "--no-deps",
                    "--format-version",
                    "1",
                ],
                cwd=repository,
            )
        )
        release_directory = Path(metadata["target_directory"]) / "release"
        binary = (release_directory / "postgres_sink").resolve(strict=True)
        recovery_binary = (
            release_directory / "postgres_sink_recovery"
        ).resolve(strict=True)
    for candidate in (binary, recovery_binary):
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            parser.error(f"PostgreSQL gate host is not executable: {candidate}")
    return binary, recovery_binary


def main() -> None:
    if not __debug__:
        raise RuntimeError("this correctness gate requires assertions; run Python without -O")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path)
    parser.add_argument("--recovery-host", type=Path)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--keep", action="store_true",
                        help="retain this run's temporary cluster and logs")
    args = parser.parse_args()
    try:
        pg_bin = absolute_existing(args.postgres_bin, "--postgres-bin")
        require_executables(pg_bin, ("initdb", "pg_ctl", "postgres", "psql"))
    except (OSError, ValueError) as error:
        parser.error(str(error))
    binary, operation_binary = resolve_hosts(
        parser, args.host, args.recovery_host
    )
    try:
        artifacts = ArtifactDirectory.create(
            args.artifacts_dir,
            prefix="dogpaddle-pg-sink-",
            keep_temporary=args.keep,
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))

    print(run([str(pg_bin / "postgres"), "--version"]))
    root = artifacts.root
    if (root / "data").exists():
        raise RuntimeError(f"refusing to overwrite an existing Sink fixture: {root}")
    print(f"isolated fixture: {root}", flush=True)
    cluster = PostgresCluster(
        root, pg_bin, socket_directory=root / "socket"
    )
    gate = Gate(root, pg_bin, binary, cluster)
    failure: Optional[BaseException] = None
    passed = False
    stopped = False
    try:
        gate.start()
        gate.run_gate()
        gate.run_operation_gate(operation_binary)
        passed = True
    except BaseException as error:
        failure = error
        print_log_tails([cluster.log, *sorted(root.glob("host-*.log"))])
        raise
    finally:
        try:
            gate.stop()
            stopped = True
        except BaseException as error:
            report_stop_failure(root, error)
            if failure is None:
                raise
        if passed and stopped and not artifacts.retain:
            artifacts.remove()
            print("removed the temporary PostgreSQL cluster and Flow")
        else:
            print(f"fixture retained at {root}", file=os.sys.stderr)


if __name__ == "__main__":
    main()
