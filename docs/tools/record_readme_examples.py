#!/usr/bin/env python3
"""Record README examples in a real terminal and render their asciicasts to GIF."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Optional


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "system-tests/postgres"))

from support import PostgresCluster, absolute_existing, require_executables, run  # noqa: E402


PASSWORD = "dogpaddle-readme"
CYAN = "\033[36m"
GREEN = "\033[32m"
AMBER = "\033[33m"
MUTED = "\033[90m"
RESET = "\033[0m"


class Session:
    def __init__(
        self,
        scenario: str,
        port: int,
        postgres_bin: Path,
        bundle: Path,
        binary: Path,
        tmux: Path,
        state: Path,
        log: Path,
    ) -> None:
        self.scenario = scenario
        self.port = port
        self.postgres_bin = postgres_bin
        self.bundle = bundle
        self.binary = binary
        self.tmux_binary = tmux
        self.log = log
        self.process: Optional[subprocess.Popen[str]] = None
        self.database = "trade_asof" if scenario == "trade-quote-asof" else "store_sales"
        self.example = REPOSITORY / "examples" / scenario
        self.work = state.parent
        self.work.mkdir(parents=True, exist_ok=True)
        (self.work / "pipeline.sql").symlink_to(self.example / "pipeline.sql")
        (self.work / "dogpaddle").symlink_to(binary)
        self.tmux_socket = f"dogpaddle-readme-{os.getpid()}-{scenario}"
        self.connection = (
            f"postgresql://dogpaddle:{PASSWORD}@127.0.0.1:{port}/{self.database}"
        )

    def psql_environment(self) -> dict[str, str]:
        return dict(
            os.environ,
            PATH=f"{self.postgres_bin}{os.pathsep}{os.environ.get('PATH', '')}",
            PGHOST="127.0.0.1",
            PGPORT=str(self.port),
            PGUSER="dogpaddle",
            PGDATABASE=self.database,
            PGPASSWORD=PASSWORD,
        )

    def psql(self, statement: str, *, tuples: bool = True) -> str:
        command = [
            str(self.postgres_bin / "psql"),
            "-X", "-h", "127.0.0.1", "-p", str(self.port),
            "-U", "dogpaddle", "-d", self.database,
            "-v", "ON_ERROR_STOP=1",
        ]
        if tuples:
            command.append("-At")
        command.extend(("-c", statement))
        result = subprocess.run(
            command,
            env=self.psql_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.stdout.strip()

    def wait(self, description: str, condition: Callable[[], bool]) -> None:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                detail = self.log.read_text(encoding="utf-8", errors="replace")
                raise RuntimeError(
                    f"DogPaddle stopped while waiting for {description}\n{detail[-20_000:]}"
                )
            try:
                if condition():
                    return
            except subprocess.CalledProcessError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"timed out waiting for {description}")

    def clear(self, title: str) -> None:
        print("\033[2J\033[H", end="")
        print(f"{CYAN}DOGPADDLE · {title}{RESET}")
        print(f"{MUTED}{'─' * 116}{RESET}")

    def show_command(self, command: str) -> None:
        print(f"{CYAN}${RESET} {command}")

    def start(self) -> None:
        command = ["./dogpaddle", "run", "pipeline.sql", "--state", "./state"]
        self.show_command(f"{shlex.join(command)} >dogpaddle.log 2>&1 &")
        environment = dict(
            os.environ,
            DOGPADDLE_DEBEZIUM_RUNTIME=str(self.bundle),
        )
        if self.scenario == "trade-quote-asof":
            environment.update({
                "DOGPADDLE_FILLS_SOURCE": self.connection,
                "DOGPADDLE_QUOTES_SOURCE": self.connection,
                "DOGPADDLE_EXECUTION_QUALITY_TARGET": self.connection,
            })
        else:
            environment.update({
                "DOGPADDLE_STORE_SALES_SOURCE": self.connection,
                "DOGPADDLE_STORE_SALES_TARGET": self.connection,
            })
        output = self.log.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                command,
                cwd=self.work,
                env=environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
            )
        finally:
            output.close()

    def stop(self) -> None:
        process, self.process = self.process, None
        if process is None:
            return
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        forced = False
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            forced = True
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        if forced:
            raise RuntimeError("DogPaddle did not stop within 30 seconds after SIGINT")
        if process.returncode not in {0, 130, -signal.SIGINT}:
            raise RuntimeError(f"DogPaddle exited with status {process.returncode}")

    def show_program(self, title: str) -> None:
        self.clear(title)
        print((self.example / "pipeline.sql").read_text(encoding="utf-8").rstrip())
        print()
        sys.stdout.flush()
        time.sleep(2.5)

    def run_step(self, step: Path) -> None:
        subprocess.run(
            [
                str(self.postgres_bin / "psql"),
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                str(step),
            ],
            cwd=REPOSITORY,
            env=self.psql_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def tmux(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(self.tmux_binary),
                "-L",
                self.tmux_socket,
                "-f",
                "/dev/null",
                *arguments,
            ],
            env=self.psql_environment(),
            check=check,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def pane_command(self, side: str) -> str:
        return shlex.join(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--pane",
                self.scenario,
                "--side",
                side,
                "--port",
                str(self.port),
                "--postgres-bin",
                str(self.postgres_bin),
            ]
        )

    def refresh_pane(self, pane: str, side: str) -> None:
        self.tmux(
            "respawn-pane",
            "-k",
            "-t",
            f"dashboard:0.{pane}",
            self.pane_command(side),
        )

    def set_dashboard_step(self, step: str) -> None:
        self.tmux("set-option", "-t", "dashboard", "status-left", f" {step} ")

    def drive_dashboard(
        self,
        steps: list[tuple[Path, str, Callable[[], bool]]],
        errors: list[BaseException],
    ) -> None:
        try:
            time.sleep(2.5)
            for step, label, condition in steps:
                self.set_dashboard_step(f"SOURCE COMMIT · {label}")
                self.run_step(step)
                self.refresh_pane("0", "source")
                time.sleep(1.5)
                self.wait(step.name, condition)
                self.set_dashboard_step(f"TARGET UPDATED · {label}")
                self.refresh_pane("1", "target")
                time.sleep(2.8)
            self.set_dashboard_step("COMPLETE · all target assertions passed")
            time.sleep(3)
        except BaseException as error:  # noqa: BLE001
            errors.append(error)
        finally:
            self.tmux("kill-session", "-t", "dashboard", check=False)

    def dashboard(
        self,
        source_title: str,
        target_title: str,
        steps: list[tuple[Path, str, Callable[[], bool]]],
    ) -> None:
        self.tmux(
            "new-session",
            "-d",
            "-s",
            "dashboard",
            "-x",
            "150",
            "-y",
            "40",
            self.pane_command("source"),
        )
        self.tmux(
            "split-window",
            "-h",
            "-t",
            "dashboard:0",
            self.pane_command("target"),
        )
        self.tmux("select-layout", "-t", "dashboard:0", "even-horizontal")
        self.tmux("set-option", "-t", "dashboard", "status-position", "top")
        self.tmux("set-option", "-t", "dashboard", "status-style", "bg=colour235,fg=colour250")
        self.tmux("set-option", "-t", "dashboard", "status-left-length", "110")
        self.tmux("set-option", "-t", "dashboard", "status-right", " DogPaddle · live PostgreSQL ")
        self.tmux("set-option", "-t", "dashboard", "pane-border-status", "top")
        self.tmux("set-option", "-t", "dashboard", "pane-border-format", " #{pane_title} ")
        self.tmux("select-pane", "-t", "dashboard:0.0", "-T", source_title)
        self.tmux("select-pane", "-t", "dashboard:0.1", "-T", target_title)
        self.set_dashboard_step("INITIAL SNAPSHOT · both panes are live queries")

        errors: list[BaseException] = []
        driver = threading.Thread(
            target=self.drive_dashboard,
            args=(steps, errors),
            daemon=True,
        )
        driver.start()
        attached = self.tmux("attach-session", "-t", "dashboard", check=False)
        driver.join(timeout=5)
        if driver.is_alive():
            raise RuntimeError("dashboard driver did not stop")
        if errors:
            raise errors[0]
        if attached.returncode not in {0, 1}:
            raise RuntimeError(f"tmux attach failed: {attached.stderr.strip()}")

    def run_asof(self) -> None:
        self.show_program("成交匹配当时最近的报价")
        self.start()
        self.wait(
            "trade_asof initial result",
            lambda: self.psql("SELECT count(*) FROM analytics.execution_quality") == "1",
        )
        self.wait(
            "trade_asof active connectors",
            lambda: self.psql(
                "SELECT count(*) FROM pg_replication_slots WHERE active"
            ) == "2",
        )
        steps = [
            ("01-backfill-closer-quote.sql", "backfill quote at 09:30:00.400", "09:30:00.400"),
            ("02-correct-quote.sql", "correct quote #2", "09:30:00.400"),
            ("03-retract-bad-quote.sql", "retract quote #2", "09:30:00.100"),
        ]
        dashboard_steps = [
            (
                self.example / "steps" / file,
                label,
                lambda expected=expected: self.psql(
                    "SELECT result.quoted_at = (extract(epoch FROM quote.quoted_at) * 1000000)::bigint "
                    "FROM analytics.execution_quality AS result JOIN market.quotes AS quote ON "
                    f"to_char(quote.quoted_at, 'HH24:MI:SS.MS') = '{expected}'"
                )
                == "t",
            )
            for file, label, expected in steps
        ]
        self.dashboard(
            "SOURCE · market.fills + market.quotes",
            "TARGET · analytics.execution_quality",
            dashboard_steps,
        )

    def run_sales(self) -> None:
        self.show_program("退款发生后修正门店销售汇总")
        self.start()
        self.wait(
            "store_sales initial result",
            lambda: self.psql("SELECT count(*) FROM analytics.store_sales") == "1",
        )
        self.wait(
            "store_sales active connector",
            lambda: self.psql(
                "SELECT count(*) FROM pg_replication_slots WHERE active"
            ) == "1",
        )
        steps = [
            ("01-pay-order.sql", "order #3003 paid", "SELECT count(*) FROM analytics.store_sales", "2"),
            (
                "02-correct-price.sql",
                "correct order #3002 amount",
                "SELECT revenue_cents FROM analytics.store_sales WHERE store_id = 11",
                "34000",
            ),
            ("03-refund-largest-order.sql", "refund store #12 order", "SELECT count(*) FROM analytics.store_sales", "1"),
            ("04-refund-last-store-order.sql", "refund remaining store #11 orders", "SELECT count(*) FROM analytics.store_sales", "0"),
        ]
        dashboard_steps = [
            (
                self.example / "steps" / file,
                label,
                lambda query=query, expected=expected: self.psql(query) == expected,
            )
            for file, label, query, expected in steps
        ]
        self.dashboard(
            "SOURCE · sales.store_orders",
            "TARGET · analytics.store_sales",
            dashboard_steps,
        )

    def run(self) -> None:
        try:
            if self.scenario == "trade-quote-asof":
                self.run_asof()
            elif self.scenario == "store-sales-summary":
                self.run_sales()
            else:
                raise ValueError(f"unknown scenario: {self.scenario}")
        finally:
            self.stop()


def record(args: argparse.Namespace) -> None:
    bundle = absolute_existing(args.bundle, "--bundle")
    postgres_bin = absolute_existing(args.postgres_bin, "--postgres-bin")
    binary = absolute_existing(args.dogpaddle, "--dogpaddle")
    require_executables(
        postgres_bin,
        ("createdb", "initdb", "pg_ctl", "postgres", "psql"),
    )
    for command in (args.asciinema, args.agg):
        if not Path(command).is_file() or not os.access(command, os.X_OK):
            raise ValueError(f"required executable is missing: {command}")

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dogpaddle-readme-terminal-") as directory, \
        tempfile.TemporaryDirectory(prefix=".readme-recording-", dir=output) as staging_directory:
        root = Path(directory)
        staging = Path(staging_directory)
        cluster = PostgresCluster(root, postgres_bin, socket_directory=Path("/tmp"))
        password = root / "password"
        try:
            password.write_text(PASSWORD + "\n", encoding="utf-8")
            password.chmod(0o600)
            try:
                cluster.start(
                    initdb_arguments=(
                        "-U", "dogpaddle", "--pwfile", str(password),
                        "--auth-local=trust", "--auth-host=scram-sha-256",
                        "--no-instructions", "--locale=C", "-E", "UTF8",
                    ),
                    server_options=(
                        "-c", "wal_level=logical",
                        "-c", "max_replication_slots=8",
                        "-c", "max_wal_senders=8",
                        "-c", "fsync=on",
                        "-c", "synchronous_commit=on",
                    ),
                )
            finally:
                password.unlink(missing_ok=True)
            scenarios = ("trade-quote-asof", "store-sales-summary")
            for scenario in scenarios:
                database = "trade_asof" if scenario == "trade-quote-asof" else "store_sales"
                environment = dict(os.environ, PGPASSWORD=PASSWORD)
                run(
                    [
                        str(postgres_bin / "createdb"), "-h", "127.0.0.1",
                        "-p", str(cluster.port), "-U", "dogpaddle", database,
                    ],
                    env=environment,
                )
                run(
                    [
                        str(postgres_bin / "psql"), "-X", "-h", "127.0.0.1",
                        "-p", str(cluster.port), "-U", "dogpaddle", "-d", database,
                        "-v", "ON_ERROR_STOP=1", "-f",
                        str(REPOSITORY / "examples" / scenario / "setup.sql"),
                    ],
                    env=environment,
                )
                cast = output / f"readme-{scenario}.cast"
                gif_name = "readme-trade-asof.gif" if scenario == "trade-quote-asof" else "readme-store-sales.gif"
                gif = output / gif_name
                staged_cast = staging / cast.name
                staged_gif = staging / gif.name
                session_command = shlex.join([
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--session", scenario,
                    "--port", str(cluster.port),
                    "--postgres-bin", str(postgres_bin),
                    "--bundle", str(bundle),
                    "--dogpaddle", str(binary),
                    "--state", str(root / scenario / "state"),
                    "--log", str(root / scenario / "dogpaddle.log"),
                ])
                subprocess.run(
                    [
                        str(args.asciinema), "record", "--overwrite", "--headless",
                        "--return", "--window-size", "120x35",
                        "--idle-time-limit", "3", "--command", session_command,
                        str(staged_cast),
                    ],
                    cwd=REPOSITORY,
                    check=True,
                )
                lines = staged_cast.read_text(encoding="utf-8").splitlines()
                header = json.loads(lines[0])
                header["command"] = (
                    "python3 docs/tools/record_readme_examples.py "
                    f"--session {scenario}"
                )
                staged_cast.write_text(
                    json.dumps(header, separators=(",", ":"))
                    + "\n"
                    + "\n".join(lines[1:])
                    + "\n",
                    encoding="utf-8",
                )
                subprocess.run(
                    [
                        str(args.agg), "--quiet", "--cols", "120", "--rows", "35",
                        "--font-size", "14", "--theme", "asciinema",
                        "--idle-time-limit", "3", "--last-frame-duration", "3",
                        str(staged_cast), str(staged_gif),
                    ],
                    check=True,
                )
                os.replace(staged_cast, cast)
                os.replace(staged_gif, gif)
                print(f"terminal recording: {cast}")
                print(f"README GIF: {gif}")
        finally:
            cluster.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--dogpaddle", type=Path, required=True)
    parser.add_argument("--postgres-bin", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--asciinema", type=Path)
    parser.add_argument("--agg", type=Path)
    parser.add_argument("--session", choices=("trade-quote-asof", "store-sales-summary"))
    parser.add_argument("--port", type=int)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.session is not None:
        if args.port is None or args.state is None or args.log is None or args.postgres_bin is None:
            parser.error("internal --session requires --port, --postgres-bin, --state and --log")
        Session(
            args.session,
            args.port,
            args.postgres_bin.resolve(strict=True),
            args.bundle.resolve(strict=True),
            args.dogpaddle.resolve(strict=True),
            args.state,
            args.log,
        ).run()
        return
    if args.postgres_bin is None or args.output_dir is None:
        parser.error("recording requires --postgres-bin and --output-dir")
    if args.asciinema is None or args.agg is None:
        parser.error("recording requires --asciinema and --agg")
    record(args)


if __name__ == "__main__":
    main()
