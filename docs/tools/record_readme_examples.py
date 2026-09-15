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
MUTED = "\033[90m"
RESET = "\033[0m"
TMUX_COMMAND_TIMEOUT_SECONDS = 10
DASHBOARD_TIMEOUT_SECONDS = 780
DASHBOARD_JOIN_TIMEOUT_SECONDS = 45
RECORDING_TIMEOUT_SECONDS = 1260


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
        self.tmux_binary = tmux
        self.log = log
        self.process: Optional[subprocess.Popen[str]] = None
        self.database = "trade_asof" if scenario == "trade-quote-asof" else "store_sales"
        self.example = REPOSITORY / "examples" / scenario
        self.work = state.parent
        self.work.mkdir(parents=True, exist_ok=True)
        (self.work / "pipeline.sql").symlink_to(self.example / "pipeline.sql")
        (self.work / "dogpaddle").symlink_to(binary)
        self.tmux_socket = self.work / "tmux.sock"
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
            TERM="xterm-256color",
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
            timeout=10,
        )
        return result.stdout.strip()

    def wait(
        self,
        description: str,
        condition: Callable[[], bool],
        cancelled: Optional[threading.Event] = None,
    ) -> None:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError(f"cancelled while waiting for {description}")
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
            timeout=30,
        )

    def tmux(
        self,
        *arguments: str,
        check: bool = True,
        interactive: bool = False,
        timeout: int = TMUX_COMMAND_TIMEOUT_SECONDS,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(self.tmux_binary),
                "-S",
                str(self.tmux_socket),
                "-f",
                "/dev/null",
                *arguments,
            ],
            env=self.psql_environment(),
            check=check,
            capture_output=not interactive,
            text=True,
            timeout=timeout,
        )

    def stop_tmux_server(self) -> None:
        try:
            self.tmux("kill-server", check=False)
        except (OSError, subprocess.TimeoutExpired):
            pass

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
        cancelled: threading.Event,
    ) -> None:
        try:
            if cancelled.wait(2.5):
                return
            for step, label, condition in steps:
                self.set_dashboard_step(f"SOURCE COMMIT · {label}")
                self.run_step(step)
                self.refresh_pane("0", "source")
                if cancelled.wait(1.5):
                    return
                self.wait(step.name, condition, cancelled)
                self.set_dashboard_step(f"TARGET UPDATED · {label}")
                self.refresh_pane("1", "target")
                if cancelled.wait(2.8):
                    return
            self.set_dashboard_step("COMPLETE · all target assertions passed")
            cancelled.wait(3)
        except BaseException as error:  # noqa: BLE001
            if not cancelled.is_set():
                errors.append(error)
        finally:
            try:
                self.tmux("kill-session", "-t", "dashboard", check=False)
            except (OSError, subprocess.TimeoutExpired) as error:
                if not cancelled.is_set():
                    errors.append(error)

    def dashboard(
        self,
        source_title: str,
        target_title: str,
        steps: list[tuple[Path, str, Callable[[], bool]]],
    ) -> None:
        errors: list[BaseException] = []
        cancelled = threading.Event()
        driver: Optional[threading.Thread] = None
        attached: Optional[subprocess.CompletedProcess[str]] = None
        body_error: Optional[BaseException] = None
        try:
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
            self.tmux(
                "set-option",
                "-t",
                "dashboard",
                "status-style",
                "bg=colour235,fg=colour250",
            )
            self.tmux("set-option", "-t", "dashboard", "status-left-length", "110")
            self.tmux(
                "set-option",
                "-t",
                "dashboard",
                "status-right",
                " DogPaddle · live PostgreSQL ",
            )
            self.tmux("set-window-option", "-t", "dashboard", "window-status-format", "")
            self.tmux(
                "set-window-option",
                "-t",
                "dashboard",
                "window-status-current-format",
                "",
            )
            self.tmux("set-option", "-t", "dashboard", "pane-border-status", "top")
            self.tmux(
                "set-option",
                "-t",
                "dashboard",
                "pane-border-format",
                " #{pane_title} ",
            )
            self.tmux("select-pane", "-t", "dashboard:0.0", "-T", source_title)
            self.tmux("select-pane", "-t", "dashboard:0.1", "-T", target_title)
            self.set_dashboard_step("INITIAL SNAPSHOT · both panes are live queries")
            driver = threading.Thread(
                target=self.drive_dashboard,
                args=(steps, errors, cancelled),
                daemon=True,
            )
            driver.start()
            attached = self.tmux(
                "attach-session",
                "-t",
                "dashboard",
                check=False,
                interactive=True,
                timeout=DASHBOARD_TIMEOUT_SECONDS,
            )
        except BaseException as error:  # noqa: BLE001
            body_error = error
        finally:
            cancelled.set()
            self.stop_tmux_server()
            if driver is not None:
                driver.join(timeout=DASHBOARD_JOIN_TIMEOUT_SECONDS)
        if body_error is not None:
            raise body_error
        if driver is not None and driver.is_alive():
            raise RuntimeError("dashboard driver did not stop")
        if errors:
            raise errors[0]
        if attached is None:
            raise RuntimeError("tmux attach did not run")
        if attached.returncode not in {0, 1}:
            raise RuntimeError(f"tmux attach failed: {(attached.stderr or '').strip()}")

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
            (
                "01-backfill-closer-quote.sql",
                "backfill quote at 09:30:00.400",
                "09:30:00.400",
                "10012",
                "6",
            ),
            (
                "02-correct-quote.sql",
                "correct quote #2",
                "09:30:00.400",
                "10014",
                "4",
            ),
            (
                "03-retract-bad-quote.sql",
                "retract quote #2",
                "09:30:00.100",
                "10010",
                "8",
            ),
        ]
        dashboard_steps = [
            (
                self.example / "steps" / file,
                label,
                lambda expected=expected, mid=mid, slip=slip: self.psql(
                    "SELECT to_char(timestamp 'epoch' + quoted_at * interval '1 microsecond', "
                    f"'HH24:MI:SS.MS') = '{expected}' AND arrival_mid_cents = {mid} "
                    f"AND slippage_cents = {slip} FROM analytics.execution_quality"
                )
                == "t",
            )
            for file, label, expected, mid, slip in steps
        ]
        self.dashboard(
            "SOURCE · market.fills + market.quotes",
            "TARGET · analytics.execution_quality",
            dashboard_steps,
        )

    def run_sales(self) -> None:
        self.show_program("退款发生后修正门店销售汇总")
        self.start()
        target_relation = (
            "SELECT store_id, order_count, revenue_cents, "
            "smallest_order_cents, largest_order_cents "
            "FROM analytics.store_sales ORDER BY store_id"
        )
        self.wait(
            "store_sales initial result",
            lambda: self.psql(target_relation) == "11|2|30000|12000|18000",
        )
        self.wait(
            "store_sales active connector",
            lambda: self.psql(
                "SELECT count(*) FROM pg_replication_slots WHERE active"
            ) == "1",
        )
        steps = [
            (
                "01-pay-order.sql",
                "order #3003 paid",
                "11|2|30000|12000|18000\n12|1|25000|25000|25000",
            ),
            (
                "02-correct-price.sql",
                "correct order #3002 amount",
                "11|2|34000|12000|22000\n12|1|25000|25000|25000",
            ),
            (
                "03-refund-largest-order.sql",
                "refund store #12 order",
                "11|2|34000|12000|22000",
            ),
            (
                "04-refund-last-store-order.sql",
                "refund remaining store #11 orders",
                "",
            ),
        ]
        dashboard_steps = [
            (
                self.example / "steps" / file,
                label,
                lambda expected=expected: self.psql(target_relation) == expected,
            )
            for file, label, expected in steps
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
            try:
                self.stop_tmux_server()
            finally:
                self.stop()


def stop_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        process.wait(timeout=10)


def stop_tmux_server(tmux: Path, socket: Path) -> None:
    try:
        subprocess.run(
            [str(tmux), "-S", str(socket), "kill-server"],
            check=False,
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass


def cast_output(path: Path) -> str:
    if not path.is_file():
        return ""
    try:
        events = path.read_text(encoding="utf-8").splitlines()[1:]
        return "".join(
            event[2]
            for line in events
            if (event := json.loads(line))[1] == "o"
        )
    except (OSError, IndexError, TypeError, json.JSONDecodeError):
        return ""


def prepare_cast(path: Path, scenario: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    header = json.loads(lines[0])
    events = [json.loads(line) for line in lines[1:]]
    complete = [
        index
        for index, event in enumerate(events)
        if event[1] == "o" and "COMPLETE ·" in event[2]
    ]
    if not complete:
        raise RuntimeError(f"terminal session did not complete for {scenario}")
    header["command"] = (
        "python3 docs/tools/record_readme_examples.py "
        f"--session {scenario}"
    )
    published_events = events[: complete[-1] + 1]
    published_events.append([0.0, "x", "0"])
    path.write_text(
        "\n".join(
            json.dumps(item, separators=(",", ":"))
            for item in (header, *published_events)
        )
        + "\n",
        encoding="utf-8",
    )


def record(args: argparse.Namespace) -> None:
    bundle = absolute_existing(args.bundle, "--bundle")
    postgres_bin = absolute_existing(args.postgres_bin, "--postgres-bin")
    binary = absolute_existing(args.dogpaddle, "--dogpaddle")
    if not os.access(binary, os.X_OK):
        raise ValueError(f"--dogpaddle is not executable: {binary}")
    require_executables(
        postgres_bin,
        ("createdb", "initdb", "pg_ctl", "postgres", "psql"),
    )
    for command in (args.asciinema, args.agg, args.tmux):
        if not Path(command).is_file() or not os.access(command, os.X_OK):
            raise ValueError(f"required executable is missing: {command}")

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dogpaddle-readme-terminal-", dir="/tmp"
    ) as directory, \
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
                    "--tmux", str(args.tmux),
                    "--state", str(root / scenario / "state"),
                    "--log", str(root / scenario / "dogpaddle.log"),
                ])
                recording = subprocess.Popen(
                    [
                        str(args.asciinema), "record", "--overwrite", "--headless",
                        "--return", "--window-size", "150x40",
                        "--idle-time-limit", "3", "--command", session_command,
                        str(staged_cast),
                    ],
                    cwd=REPOSITORY,
                    start_new_session=True,
                )
                try:
                    returncode = recording.wait(timeout=RECORDING_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired as error:
                    stop_process_group(recording)
                    raise RuntimeError(
                        f"terminal session timed out for {scenario}"
                    ) from error
                finally:
                    stop_tmux_server(
                        args.tmux,
                        root / scenario / "tmux.sock",
                    )
                if returncode != 0:
                    visible = cast_output(staged_cast)
                    detail = f"\n{visible[-20_000:]}" if visible else ""
                    raise RuntimeError(
                        f"terminal session failed for {scenario} with exit status "
                        f"{returncode}{detail}"
                    )
                prepare_cast(staged_cast, scenario)
                subprocess.run(
                    [
                        str(args.agg), "--quiet", "--cols", "150", "--rows", "40",
                        "--font-size", "13", "--theme", "asciinema",
                        "--idle-time-limit", "3", "--last-frame-duration", "3",
                        str(staged_cast), str(staged_gif),
                    ],
                    check=True,
                    timeout=120,
                )
                os.replace(staged_cast, cast)
                os.replace(staged_gif, gif)
                print(f"terminal recording: {cast}")
                print(f"README GIF: {gif}")
        finally:
            cluster.stop()


def show_pane(args: argparse.Namespace) -> None:
    database = "trade_asof" if args.pane == "trade-quote-asof" else "store_sales"
    environment = dict(
        os.environ,
        PGHOST="127.0.0.1",
        PGPORT=str(args.port),
        PGUSER="dogpaddle",
        PGDATABASE=database,
        PGPASSWORD=PASSWORD,
        PSQL_PAGER="cat",
    )
    psql = str(args.postgres_bin / "psql")
    if args.pane == "trade-quote-asof":
        if args.side == "source":
            sections = [
                (
                    "FILLS",
                    "SELECT trade_id AS trade, symbol, to_char(executed_at, "
                    "'HH24:MI:SS.MS') AS executed, side, price_cents AS price "
                    "FROM market.fills ORDER BY trade_id",
                ),
                (
                    "QUOTES",
                    "SELECT quote_id AS id, to_char(quoted_at, 'HH24:MI:SS.MS') "
                    "AS quoted_at, bid_cents AS bid, ask_cents AS ask "
                    "FROM market.quotes ORDER BY quote_id",
                ),
            ]
        else:
            sections = [
                (
                    "EXECUTION QUALITY",
                    "SELECT trade_id AS trade, to_char(timestamp 'epoch' + quoted_at * "
                    "interval '1 microsecond', 'HH24:MI:SS.MS') AS matched_quote, "
                    "execution_price_cents AS exec, arrival_mid_cents AS mid, "
                    "slippage_cents AS slip FROM analytics.execution_quality "
                    "ORDER BY trade_id",
                )
            ]
    elif args.side == "source":
        sections = [
            (
                "STORE ORDERS",
                "SELECT order_id AS id, store_id AS store, status, "
                "amount_cents AS amount FROM sales.store_orders ORDER BY order_id",
            )
        ]
    else:
        sections = [
            (
                "STORE SALES",
                "SELECT store_id AS store, order_count AS count, "
                "revenue_cents AS revenue, smallest_order_cents AS min, "
                "largest_order_cents AS max FROM analytics.store_sales "
                "ORDER BY store_id",
            )
        ]

    print("\033[2J\033[H", end="")
    for index, (title, query) in enumerate(sections):
        if index:
            print()
        print(f"{CYAN}{title}{RESET}")
        subprocess.run(
            [psql, "-X", "-P", "pager=off", "-c", query],
            env=environment,
            check=True,
            timeout=60,
        )
    sys.stdout.flush()
    while True:
        time.sleep(3600)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--dogpaddle", type=Path)
    parser.add_argument("--postgres-bin", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--asciinema", type=Path)
    parser.add_argument("--agg", type=Path)
    parser.add_argument("--tmux", type=Path)
    parser.add_argument("--session", choices=("trade-quote-asof", "store-sales-summary"))
    parser.add_argument("--pane", choices=("trade-quote-asof", "store-sales-summary"))
    parser.add_argument("--side", choices=("source", "target"))
    parser.add_argument("--port", type=int)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.pane is not None:
        if args.side is None or args.port is None or args.postgres_bin is None:
            parser.error("internal --pane requires --side, --port and --postgres-bin")
        show_pane(args)
        return
    if args.session is not None:
        if any(
            value is None
            for value in (
                args.port,
                args.state,
                args.log,
                args.postgres_bin,
                args.bundle,
                args.dogpaddle,
                args.tmux,
            )
        ):
            parser.error(
                "internal --session requires --port, --postgres-bin, --bundle, "
                "--dogpaddle, --tmux, --state and --log"
            )
        Session(
            args.session,
            args.port,
            args.postgres_bin.resolve(strict=True),
            args.bundle.resolve(strict=True),
            args.dogpaddle.resolve(strict=True),
            args.tmux.resolve(strict=True),
            args.state,
            args.log,
        ).run()
        return
    if any(
        value is None
        for value in (
            args.bundle,
            args.dogpaddle,
            args.postgres_bin,
            args.output_dir,
            args.asciinema,
            args.agg,
            args.tmux,
        )
    ):
        parser.error(
            "recording requires --bundle, --dogpaddle, --postgres-bin, "
            "--output-dir, --asciinema, --agg and --tmux"
        )
    record(args)


if __name__ == "__main__":
    main()
