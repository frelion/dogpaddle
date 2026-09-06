#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["imageio-ffmpeg==0.6.0", "pillow==11.3.0"]
# ///
"""Render a silent, continuous source -> fixed ETL -> target observation.

Database rows are projected from verified snapshots. The renderer never evaluates
ETL expressions. Presentation delays are editorial, not measured CDC latency.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path
from typing import Any, Optional

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont

WIDTH, HEIGHT = 2560, 1440
BG, PANEL = "#10171f", "#151f2a"
WHITE, MUTED = "#e8eff6", "#9baebf"
AMBER, GREEN, RED, BLUE = "#ffd17b", "#8be0b1", "#ff9292", "#9fcaff"


def keyed(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    result = {row["order_id"]: row for row in rows}
    if len(result) != len(rows):
        raise ValueError("duplicate order IDs in a captured snapshot")
    return result


def delta(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> tuple[set[int], set[int], set[int]]:
    old, new = keyed(before), keyed(after)
    added, removed = new.keys() - old.keys(), old.keys() - new.keys()
    changed = {key for key in old.keys() & new.keys() if old[key] != new[key]}
    return added, changed, removed


def read_trace(path: Path) -> dict[str, Any]:
    trace = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(trace, dict) or trace.get("version") != 3 or trace.get("passed") is not True:
        raise ValueError("a successful version 3 PostgreSQL gate trace is required")
    if not isinstance(trace.get("program"), str) or not trace["program"].strip():
        raise ValueError("trace has no captured ETL program")
    updates = trace.get("updates")
    if not isinstance(updates, list) or len(updates) < 12:
        raise ValueError("at least 12 real continuous updates are required")
    source_fields = {"order_id", "region", "status", "quantity", "unit_price_cents", "discount_pct"}
    target_fields = {"order_id", "rid", "payable_cents", "handling_lane", "fulfillment_center"}
    previous = None
    for update in updates:
        if not isinstance(update.get("sql"), str) or not update["sql"].strip():
            raise ValueError("an update has no recorded SQL")
        for side in ("source_before", "source_after", "target_before", "target_after", "target_before_advance"):
            rows = update.get(side)
            required = source_fields if side.startswith("source") else target_fields
            if not isinstance(rows, list) or not all(isinstance(row, dict) and required <= row.keys() for row in rows):
                raise ValueError(f"invalid {side} snapshot")
            keyed(rows)
        if update["target_before_advance"] != update["target_before"]:
            raise ValueError("source-only phase must have its own unchanged target observation")
        if previous and (previous["source_after"] != update["source_before"] or previous["target_after"] != update["target_before"]):
            raise ValueError("continuous snapshots are not consecutive")
        if sum(map(len, delta(update["source_before"], update["source_after"]))) != 1:
            raise ValueError("each captured write must change exactly one source order")
        previous = update
    recorded_sql = [e["text"] for e in trace["events"] if e["kind"] == "sql"]
    if recorded_sql[-len(updates):] != [update["sql"] for update in updates]:
        raise ValueError("snapshot mutations differ from the captured SQL transcript")
    return trace


def find_font(supplied: Optional[Path], chinese: bool = False) -> Path:
    if supplied:
        return supplied.resolve(strict=True)
    candidates = (
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    ) if chinese else (
        "/System/Library/Fonts/Menlo.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationMono-Regular.ttf",
    )
    for path in candidates:
        if Path(path).is_file():
            return Path(path)
    raise ValueError("supply --caption-font for Chinese labels and --font for monospace code")


class View:
    def __init__(self, mono: Path, chinese: Path, program: str) -> None:
        self.mono = {size: ImageFont.truetype(str(mono), size) for size in (17, 18, 19, 20, 22, 26)}
        self.cn = {size: ImageFont.truetype(str(chinese), size) for size in (17, 18, 19, 20, 22, 26, 30)}
        self.program = program.splitlines()
        self.sql_size = next((size for size in (20, 19, 18, 17)
                              if len(self.program) * (size + 3) <= 1080
                              and all(self.mono[size].getlength(line) <= 1270 for line in self.program)), None)
        if self.sql_size is None:
            raise ValueError("the complete SQL file does not fit; enlarge the layout")

    def text(self, draw: ImageDraw.ImageDraw, x: int, y: int, value: str,
             size: int = 20, color: str = WHITE, code: bool = False,
             width: Optional[int] = None) -> None:
        font = self.mono[size] if code else self.cn[size]
        if font.getlength(value) > (WIDTH - x - 20 if width is None else width):
            raise ValueError(f"text exceeds its column: {value}")
        draw.text((x, y), value, font=font, fill=color)

    def sql_lines(self, value: str, width: int, size: int = 18) -> list[str]:
        result: list[str] = []
        for line in value.splitlines():
            result.extend(textwrap.wrap(
                line, width=int(width / self.mono[size].getlength("M")),
                replace_whitespace=False, break_long_words=False, break_on_hyphens=False,
            ) or [""])
        return result

    def logic(self, draw: ImageDraw.ImageDraw, active: set[str]) -> None:
        tokens = {
            "amount": ("quantity", "discount_pct", "subtotal_cents -"),
            "filter": ("WHERE status",),
            "priority": ("'priority'", "'standard'"),
            "route": ("WHERE region", "'CN-HUB'", "'GLOBAL-HUB'", "UNION ALL"),
            "reason": ("'export_review'", "handling_reason"),
        }
        size = self.sql_size
        assert size is not None
        for index, line in enumerate(self.program):
            y = 229 + index * (size + 3)
            highlighted = any(token in line for name in active for token in tokens[name])
            if highlighted:
                draw.rectangle((602, y - 1, 1940, y + size + 2), fill="#1b2b3a")
                draw.rectangle((602, y - 1, 605, y + size + 2), fill=BLUE)
            self.text(draw, 613, y, f"{index + 1:02}", 17, MUTED, True, 35)
            self.text(draw, 661, y, line, size, MUTED if line.startswith("--") else WHITE, True, 1270)

    def table(self, draw: ImageDraw.ImageDraw, x: int,
              rows: list[dict[str, Any]], before: list[dict[str, Any]],
              source: bool, ghosts: bool) -> None:
        fields = ("order_id", "status", "region", "quantity", "unit_price_cents", "discount_pct") if source else ("order_id", "payable_cents", "handling_lane", "fulfillment_center")
        labels = ("id", "status", "region", "qty", "price", "disc%") if source else ("id", "cents", "lane", "hub")
        offsets = (0, 68, 156, 278, 325, 432) if source else (0, 70, 180, 300)
        widths = (55, 85, 115, 40, 100, 70) if source else (60, 100, 110, 210)
        for offset, label, width in zip(offsets, labels, widths):
            self.text(draw, x + offset + 8, 240, label, 17, MUTED, True, width)
        draw.line((x, 277, x + 520, 277), fill="#394b5e")
        old, current = keyed(before), keyed(rows)
        visible = sorted(current.keys() | (old.keys() - current.keys() if ghosts else set()))
        if len(visible) > 6:
            raise ValueError("captured table exceeds the six-row display")
        for index, order in enumerate(visible):
            top = 296 + index * 72
            removed = order not in current
            row = old[order] if removed else current[order]
            changed = removed or old.get(order) != row
            tone = RED if removed else (AMBER if source else GREEN)
            if changed:
                draw.rectangle((x, top - 6, x + 520, top + 38), fill="#30252b" if removed else "#283028")
                draw.rectangle((x, top - 6, x + 4, top + 38), fill=tone)
            for offset, field, width in zip(offsets, fields, widths):
                value = str(row[field])
                tint = tone if removed or old.get(order, {}).get(field) != row[field] else WHITE
                self.text(draw, x + offset + 8, top, value, 22, tint, True, width)
            if removed:
                draw.line((x + 8, top + 15, x + 515, top + 15), fill=RED, width=2)
        if not visible:
            self.text(draw, x + 8, 311, "（当前没有结果行）", 22, MUTED)

    def diff_notes(self, draw: ImageDraw.ImageDraw, x: int,
                   before: list[dict[str, Any]], after: list[dict[str, Any]], source: bool) -> None:
        added, changed, removed = delta(before, after)
        old, new = keyed(before), keyed(after)
        self.text(draw, x, 935, "源表本轮变化" if source else "目标表本轮变化", 22, AMBER if source else GREEN)
        labels = {"status": "status", "quantity": "qty", "unit_price_cents": "price", "discount_pct": "disc%", "region": "region", "payable_cents": "cents", "handling_lane": "lane", "fulfillment_center": "hub", "handling_reason": "reason"}
        notes = [f"+ 新增订单 {order}" for order in sorted(added)]
        removal = "删除" if source else "撤回"
        notes += [f"- {removal}订单 {order}" for order in sorted(removed)]
        for order in sorted(changed):
            for field, label in labels.items():
                if field in old[order] and old[order][field] != new[order][field]:
                    previous = "NULL" if old[order][field] is None else str(old[order][field])
                    current = "NULL" if new[order][field] is None else str(new[order][field])
                    notes.append(f"{order} · {label}: {previous} → {current}")
        if not notes:
            notes = ["保持不变"]
        for index, line in enumerate(notes[:5]):
            self.text(draw, x, 981 + index * 35, line, 18, WHITE, width=520)

    def frame(self, update: dict[str, Any], index: int, total: int, phase: str) -> Image.Image:
        canvas = Image.new("RGB", (WIDTH, HEIGHT), BG)
        draw = ImageDraw.Draw(canvas)
        self.text(draw, 30, 24, "DogPaddle  ·  持续观察源表与目标表的变化", 30)
        self.text(draw, 2250, 28, "无声 · 真实 PostgreSQL 记录", 18, MUTED)
        self.text(draw, 30, 77, "黄色：源表改变    绿色：结果改变    红色划线：刚刚删除或撤回    金额单位：分", 18, MUTED)
        self.text(draw, 2270, 77, f"变更 {index:02} / {total:02}", 20, MUTED)
        draw.line((28, 122, 2532, 122), fill="#394b5e")
        self.text(draw, 30, 151, "源表  sales.orders", 26, AMBER)
        self.text(draw, 607, 151, "ETL  fulfillment.sql", 26, BLUE)
        self.text(draw, 1990, 151, "目标  ops.fulfillment_queue", 22, GREEN)
        draw.polygon(((558, 162), (576, 171), (558, 180)), fill=BLUE)
        draw.polygon(((1962, 162), (1980, 171), (1962, 180)), fill=GREEN)
        start = phase == "initial"
        source = update["source_before"] if start else update["source_after"]
        target = update["target_before_advance"] if phase == "source" else (update["target_before"] if start else update["target_after"])
        self.text(draw, 30, 197, f"{len(source)} 行 · 后端持续执行 INSERT / UPDATE / DELETE", 17, MUTED)
        self.text(draw, 607, 197, "完整 SQL 文件 · 端点、CTE、筛选和 UNION ALL 全程可见", 17, MUTED)
        self.text(draw, 1990, 197, f"{len(target)} 行 · DogPaddle 维护的派生结果", 17, MUTED)
        draw.line((582, 148, 582, 1300), fill="#394b5e")
        draw.line((1960, 148, 1960, 1300), fill="#394b5e")
        active: set[str] = set()
        sql = update["sql"]
        if not start:
            if "quantity" in sql or "unit_price_cents" in sql or "discount_pct" in sql:
                active |= {"amount", "filter", "priority", "reason"}
            if "status" in sql:
                active.add("filter")
            if "region" in sql:
                active |= {"route", "reason"}
            if sql.startswith(("INSERT", "DELETE")):
                active |= {"amount", "filter", "priority", "route", "reason"}
        self.logic(draw, active)
        self.table(draw, 30, source, source if start else update["source_before"], True, phase == "source")
        self.table(draw, 1990, target, target if phase in {"initial", "source"} else update["target_before"], False, phase == "result")
        if not start:
            self.diff_notes(draw, 30, update["source_before"], source, True)
            if phase == "source":
                self.text(draw, 1990, 935, "源表已写入，接着观察目标表", 22, MUTED)
            else:
                self.diff_notes(draw, 1990, update["target_before"], target, False)
        draw.rectangle((0, 1315, WIDTH, HEIGHT), fill=PANEL)
        self.text(draw, 30, 1329, "当前源表 SQL" if not start else "接下来持续写入源表；中间 ETL 逻辑保持不变", 18, AMBER)
        for line_index, line in enumerate(self.sql_lines(sql if not start else "", 2470, 22)):
            self.text(draw, 30, 1364 + line_index * 29, line, 22, WHITE, True, 2470)
        self.text(draw, 2010, 1330, "呈现间隔经编辑，不代表实际处理延迟", 17, MUTED)
        return canvas


def render(trace_path: Path, poster: Path, video: Path, transcript: Path,
           mono: Optional[Path], chinese: Optional[Path], interval: float) -> None:
    if not math.isfinite(interval) or not 4 <= interval <= 15:
        raise ValueError("--interval must be between 4 and 15 seconds")
    trace = read_trace(trace_path)
    updates = trace["updates"]
    view = View(find_font(mono), find_font(chinese, True), trace["program"])
    for destination in (poster, video, transcript):
        destination.parent.mkdir(parents=True, exist_ok=True)
    timeline: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="dogpaddle-continuous-") as root:
        staging = Path(root)
        frames: list[tuple[Path, float]] = []

        def frame(update: dict[str, Any], index: int, phase: str, duration: float) -> Path:
            path = staging / f"{len(frames):05}.png"
            view.frame(update, index, len(updates), phase).save(path)
            frames.append((path, duration))
            return path

        frame(updates[0], 0, "initial", 8)
        elapsed = 8.0
        poster_frame = None
        for index, update in enumerate(updates, 1):
            timeline.append({"start": elapsed, "sql": update["sql"]})
            frame(update, index, "source", 1.5)
            frame(update, index, "result", 1.5)
            last = frame(update, index, "hold", interval - 3)
            if index == 4:
                poster_frame = last
            elapsed += interval
        frame(updates[-1], len(updates), "hold", 8)
        elapsed += 8
        concat = staging / "frames.txt"
        concat.write_text("".join(f"file '{p.name}'\nduration {duration:.6f}\n" for p, duration in frames) + f"file '{frames[-1][0].name}'\n", encoding="utf-8")
        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        encoded = staging / "fulfillment-continuous.mp4"
        subprocess.run([
            ffmpeg, "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", str(concat),
            "-t", f"{elapsed:.6f}", "-an", "-vf", "fps=25", "-c:v", "libx264",
            "-preset", "medium", "-crf", "18", "-pix_fmt", "yuv420p", "-colorspace", "bt709",
            "-color_primaries", "bt709", "-color_trc", "bt709", "-movflags", "+faststart", str(encoded),
        ], check=True, timeout=300)
        subprocess.run([ffmpeg, "-v", "error", "-i", str(encoded), "-f", "null", "-"], check=True, timeout=120)
        if poster_frame is None:
            raise ValueError("no continuous result frame was rendered")
        for source, destination in ((encoded, video), (poster_frame, poster)):
            temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
            try:
                shutil.copyfile(source, temporary)
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
    with transcript.open("w", encoding="utf-8") as output:
        output.write("Verified PostgreSQL gate. Original timestamps; playback pacing is edited.\n\n")
        output.write(trace["program"] + "\n")
        for event in trace["events"]:
            output.write(f"\n[{event['at']:8.3f}s] {event['kind']}\n{event['text']}\n")
        output.write(f"\nPASS: recovery gate and {len(updates)} continuous updates against native PostgreSQL SQL.\n")
    transcript.with_name("fulfillment-timeline.json").write_text(json.dumps(timeline, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"video: {video} ({elapsed:.1f}s; {len(updates)} updates; no audio stream)")
    print(f"poster: {poster}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--poster", type=Path, required=True)
    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument("--transcript", type=Path, required=True)
    parser.add_argument("--font", type=Path)
    parser.add_argument("--caption-font", type=Path)
    parser.add_argument("--interval", type=float, default=6, help="presentation seconds per source write (4-15)")
    args = parser.parse_args()
    render(args.trace.resolve(strict=True), args.poster.resolve(), args.video.resolve(),
           args.transcript.resolve(), args.font, args.caption_font, args.interval)


if __name__ == "__main__":
    main()
