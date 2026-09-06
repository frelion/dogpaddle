#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "imageio-ffmpeg==0.6.0",
#   "playwright==1.60.0",
# ]
# ///
"""Render the fulfillment hero and video from a real PostgreSQL trace."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import imageio_ffmpeg
from playwright.sync_api import Browser, Page, sync_playwright


WIDTH = 1440
HEIGHT = 810
VIDEO_SECONDS = 31
EXPECTED_SCENES = [
    "connected",
    "inserted",
    "paid",
    "repriced",
    "deleted",
    "crashed",
    "recovered",
    "resumed",
]
SOURCE_FIELDS = {
    "order_id",
    "customer",
    "region",
    "status",
    "quantity",
    "unit_price_cents",
    "discount_pct",
}
TARGET_FIELDS = {
    "rid",
    "order_id",
    "customer",
    "subtotal_cents",
    "payable_cents",
    "fulfillment_center",
    "handling_lane",
    "handling_reason",
}


def read_trace(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        trace = json.load(source)
    if not isinstance(trace, dict):
        raise ValueError("trace must be a JSON object")
    scenes = trace.get("scenes")
    if not isinstance(scenes, list):
        raise ValueError("trace must contain a scenes array")
    names = [scene.get("name") for scene in scenes if isinstance(scene, dict)]
    if names != EXPECTED_SCENES:
        raise ValueError(f"unexpected trace scenes: {names}")
    for scene in scenes:
        if not isinstance(scene.get("source"), list) or not isinstance(
            scene.get("target"), list
        ):
            raise ValueError(f"scene {scene.get('name')} has invalid rows")
        for side, required in (("source", SOURCE_FIELDS), ("target", TARGET_FIELDS)):
            for row in scene[side]:
                if not isinstance(row, dict) or not required.issubset(row):
                    raise ValueError(
                        f"scene {scene.get('name')} has an invalid {side} row"
                    )
    return trace


def inject_trace(page: Page, trace: dict[str, Any]) -> None:
    encoded = json.dumps(trace, separators=(",", ":"), ensure_ascii=True)
    page.add_init_script(f"window.TRACE = {encoded};")


def open_demo(page: Page, html: Path, query: str = "") -> list[str]:
    page_errors: list[str] = []
    page.on("pageerror", lambda error: page_errors.append(str(error)))
    page.goto(f"{html.as_uri()}{query}", wait_until="load")
    page.wait_for_function(
        "document.documentElement.dataset.demoState === 'ready' || "
        "document.documentElement.dataset.demoState === 'error'",
        timeout=5_000,
    )
    if page_errors:
        raise RuntimeError(f"demo page error: {page_errors[0]}")
    if page.locator("html").get_attribute("data-demo-state") != "ready":
        detail = page.locator("#traceErrorDetail").inner_text()
        raise RuntimeError(f"demo rejected the trace: {detail}")
    page.evaluate("document.fonts.ready")
    return page_errors


def render_poster(
    browser: Browser, html: Path, trace: dict[str, Any], destination: Path
) -> None:
    page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})
    inject_trace(page, trace)
    page_errors = open_demo(page, html, "?poster=1")
    page.screenshot(path=str(destination), type="png")
    if page_errors:
        raise RuntimeError(f"demo page error: {page_errors[0]}")
    page.close()


def record_webm(
    browser: Browser,
    html: Path,
    trace: dict[str, Any],
    recording_directory: Path,
    destination: Path,
) -> None:
    context = browser.new_context(
        viewport={"width": WIDTH, "height": HEIGHT},
        device_scale_factor=1,
        record_video_dir=str(recording_directory),
        record_video_size={"width": WIDTH, "height": HEIGHT},
    )
    page = context.new_page()
    inject_trace(page, trace)
    page_errors = open_demo(page, html)
    page.wait_for_timeout((VIDEO_SECONDS + 0.5) * 1000)
    if page_errors:
        raise RuntimeError(f"demo page error: {page_errors[0]}")
    video = page.video
    if video is None:
        raise RuntimeError("Playwright did not start video capture")
    context.close()
    video.save_as(str(destination))


def encode_mp4(source: Path, destination: Path) -> None:
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    subprocess.run(
        [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(source),
            "-t",
            str(VIDEO_SECONDS),
            "-an",
            "-vf",
            (
                "setparams=range=limited:color_primaries=bt709:"
                "color_trc=bt709:colorspace=bt709"
            ),
            "-c:v",
            "libx264",
            "-preset",
            "slow",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "high",
            "-level",
            "4.1",
            "-colorspace",
            "bt709",
            "-color_primaries",
            "bt709",
            "-color_trc",
            "bt709",
            "-color_range",
            "tv",
            "-movflags",
            "+faststart",
            str(destination),
        ],
        check=True,
    )
    subprocess.run(
        [ffmpeg, "-v", "error", "-i", str(destination), "-f", "null", "-"],
        check=True,
    )


def render(trace_path: Path, html: Path, poster: Path, video: Path) -> None:
    trace = read_trace(trace_path.resolve(strict=True))
    html = html.resolve(strict=True)
    poster = poster.resolve()
    video = video.resolve()
    poster.parent.mkdir(parents=True, exist_ok=True)
    video.parent.mkdir(parents=True, exist_ok=True)

    poster_file = tempfile.NamedTemporaryFile(
        prefix=f".{poster.name}.", suffix=".png", dir=poster.parent, delete=False
    )
    video_file = tempfile.NamedTemporaryFile(
        prefix=f".{video.name}.", suffix=".mp4", dir=video.parent, delete=False
    )
    poster_file.close()
    video_file.close()
    staged_poster = Path(poster_file.name)
    staged_video = Path(video_file.name)
    try:
        with tempfile.TemporaryDirectory(
            prefix="dogpaddle-fulfillment-render-"
        ) as root:
            staging = Path(root)
            staged_webm = staging / "fulfillment-demo.webm"
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    render_poster(browser, html, trace, staged_poster)
                    record_webm(
                        browser,
                        html,
                        trace,
                        staging / "recording",
                        staged_webm,
                    )
                finally:
                    browser.close()
            encode_mp4(staged_webm, staged_video)
        os.replace(staged_video, video)
        os.replace(staged_poster, poster)
        video.chmod(0o644)
        poster.chmod(0o644)
    finally:
        staged_poster.unlink(missing_ok=True)
        staged_video.unlink(missing_ok=True)

    print(f"poster: {poster}")
    print(f"video: {video}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--poster", type=Path, required=True)
    parser.add_argument("--video", type=Path, required=True)
    args = parser.parse_args()
    render(args.trace, args.html, args.poster, args.video)


if __name__ == "__main__":
    main()
