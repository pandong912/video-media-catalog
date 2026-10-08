from __future__ import annotations

import argparse

import pytest

from video_media_catalog.tmdb_capture_cli import build_parser, run
from video_media_catalog.tmdb_sync import TMDBChangeWindowsRequired

IMAGE_DIGEST = "sha256:" + ("a" * 64)
CURSORS = ("sha256:" + ("b" * 64), "sha256:" + ("c" * 64))


def _parsed(*extra: str) -> argparse.Namespace:
    return build_parser().parse_args(
        [
            "--mode",
            "bootstrap",
            "--export-date",
            "2026-10-07",
            "--window-start",
            "2026-09-24",
            "--window-end",
            "2026-10-07",
            "--destination-prefix",
            "s3://catalog/landing/research/capture",
            "--image-digest",
            IMAGE_DIGEST,
            "--acquired-at",
            "2026-10-08T01:00:00Z",
            "--user-agent",
            "video-media-catalog/test",
            *extra,
        ]
    )


def test_bootstrap_runs_inventory_and_every_bounded_change_shard() -> None:
    calls = []

    def runner(parsed):
        calls.append(parsed)
        if parsed.mode == "daily-export":
            return {"batchId": "inventory"}
        if parsed.window_cursor is None:
            raise TMDBChangeWindowsRequired(CURSORS)
        index = CURSORS.index(parsed.window_cursor)
        return {
            "batchId": f"changes-{index}",
            "windowCursor": parsed.window_cursor,
            "windowShardIndex": index,
            "windowShardCount": len(CURSORS),
        }

    result = run(_parsed(), runner=runner)

    assert result["captureCount"] == 3
    assert result["inventory"] == {"batchId": "inventory"}
    assert [item["batchId"] for item in result["changes"]] == [
        "changes-0",
        "changes-1",
    ]
    assert [item.mode for item in calls] == [
        "daily-export",
        "changes",
        "changes",
        "changes",
    ]
    assert calls[1].window_cursor is None
    assert [item.window_cursor for item in calls[2:]] == list(CURSORS)


def test_daily_mode_skips_inventory_and_commits_single_window() -> None:
    parsed = _parsed()
    parsed.mode = "daily"
    parsed.export_date = None

    def runner(arguments):
        assert arguments.mode == "changes"
        return {
            "batchId": "daily",
            "windowCursor": "sha256:" + ("d" * 64),
            "windowShardIndex": 0,
            "windowShardCount": 1,
        }

    result = run(parsed, runner=runner)

    assert result["inventory"] is None
    assert result["captureCount"] == 1
    assert result["changes"][0]["batchId"] == "daily"


def test_inventory_only_does_not_require_a_change_window() -> None:
    parsed = _parsed()
    parsed.mode = "inventory-only"
    parsed.window_start = None
    parsed.window_end = None

    result = run(
        parsed,
        runner=lambda arguments: {
            "batchId": arguments.export_date,
        },
    )

    assert result["captureCount"] == 1
    assert result["changes"] == []
    assert result["inventory"]["batchId"] == "2026-10-07"


def test_capture_rejects_excessive_change_shards() -> None:
    parsed = _parsed("--max-change-shards", "1")

    def runner(arguments):
        if arguments.mode == "daily-export":
            return {"batchId": "inventory"}
        raise TMDBChangeWindowsRequired(CURSORS)

    with pytest.raises(RuntimeError, match="exceeds max-change-shards"):
        run(parsed, runner=runner)


def test_capture_rejects_change_windows_over_fourteen_days() -> None:
    parsed = _parsed()
    parsed.window_start = "2026-09-23"

    with pytest.raises(ValueError, match="between 1 and 14 inclusive days"):
        run(parsed, runner=lambda _: {})
