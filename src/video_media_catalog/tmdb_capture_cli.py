"""Automation wrapper for bounded TMDB baseline and delta captures."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.connector_publish import DEFAULT_RECORD_SHARD_BYTES
from video_media_catalog.tmdb_sync import (
    DEFAULT_MAX_API_BYTES,
    DEFAULT_MAX_CHANGE_PAGES,
    DEFAULT_MAX_CHANGED_IDS,
    DEFAULT_MAX_EXPORT_BYTES,
    TMDBChangeWindowsRequired,
)
from video_media_catalog.tmdb_sync_cli import run as run_sync

DEFAULT_MAX_CHANGE_SHARDS = 64
CaptureRunner = Callable[[argparse.Namespace], dict[str, object]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-tmdb-capture",
        description=(
            "Publish a TMDB ID baseline and/or bounded changes/detail captures."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("bootstrap", "daily", "inventory-only"),
        required=True,
    )
    parser.add_argument("--export-date")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--watermark")
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--acquired-at")
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("MEDIA_CATALOG_TMDB_USER_AGENT"),
    )
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument("--daily-timeout-seconds", type=float, default=120)
    parser.add_argument("--daily-max-attempts", type=int, default=5)
    parser.add_argument(
        "--max-export-bytes",
        type=int,
        default=DEFAULT_MAX_EXPORT_BYTES,
    )
    parser.add_argument("--changes-timeout-seconds", type=float, default=30)
    parser.add_argument(
        "--minimum-interval-seconds",
        type=float,
        default=0.05,
    )
    parser.add_argument("--changes-max-attempts", type=int, default=5)
    parser.add_argument(
        "--max-api-bytes",
        type=int,
        default=DEFAULT_MAX_API_BYTES,
    )
    parser.add_argument(
        "--max-change-pages",
        type=int,
        default=DEFAULT_MAX_CHANGE_PAGES,
    )
    parser.add_argument(
        "--max-changed-ids",
        type=int,
        default=DEFAULT_MAX_CHANGED_IDS,
    )
    parser.add_argument(
        "--max-change-shards",
        type=int,
        default=DEFAULT_MAX_CHANGE_SHARDS,
    )
    return parser


def _iso_date(value: str | None, *, label: str) -> date:
    if value is None:
        raise ValueError(f"{label} is required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO date") from exc


def _acquired_at(value: str | None) -> str:
    return value or datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _common(parsed: argparse.Namespace, acquired_at: str) -> dict[str, Any]:
    return {
        "destination_prefix": parsed.destination_prefix,
        "image_digest": parsed.image_digest,
        "acquired_at": acquired_at,
        "user_agent": parsed.user_agent,
        "record_shard_bytes": parsed.record_shard_bytes,
        "aws_region": parsed.aws_region,
        "s3_endpoint": parsed.s3_endpoint,
        "s3_path_style_access": parsed.s3_path_style_access,
    }


def _inventory_args(
    parsed: argparse.Namespace,
    *,
    acquired_at: str,
    export_date: date,
) -> argparse.Namespace:
    return argparse.Namespace(
        mode="daily-export",
        export_date=export_date.isoformat(),
        timeout_seconds=parsed.daily_timeout_seconds,
        max_attempts=parsed.daily_max_attempts,
        max_export_bytes=parsed.max_export_bytes,
        **_common(parsed, acquired_at),
    )


def _changes_args(
    parsed: argparse.Namespace,
    *,
    acquired_at: str,
    window_start: date,
    window_end: date,
    window_cursor: str | None,
) -> argparse.Namespace:
    return argparse.Namespace(
        mode="changes",
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        window_cursor=window_cursor,
        watermark=parsed.watermark,
        timeout_seconds=parsed.changes_timeout_seconds,
        minimum_interval_seconds=parsed.minimum_interval_seconds,
        max_attempts=parsed.changes_max_attempts,
        max_api_bytes=parsed.max_api_bytes,
        max_change_pages=parsed.max_change_pages,
        max_changed_ids=parsed.max_changed_ids,
        **_common(parsed, acquired_at),
    )


def _run_all_change_shards(
    parsed: argparse.Namespace,
    *,
    acquired_at: str,
    window_start: date,
    window_end: date,
    runner: CaptureRunner,
) -> list[dict[str, object]]:
    initial = _changes_args(
        parsed,
        acquired_at=acquired_at,
        window_start=window_start,
        window_end=window_end,
        window_cursor=None,
    )
    try:
        return [runner(initial)]
    except TMDBChangeWindowsRequired as exc:
        cursors = exc.cursors
    if len(cursors) > parsed.max_change_shards:
        raise RuntimeError(
            "TMDB change plan exceeds max-change-shards: "
            f"{len(cursors)} > {parsed.max_change_shards}"
        )
    results = []
    for cursor in cursors:
        result = runner(
            _changes_args(
                parsed,
                acquired_at=acquired_at,
                window_start=window_start,
                window_end=window_end,
                window_cursor=cursor,
            )
        )
        if result.get("windowCursor") != cursor:
            raise RuntimeError("TMDB change capture returned an unexpected cursor")
        results.append(result)
    shard_counts = {item.get("windowShardCount") for item in results}
    shard_indexes = {item.get("windowShardIndex") for item in results}
    if shard_counts != {len(cursors)} or shard_indexes != set(range(len(cursors))):
        raise RuntimeError("TMDB change captures do not cover the complete window plan")
    return results


def run(
    parsed: argparse.Namespace,
    *,
    runner: CaptureRunner = run_sync,
) -> dict[str, object]:
    if parsed.record_shard_bytes < 1:
        raise ValueError("record-shard-bytes must be positive")
    if parsed.max_change_shards < 1:
        raise ValueError("max-change-shards must be positive")
    acquired_at = _acquired_at(parsed.acquired_at)

    inventory = None
    export_date = None
    if parsed.mode in {"bootstrap", "inventory-only"}:
        export_date = _iso_date(parsed.export_date, label="export-date")
        inventory = runner(
            _inventory_args(
                parsed,
                acquired_at=acquired_at,
                export_date=export_date,
            )
        )

    changes: list[dict[str, object]] = []
    window_start = None
    window_end = None
    if parsed.mode in {"bootstrap", "daily"}:
        window_start = _iso_date(parsed.window_start, label="window-start")
        window_end = _iso_date(parsed.window_end, label="window-end")
        if window_end < window_start or window_end - window_start > timedelta(days=13):
            raise ValueError(
                "TMDB change window must be between 1 and 14 inclusive days"
            )
        capture_date = window_start
        while capture_date <= window_end:
            changes.extend(
                _run_all_change_shards(
                    parsed,
                    acquired_at=acquired_at,
                    window_start=capture_date,
                    window_end=capture_date,
                    runner=runner,
                )
            )
            capture_date += timedelta(days=1)

    return {
        "schemaVersion": "1.0",
        "mode": parsed.mode,
        "acquiredAt": acquired_at,
        "exportDate": export_date.isoformat() if export_date else None,
        "windowStart": window_start.isoformat() if window_start else None,
        "windowEnd": window_end.isoformat() if window_end else None,
        "inventory": inventory,
        "changes": changes,
        "captureCount": (1 if inventory is not None else 0) + len(changes),
    }


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
