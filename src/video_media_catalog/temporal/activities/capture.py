"""Capture activities wrapping TMDB sync with Temporal heartbeats."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from temporalio import activity

from video_media_catalog.connector_publish import DEFAULT_RECORD_SHARD_BYTES
from video_media_catalog.temporal.errors import (
    NonRetryableCaptureError,
    classify_capture_exception,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    CaptureUnit,
    ObjectRefPayload,
    PipelineEnv,
)
from video_media_catalog.tmdb_capture_cli import (
    DEFAULT_MAX_CHANGE_SHARDS,
    _run_all_change_shards,
)
from video_media_catalog.tmdb_sync import (
    DEFAULT_MAX_API_BYTES,
    DEFAULT_MAX_CHANGE_PAGES,
    DEFAULT_MAX_CHANGED_IDS,
    DEFAULT_MAX_EXPORT_BYTES,
)
from video_media_catalog.tmdb_sync_cli import run as run_sync


def _now_rfc3339() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _heartbeat(details: dict[str, Any]) -> None:
    if activity.in_activity():
        activity.heartbeat(details)


class _HeartbeatingSyncRunner:
    def __init__(self) -> None:
        self._calls = 0

    def __call__(self, parsed: argparse.Namespace) -> dict[str, object]:
        self._calls += 1
        _heartbeat(
            {
                "phase": "capture-sync",
                "call": self._calls,
                "mode": getattr(parsed, "mode", None),
                "windowCursor": getattr(parsed, "window_cursor", None),
            }
        )
        return run_sync(parsed)


def _base_namespace(env: PipelineEnv, *, acquired_at: str) -> dict[str, Any]:
    return {
        "destination_prefix": env.destination_prefix,
        "image_digest": env.image_digest,
        "acquired_at": acquired_at,
        "user_agent": env.user_agent,
        "record_shard_bytes": DEFAULT_RECORD_SHARD_BYTES,
        "aws_region": env.aws_region,
        "s3_endpoint": env.s3_endpoint,
        "s3_path_style_access": env.s3_path_style_access,
    }


def _inventory_namespace(
    env: PipelineEnv, *, acquired_at: str, export_date: str
) -> argparse.Namespace:
    return argparse.Namespace(
        mode="daily-export",
        export_date=export_date,
        timeout_seconds=120.0,
        max_attempts=5,
        max_export_bytes=DEFAULT_MAX_EXPORT_BYTES,
        **_base_namespace(env, acquired_at=acquired_at),
    )


def _to_capture_result(
    *,
    kind: str,
    payload: dict[str, object],
    export_date: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> CaptureResult:
    batch = ObjectRefPayload.from_mapping(payload["batchManifest"])  # type: ignore[arg-type]
    record_set = ObjectRefPayload.from_mapping(
        payload["recordSetManifest"]  # type: ignore[arg-type]
    )
    return CaptureResult(
        kind=kind,  # type: ignore[arg-type]
        batch_id=str(payload["batchId"]),
        record_set_id=str(payload["recordSetId"]),
        record_count=int(payload["recordCount"]),
        batch_manifest=batch,
        record_set_manifest=record_set,
        export_date=export_date,
        window_start=window_start,
        window_end=window_end,
        window_cursor=(
            str(payload["windowCursor"])
            if payload.get("windowCursor") is not None
            else None
        ),
        window_shard_index=(
            int(payload["windowShardIndex"])
            if payload.get("windowShardIndex") is not None
            else None
        ),
        window_shard_count=(
            int(payload["windowShardCount"])
            if payload.get("windowShardCount") is not None
            else None
        ),
        retry_count=int(payload.get("retryCount") or 0),
        rate_limit_count=int(payload.get("rateLimitCount") or 0),
    )


@activity.defn(name="ValidateTmdbToken")
def validate_tmdb_token(user_agent: str) -> bool:
    token = os.environ.get("MEDIA_CATALOG_TMDB_API_READ_TOKEN", "").strip()
    if not token:
        raise NonRetryableCaptureError(
            "MEDIA_CATALOG_TMDB_API_READ_TOKEN is not configured"
        )

    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, request, fp, code, message, headers, url):
            return None

    request = Request(
        "https://api.themoviedb.org/3/authentication",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": user_agent,
        },
    )
    try:
        with build_opener(NoRedirect).open(request, timeout=30) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise classify_capture_exception(exc) from exc
    if payload.get("success") is not True:
        raise NonRetryableCaptureError(
            "TMDB rejected the configured API Read Access Token"
        )
    _heartbeat({"phase": "token-validated"})
    return True


@activity.defn(name="CaptureTmdbDay")
def capture_tmdb_day(
    unit: CaptureUnit,
    env: PipelineEnv,
    acquired_at: str | None = None,
) -> list[CaptureResult]:
    """Capture inventory or one natural-day changes window (all shards)."""
    acquired = acquired_at or _now_rfc3339()
    runner = _HeartbeatingSyncRunner()
    try:
        if unit.kind == "inventory":
            if not unit.export_date:
                raise NonRetryableCaptureError("inventory capture requires export_date")
            _heartbeat({"phase": "inventory", "exportDate": unit.export_date})
            payload = runner(
                _inventory_namespace(
                    env,
                    acquired_at=acquired,
                    export_date=unit.export_date,
                )
            )
            return [
                _to_capture_result(
                    kind="inventory",
                    payload=payload,
                    export_date=unit.export_date,
                )
            ]

        if unit.kind != "changes" or not unit.window_start or not unit.window_end:
            raise NonRetryableCaptureError("changes day requires window bounds")

        parsed = argparse.Namespace(
            mode="daily",
            export_date=None,
            window_start=unit.window_start,
            window_end=unit.window_end,
            watermark=None,
            max_change_shards=DEFAULT_MAX_CHANGE_SHARDS,
            destination_prefix=env.destination_prefix,
            image_digest=env.image_digest,
            acquired_at=acquired,
            user_agent=env.user_agent,
            record_shard_bytes=DEFAULT_RECORD_SHARD_BYTES,
            aws_region=env.aws_region,
            s3_endpoint=env.s3_endpoint,
            s3_path_style_access=env.s3_path_style_access,
            daily_timeout_seconds=120.0,
            daily_max_attempts=5,
            max_export_bytes=DEFAULT_MAX_EXPORT_BYTES,
            changes_timeout_seconds=30.0,
            minimum_interval_seconds=0.05,
            changes_max_attempts=5,
            max_api_bytes=DEFAULT_MAX_API_BYTES,
            max_change_pages=DEFAULT_MAX_CHANGE_PAGES,
            max_changed_ids=DEFAULT_MAX_CHANGED_IDS,
        )
        results = _run_all_change_shards(
            parsed,
            acquired_at=acquired,
            window_start=date.fromisoformat(unit.window_start),
            window_end=date.fromisoformat(unit.window_end),
            runner=runner,
        )
        if not results:
            raise NonRetryableCaptureError("TMDB changes capture produced no shards")
        return [
            _to_capture_result(
                kind="changes",
                payload=item,
                window_start=unit.window_start,
                window_end=unit.window_end,
            )
            for item in results
        ]
    except Exception as exc:
        raise classify_capture_exception(exc) from exc
