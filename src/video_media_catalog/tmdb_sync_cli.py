"""CLI for TMDB daily ID exports and replayable changes/detail capture."""

from __future__ import annotations

import argparse
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.connector_publish import DEFAULT_RECORD_SHARD_BYTES
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.official_http import OfficialHttpsDownloader
from video_media_catalog.tmdb import (
    TMDB_ENTITY_KINDS,
    TMDB_FILES_ORIGIN,
)
from video_media_catalog.tmdb_sync import (
    DEFAULT_MAX_API_BYTES,
    DEFAULT_MAX_CHANGE_PAGES,
    DEFAULT_MAX_CHANGED_IDS,
    DEFAULT_MAX_EXPORT_BYTES,
    TMDBHttpClient,
    capture_tmdb_changes,
    capture_tmdb_daily_exports,
    tmdb_export_filename,
)


def _common(parser: argparse.ArgumentParser) -> None:
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-tmdb-sync")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    daily = subparsers.add_parser(
        "daily-export",
        description="Capture official TMDB daily ID exports.",
    )
    _common(daily)
    daily.add_argument("--export-date", required=True)
    daily.add_argument("--timeout-seconds", type=float, default=120)
    daily.add_argument("--max-attempts", type=int, default=5)
    daily.add_argument(
        "--max-export-bytes",
        type=int,
        default=DEFAULT_MAX_EXPORT_BYTES,
    )

    changes = subparsers.add_parser(
        "changes",
        description="Capture TMDB changed IDs and current detail responses.",
    )
    _common(changes)
    changes.add_argument("--window-start", required=True)
    changes.add_argument("--window-end", required=True)
    changes.add_argument("--timeout-seconds", type=float, default=30)
    changes.add_argument("--minimum-interval-seconds", type=float, default=0.05)
    changes.add_argument("--max-attempts", type=int, default=5)
    changes.add_argument("--max-api-bytes", type=int, default=DEFAULT_MAX_API_BYTES)
    changes.add_argument(
        "--max-change-pages",
        type=int,
        default=DEFAULT_MAX_CHANGE_PAGES,
    )
    changes.add_argument(
        "--max-changed-ids",
        type=int,
        default=DEFAULT_MAX_CHANGED_IDS,
    )
    return parser


def _date(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO date") from exc


def _store(parsed: argparse.Namespace) -> BoundedObjectStore:
    scheme = urlsplit(parsed.destination_prefix).scheme
    if scheme == "file":
        return BoundedObjectStore(client=object())
    if scheme != "s3":
        raise ValueError("destination prefix must use file:// or s3://")
    return BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
    )


def run(parsed: argparse.Namespace) -> dict[str, object]:
    if not parsed.user_agent:
        raise ValueError("--user-agent or MEDIA_CATALOG_TMDB_USER_AGENT is required")
    if parsed.record_shard_bytes < 1:
        raise ValueError("record-shard-bytes must be positive")
    acquired_at = parsed.acquired_at or datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    store = _store(parsed)
    if parsed.mode == "daily-export":
        export_date = _date(parsed.export_date, label="export-date")
        config_digest = sha256_digest(
            canonical_json(
                {
                    "connector": "tmdb-daily-id-export",
                    "version": "1.0.0",
                    "exportDate": export_date.isoformat(),
                    "entityKinds": list(TMDB_ENTITY_KINDS),
                    "timeoutSeconds": parsed.timeout_seconds,
                    "maxAttempts": parsed.max_attempts,
                    "maxExportBytes": parsed.max_export_bytes,
                    "recordShardBytes": parsed.record_shard_bytes,
                }
            )
        )
        downloader = OfficialHttpsDownloader(
            allowed_host="files.tmdb.org",
            allowed_path_prefix="/p/exports/",
            user_agent=parsed.user_agent,
            timeout_seconds=parsed.timeout_seconds,
            max_attempts=parsed.max_attempts,
        )
        with tempfile.TemporaryDirectory(prefix="tmdb-daily-export-") as tmp:
            paths: dict[str, Path] = {}
            retries = 0
            rate_limits = 0
            for kind in TMDB_ENTITY_KINDS:
                filename = tmdb_export_filename(kind, export_date)
                downloaded = downloader.download(
                    f"{TMDB_FILES_ORIGIN}/p/exports/{filename}",
                    Path(tmp) / filename,
                    max_bytes=parsed.max_export_bytes,
                )
                paths[kind] = downloaded.path
                retries += downloaded.retry_count
                rate_limits += downloaded.rate_limit_count
            capture = capture_tmdb_daily_exports(
                export_paths=paths,
                export_date=export_date,
                destination_prefix=parsed.destination_prefix,
                acquired_at=acquired_at,
                image_digest=parsed.image_digest,
                config_digest=config_digest,
                store=store,
                retry_count=retries,
                rate_limit_count=rate_limits,
                max_export_bytes=parsed.max_export_bytes,
                record_shard_bytes=parsed.record_shard_bytes,
            )
    else:
        read_token = os.environ.get("MEDIA_CATALOG_TMDB_API_READ_TOKEN")
        if not read_token:
            raise ValueError("MEDIA_CATALOG_TMDB_API_READ_TOKEN is required")
        window_start = _date(parsed.window_start, label="window-start")
        window_end = _date(parsed.window_end, label="window-end")
        config_digest = sha256_digest(
            canonical_json(
                {
                    "connector": "tmdb-changes-detail",
                    "version": "1.0.0",
                    "windowStart": window_start.isoformat(),
                    "windowEnd": window_end.isoformat(),
                    "entityKinds": list(TMDB_ENTITY_KINDS),
                    "appendToResponse": {
                        "movie": "credits,external_ids,translations,images",
                        "tv": "credits,external_ids,translations,images",
                        "person": ("combined_credits,external_ids,translations,images"),
                    },
                    "timeoutSeconds": parsed.timeout_seconds,
                    "minimumIntervalSeconds": parsed.minimum_interval_seconds,
                    "maxAttempts": parsed.max_attempts,
                    "maxApiBytes": parsed.max_api_bytes,
                    "maxChangePages": parsed.max_change_pages,
                    "maxChangedIds": parsed.max_changed_ids,
                    "recordShardBytes": parsed.record_shard_bytes,
                }
            )
        )
        capture = capture_tmdb_changes(
            window_start=window_start,
            window_end=window_end,
            destination_prefix=parsed.destination_prefix,
            acquired_at=acquired_at,
            image_digest=parsed.image_digest,
            config_digest=config_digest,
            fetcher=TMDBHttpClient(
                read_token=read_token,
                user_agent=parsed.user_agent,
                timeout_seconds=parsed.timeout_seconds,
                minimum_interval_seconds=parsed.minimum_interval_seconds,
                max_attempts=parsed.max_attempts,
                max_response_bytes=parsed.max_api_bytes,
            ),
            store=store,
            max_change_pages=parsed.max_change_pages,
            max_changed_ids=parsed.max_changed_ids,
            max_api_bytes=parsed.max_api_bytes,
            record_shard_bytes=parsed.record_shard_bytes,
        )
    return {
        "batchId": capture.batch_manifest.batch_id,
        "batchManifest": capture.batch_manifest_object.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "recordSetId": capture.record_set_manifest.record_set_id,
        "recordSetManifest": capture.record_set_manifest_object.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "recordCount": capture.record_set_manifest.record_count,
        "retryCount": capture.batch_manifest.retry_count,
        "rateLimitCount": capture.batch_manifest.rate_limit_count,
    }


def main() -> None:
    print(canonical_json(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
