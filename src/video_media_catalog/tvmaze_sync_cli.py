"""CLI for immutable TVmaze show-index capture."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.tvmaze_sync import (
    DEFAULT_MAX_PAGE_BYTES,
    DEFAULT_MAX_PAGES,
    DEFAULT_RECORD_SHARD_BYTES,
    TVMazeHttpFetcher,
    capture_tvmaze_show_index,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-tvmaze-sync",
        description="Capture a complete immutable TVmaze show-index snapshot.",
    )
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("MEDIA_CATALOG_TVMAZE_USER_AGENT"),
    )
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--acquired-at")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--cursor")
    parser.add_argument("--watermark")
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.55)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument(
        "--max-page-bytes",
        type=int,
        default=DEFAULT_MAX_PAGE_BYTES,
    )
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    return parser


def run(parsed: argparse.Namespace) -> dict[str, object]:
    if not parsed.user_agent:
        raise ValueError("--user-agent or MEDIA_CATALOG_TVMAZE_USER_AGENT is required")
    acquired_at = parsed.acquired_at or (
        datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    config_values = {
        "connector": "tvmaze-show-index",
        "version": "1.0.0",
        "timeoutSeconds": parsed.timeout_seconds,
        "minimumIntervalSeconds": parsed.minimum_interval_seconds,
        "maxAttempts": parsed.max_attempts,
        "maxPages": parsed.max_pages,
        "maxPageBytes": parsed.max_page_bytes,
        "recordShardBytes": parsed.record_shard_bytes,
    }
    config_values.update(
        {
            key: value
            for key, value in {
                "windowStart": parsed.window_start,
                "windowEnd": parsed.window_end,
                "cursor": parsed.cursor,
                "watermark": parsed.watermark,
            }.items()
            if value is not None
        }
    )
    config_digest = sha256_digest(canonical_json(config_values))
    fetcher = TVMazeHttpFetcher(
        user_agent=parsed.user_agent,
        timeout_seconds=parsed.timeout_seconds,
        minimum_interval_seconds=parsed.minimum_interval_seconds,
        max_attempts=parsed.max_attempts,
        max_page_bytes=parsed.max_page_bytes,
    )
    store = (
        BoundedObjectStore(client=object())
        if urlsplit(parsed.destination_prefix).scheme == "file"
        else BoundedObjectStore(
            region=parsed.aws_region,
            endpoint_url=parsed.s3_endpoint,
            path_style_access=parsed.s3_path_style_access,
        )
    )
    result = capture_tvmaze_show_index(
        destination_prefix=parsed.destination_prefix,
        acquired_at=acquired_at,
        image_digest=parsed.image_digest,
        config_digest=config_digest,
        fetcher=fetcher,
        store=store,
        max_pages=parsed.max_pages,
        max_page_bytes=parsed.max_page_bytes,
        record_shard_bytes=parsed.record_shard_bytes,
        window_start=parsed.window_start,
        window_end=parsed.window_end,
        cursor=parsed.cursor,
        watermark=parsed.watermark,
    )
    return {
        "batchId": result.batch_manifest.batch_id,
        "batchManifest": result.batch_manifest_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "recordSetId": result.record_set_manifest.record_set_id,
        "recordSetManifest": result.record_set_manifest_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "pageCount": result.page_count,
        "recordCount": result.record_count,
    }


def main() -> None:
    print(canonical_json(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
