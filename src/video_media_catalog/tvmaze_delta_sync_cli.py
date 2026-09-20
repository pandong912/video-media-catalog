"""CLI for immutable TVmaze update-index and show-detail deltas."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.tvmaze_sync import (
    DEFAULT_MAX_PAGE_BYTES,
    DEFAULT_MAX_UPDATES,
    DEFAULT_RECORD_SHARD_BYTES,
    TVMazeHttpFetcher,
    capture_tvmaze_show_delta,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-tvmaze-delta-sync",
        description="Capture TVmaze updated-show IDs and current show details.",
    )
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("MEDIA_CATALOG_TVMAZE_USER_AGENT"),
    )
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--acquired-at")
    parser.add_argument("--since", choices=("day", "week", "month"), default="day")
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.55)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument(
        "--max-page-bytes",
        type=int,
        default=DEFAULT_MAX_PAGE_BYTES,
    )
    parser.add_argument("--max-updates", type=int, default=DEFAULT_MAX_UPDATES)
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument("--s3-path-style-access", action="store_true")
    return parser


def run(parsed: argparse.Namespace) -> dict[str, object]:
    if not parsed.user_agent:
        raise ValueError("--user-agent or MEDIA_CATALOG_TVMAZE_USER_AGENT is required")
    acquired_at = parsed.acquired_at or datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    config_digest = sha256_digest(
        canonical_json(
            {
                "connector": "tvmaze-show-updates",
                "version": "1.0.0",
                "since": parsed.since,
                "timeoutSeconds": parsed.timeout_seconds,
                "minimumIntervalSeconds": parsed.minimum_interval_seconds,
                "maxAttempts": parsed.max_attempts,
                "maxPageBytes": parsed.max_page_bytes,
                "maxUpdates": parsed.max_updates,
                "recordShardBytes": parsed.record_shard_bytes,
            }
        )
    )
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
    capture = capture_tvmaze_show_delta(
        destination_prefix=parsed.destination_prefix,
        acquired_at=acquired_at,
        image_digest=parsed.image_digest,
        config_digest=config_digest,
        since=parsed.since,
        fetcher=fetcher,
        store=store,
        max_page_bytes=parsed.max_page_bytes,
        max_updates=parsed.max_updates,
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
