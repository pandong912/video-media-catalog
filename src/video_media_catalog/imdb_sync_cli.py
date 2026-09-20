"""CLI for official IMDb TSV acquisition; no IMDb web pages are accessed."""

from __future__ import annotations

import argparse
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.connector_publish import DEFAULT_RECORD_SHARD_BYTES
from video_media_catalog.imdb import IMDB_DATASET_FILES, IMDB_DATASET_ORIGIN
from video_media_catalog.imdb_sync import (
    DEFAULT_MAX_DATASET_BYTES,
    capture_imdb_snapshot,
)
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.official_http import OfficialHttpsDownloader


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-imdb-sync",
        description="Capture the seven official IMDb non-commercial TSV files.",
    )
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("MEDIA_CATALOG_IMDB_USER_AGENT"),
    )
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--acquired-at")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--cursor")
    parser.add_argument("--watermark")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument(
        "--max-dataset-bytes",
        type=int,
        default=DEFAULT_MAX_DATASET_BYTES,
    )
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
        raise ValueError("--user-agent or MEDIA_CATALOG_IMDB_USER_AGENT is required")
    if parsed.max_dataset_bytes < 1 or parsed.record_shard_bytes < 1:
        raise ValueError("dataset and shard byte limits must be positive")
    acquired_at = parsed.acquired_at or datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    config_values = {
        "connector": "imdb-official-tsv",
        "version": "1.0.0",
        "origin": IMDB_DATASET_ORIGIN,
        "datasets": list(IMDB_DATASET_FILES),
        "timeoutSeconds": parsed.timeout_seconds,
        "maxAttempts": parsed.max_attempts,
        "maxDatasetBytes": parsed.max_dataset_bytes,
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
    downloader = OfficialHttpsDownloader(
        allowed_host="datasets.imdbws.com",
        allowed_path_prefix="/",
        user_agent=parsed.user_agent,
        timeout_seconds=parsed.timeout_seconds,
        max_attempts=parsed.max_attempts,
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
    with tempfile.TemporaryDirectory(prefix="imdb-official-tsv-") as tmp:
        root = Path(tmp)
        paths: dict[str, Path] = {}
        retries = 0
        rate_limits = 0
        for dataset in IMDB_DATASET_FILES:
            result = downloader.download(
                f"{IMDB_DATASET_ORIGIN}/{dataset}",
                root / dataset,
                max_bytes=parsed.max_dataset_bytes,
            )
            paths[dataset] = result.path
            retries += result.retry_count
            rate_limits += result.rate_limit_count
        capture = capture_imdb_snapshot(
            dataset_paths=paths,
            destination_prefix=parsed.destination_prefix,
            acquired_at=acquired_at,
            image_digest=parsed.image_digest,
            config_digest=config_digest,
            store=store,
            retry_count=retries,
            rate_limit_count=rate_limits,
            max_dataset_bytes=parsed.max_dataset_bytes,
            record_shard_bytes=parsed.record_shard_bytes,
            window_start=parsed.window_start,
            window_end=parsed.window_end,
            cursor=parsed.cursor,
            watermark=parsed.watermark,
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
