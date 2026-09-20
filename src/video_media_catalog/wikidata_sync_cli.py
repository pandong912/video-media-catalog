"""CLI for immutable synchronization of official dated Wikidata dumps."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Sequence
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.wikidata_sync import (
    DEFAULT_COPY_PART_BYTES,
    DEFAULT_MAX_DUMP_BYTES,
    DEFAULT_RANGE_ATTEMPTS,
    DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    DEFAULT_UPLOAD_PART_BYTES,
    HttpTransport,
    StdlibHttpTransport,
    WikidataSyncResult,
    sync_official_dump,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-wikidata-sync",
        description=(
            "Range-download one official dated Wikidata JSON bzip2 dump into an "
            "immutable, versioned S3 object."
        ),
    )
    parser.add_argument(
        "--source-url",
        required=True,
        help=(
            "canonical https://dumps.wikimedia.org/wikidatawiki/entities/"
            "YYYYMMDD/wikidata-YYYYMMDD-all.json.bz2 URL"
        ),
    )
    parser.add_argument(
        "--destination-prefix",
        required=True,
        help="S3 prefix under which the content-addressed object is published",
    )
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--cursor")
    parser.add_argument("--watermark")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_DUMP_BYTES,
        help="hard response size ceiling (default: 200 GiB)",
    )
    parser.add_argument(
        "--upload-part-bytes",
        type=int,
        default=DEFAULT_UPLOAD_PART_BYTES,
        help="streaming multipart upload part size (default: 64 MiB)",
    )
    parser.add_argument(
        "--copy-part-bytes",
        type=int,
        default=DEFAULT_COPY_PART_BYTES,
        help="server-side immutable publication part size (default: 512 MiB)",
    )
    parser.add_argument("--max-redirects", type=int, default=3)
    parser.add_argument("--http-timeout-seconds", type=float, default=60.0)
    parser.add_argument(
        "--range-attempts",
        type=int,
        default=DEFAULT_RANGE_ATTEMPTS,
        help="attempts per byte range (default: 5)",
    )
    parser.add_argument(
        "--retry-initial-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
        help="initial byte-range retry delay (default: 1 second)",
    )
    parser.add_argument(
        "--retry-max-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
        help="maximum byte-range retry delay (default: 30 seconds)",
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    return parser


def _s3_client(parsed: argparse.Namespace) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={
                "addressing_style": (
                    "path" if parsed.s3_path_style_access else "virtual"
                )
            },
        ),
    )


def run(
    parsed: argparse.Namespace,
    *,
    s3: Any | None = None,
    http: HttpTransport | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> WikidataSyncResult:
    return sync_official_dump(
        source_url=parsed.source_url,
        destination_prefix=parsed.destination_prefix,
        s3=s3 or _s3_client(parsed),
        http=http or StdlibHttpTransport(timeout_seconds=parsed.http_timeout_seconds),
        max_bytes=parsed.max_bytes,
        upload_part_bytes=parsed.upload_part_bytes,
        copy_part_bytes=parsed.copy_part_bytes,
        max_redirects=parsed.max_redirects,
        range_attempts=parsed.range_attempts,
        retry_initial_backoff_seconds=parsed.retry_initial_backoff_seconds,
        retry_max_backoff_seconds=parsed.retry_max_backoff_seconds,
        sleeper=sleeper,
        window_start=parsed.window_start,
        window_end=parsed.window_end,
        cursor=parsed.cursor,
        watermark=parsed.watermark,
    )


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    print(canonical_json(result.model_dump(mode="json", by_alias=True)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
