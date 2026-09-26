"""CLI for manual, bounded Europeana OAI-PMH metadata capture."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.europeana_oai import (
    CONTROL_OBJECT_MAX_BYTES,
    DEFAULT_EUROPEANA_USER_AGENT,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_PAGE_BYTES,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_RECORD_BYTES,
    DEFAULT_MAX_RECORDS,
    DEFAULT_MAX_RETRY_AFTER_SECONDS,
    DEFAULT_MAX_TOTAL_BYTES,
    DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS,
    DEFAULT_RECORD_SHARD_BYTES,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    EuropeanaOAIClient,
    capture_europeana_oai,
    read_europeana_watermark,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore

SOURCE_WATERMARK_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.source-watermark.v1+json"
)
MAX_URI_LENGTH = 2_048


def _add_object_ref_args(parser: argparse.ArgumentParser, prefix: str) -> None:
    dashed = prefix.replace("_", "-")
    parser.add_argument(f"--{dashed}-uri")
    parser.add_argument(f"--{dashed}-hash")
    parser.add_argument(f"--{dashed}-size", type=int)
    parser.add_argument(f"--{dashed}-version", default="")
    parser.add_argument(f"--{dashed}-etag", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-europeana-oai",
        description=(
            "Capture one bounded Europeana OAI-PMH EDM window as PARTIAL/DELTA "
            "metadata. The connector is keyless, fixed-origin, manual-only, and "
            "never downloads linked media."
        ),
    )
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument("--acquired-at", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--set-spec")
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--user-agent", default=DEFAULT_EUROPEANA_USER_AGENT)
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--minimum-request-interval-seconds",
        type=float,
        default=DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS,
    )
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument(
        "--retry-initial-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--retry-max-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=float,
        default=DEFAULT_MAX_RETRY_AFTER_SECONDS,
    )
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument(
        "--max-page-bytes",
        type=int,
        default=DEFAULT_MAX_PAGE_BYTES,
    )
    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=DEFAULT_MAX_TOTAL_BYTES,
    )
    parser.add_argument(
        "--max-record-bytes",
        type=int,
        default=DEFAULT_MAX_RECORD_BYTES,
    )
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    _add_object_ref_args(parser, "checkpoint")
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    return parser


def _validate_object_uri(value: str, *, label: str) -> str:
    normalized = value.strip()
    parsed = urlsplit(normalized)
    if (
        len(normalized) > MAX_URI_LENGTH
        or parsed.scheme not in {"file", "s3"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must be a bounded file:// or s3:// URI")
    if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
        raise ValueError(f"{label} must identify an S3 object")
    if parsed.scheme == "file" and not parsed.path:
        raise ValueError(f"{label} must identify a local path")
    return normalized


def _checkpoint_ref(parsed: argparse.Namespace) -> ObjectRef | None:
    uri_value = parsed.checkpoint_uri
    hash_value = parsed.checkpoint_hash
    size_value = parsed.checkpoint_size
    version = str(parsed.checkpoint_version or "").strip()
    etag = str(parsed.checkpoint_etag or "").strip().strip('"')
    supplied = (
        uri_value is not None,
        hash_value is not None,
        size_value is not None,
        bool(version),
        bool(etag),
    )
    if not any(supplied):
        return None
    if uri_value is None or hash_value is None or size_value is None:
        raise ValueError("checkpoint URI, hash, and size must be supplied together")
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        str(hash_value).strip(),
    )
    if match is None:
        raise ValueError("checkpoint hash must contain 64 SHA-256 hex digits")
    size = int(size_value)
    if not 0 < size <= CONTROL_OBJECT_MAX_BYTES:
        raise ValueError("checkpoint size is outside the control-object bound")
    uri = _validate_object_uri(str(uri_value), label="checkpoint URI")
    scheme = urlsplit(uri).scheme
    if scheme == "s3" and (not version or not etag):
        raise ValueError("S3 checkpoint requires VersionId and ETag")
    if scheme == "file" and (version or etag):
        raise ValueError("file checkpoint cannot declare S3 metadata")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=SOURCE_WATERMARK_MEDIA_TYPE,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=size,
        etag=etag or None,
        object_version=version or None,
    )


def run(parsed: argparse.Namespace) -> dict[str, object]:
    destination = _validate_object_uri(
        parsed.destination_prefix,
        label="destination prefix",
    ).rstrip("/")
    checkpoint_ref = _checkpoint_ref(parsed)
    all_local = urlsplit(destination).scheme == "file" and (
        checkpoint_ref is None or urlsplit(checkpoint_ref.uri).scheme == "file"
    )
    store = (
        BoundedObjectStore(client=object())
        if all_local
        else BoundedObjectStore(
            region=parsed.aws_region,
            endpoint_url=parsed.s3_endpoint,
            path_style_access=parsed.s3_path_style_access,
        )
    )
    resume = (
        None
        if checkpoint_ref is None
        else read_europeana_watermark(reference=checkpoint_ref, store=store)
    )
    config_values = {
        "connector": "europeana-oai-pmh",
        "version": "1.0.0",
        "setSpec": parsed.set_spec,
        "windowStart": parsed.window_start,
        "windowEnd": parsed.window_end,
        "requestTimeoutSeconds": parsed.request_timeout_seconds,
        "minimumRequestIntervalSeconds": parsed.minimum_request_interval_seconds,
        "maxAttempts": parsed.max_attempts,
        "retryInitialBackoffSeconds": parsed.retry_initial_backoff_seconds,
        "retryMaxBackoffSeconds": parsed.retry_max_backoff_seconds,
        "maxRetryAfterSeconds": parsed.max_retry_after_seconds,
        "maxPages": parsed.max_pages,
        "maxRecords": parsed.max_records,
        "maxPageBytes": parsed.max_page_bytes,
        "maxTotalBytes": parsed.max_total_bytes,
        "maxRecordBytes": parsed.max_record_bytes,
        "recordShardBytes": parsed.record_shard_bytes,
        "mediaBinaryAcquisition": "DISABLED",
        "sourceCompleteness": "PARTIAL",
    }
    config_digest = sha256_digest(canonical_json(config_values))
    client = EuropeanaOAIClient(
        user_agent=parsed.user_agent,
        request_timeout_seconds=parsed.request_timeout_seconds,
        minimum_request_interval_seconds=parsed.minimum_request_interval_seconds,
        max_attempts=parsed.max_attempts,
        max_page_bytes=parsed.max_page_bytes,
        max_records_per_page=parsed.max_records,
        max_record_bytes=parsed.max_record_bytes,
        retry_initial_backoff_seconds=parsed.retry_initial_backoff_seconds,
        retry_max_backoff_seconds=parsed.retry_max_backoff_seconds,
        max_retry_after_seconds=parsed.max_retry_after_seconds,
    )
    result = capture_europeana_oai(
        destination_prefix=destination,
        acquired_at=parsed.acquired_at,
        window_start=parsed.window_start,
        window_end=parsed.window_end,
        image_digest=parsed.image_digest,
        config_digest=config_digest,
        fetcher=client,
        store=store,
        set_spec=parsed.set_spec,
        resume_watermark=resume,
        max_pages=parsed.max_pages,
        max_records=parsed.max_records,
        max_page_bytes=parsed.max_page_bytes,
        max_total_bytes=parsed.max_total_bytes,
        max_record_bytes=parsed.max_record_bytes,
        record_shard_bytes=parsed.record_shard_bytes,
    )
    return {
        "sourceCompleteness": "PARTIAL",
        "changeSemantics": "DELTA",
        "mediaBinaryAcquisition": "DISABLED",
        "terminal": result.terminal,
        "nextResumptionTokenPresent": result.next_resumption_token is not None,
        "pageCount": result.page_count,
        "recordCount": result.record_count,
        "retryCount": result.retry_count,
        "rateLimitCount": result.rate_limit_count,
        "totalRawBytes": result.total_raw_bytes,
        "batchId": result.capture.batch_manifest.batch_id,
        "batchManifest": result.capture.batch_manifest_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "recordSetId": result.capture.record_set_manifest.record_set_id,
        "recordSetManifest": result.capture.record_set_manifest_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "watermarkId": result.control.source_watermark.watermark_id,
        "watermark": result.control.source_watermark_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "receiptId": result.control.receipt.receipt_id,
        "receipt": result.control.receipt_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    print(canonical_json(run(parsed)))
    return 0


if __name__ == "__main__":
    main()
