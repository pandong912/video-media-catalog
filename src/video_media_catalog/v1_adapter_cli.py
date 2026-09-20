"""CLI for replayable Wikidata and EIDR v1-to-v2 adapters."""

from __future__ import annotations

import argparse
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.connector_publish import DEFAULT_RECORD_SHARD_BYTES
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.v1_adapters import capture_v1_adapter

DEFAULT_MAX_SOURCE_BYTES = 2 * 1024**4


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-v1-adapter",
        description="Wrap a verified Wikidata or EIDR v1 source in v2 contracts.",
    )
    parser.add_argument("--source", required=True, choices=("wikidata", "eidr"))
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--input-hash", required=True)
    parser.add_argument("--input-size", required=True, type=int)
    parser.add_argument("--input-version", default="")
    parser.add_argument("--input-etag", default="")
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument("--coverage-id", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--acquired-at")
    parser.add_argument("--eidr-complete-snapshot", action="store_true")
    parser.add_argument(
        "--max-source-bytes",
        type=int,
        default=DEFAULT_MAX_SOURCE_BYTES,
    )
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    return parser


def _input_object(parsed: argparse.Namespace) -> ObjectRef:
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        parsed.input_hash,
    )
    if match is None:
        raise ValueError("input hash must contain 64 SHA-256 hex digits")
    scheme = urlsplit(parsed.input_uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError("input URI must use file:// or s3://")
    version = parsed.input_version.strip() or None
    etag = parsed.input_etag.strip().strip('"') or None
    if scheme == "s3" and (version is None or etag is None):
        raise ValueError("S3 input requires --input-version and --input-etag")
    media_type = (
        "application/xml" if parsed.source == "eidr" else "application/octet-stream"
    )
    return ObjectRef(
        uri=parsed.input_uri,
        format="OBJECT_FORMAT_OTHER",
        media_type=media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=parsed.input_size,
        etag=etag,
        object_version=version,
    )


def run(parsed: argparse.Namespace) -> dict[str, object]:
    if parsed.max_source_bytes < 1 or parsed.record_shard_bytes < 1:
        raise ValueError("source and shard byte limits must be positive")
    source = _input_object(parsed)
    schemes = {
        urlsplit(source.uri).scheme,
        urlsplit(parsed.destination_prefix).scheme,
    }
    if not schemes <= {"file", "s3"}:
        raise ValueError("destination prefix must use file:// or s3://")
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if schemes == {"file"} else None,
    )
    store.verify(source, max_bytes=parsed.max_source_bytes)
    acquired_at = parsed.acquired_at or datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    config_digest = sha256_digest(
        canonical_json(
            {
                "connector": f"{parsed.source}-v2-adapter",
                "version": "1.0.0",
                "coverageId": parsed.coverage_id,
                "eidrCompleteSnapshot": parsed.eidr_complete_snapshot,
                "recordShardBytes": parsed.record_shard_bytes,
            }
        )
    )
    with tempfile.TemporaryDirectory(prefix=f"{parsed.source}-v2-adapter-") as tmp:
        materialized = store.download(
            source,
            Path(tmp) / "source",
            max_bytes=parsed.max_source_bytes,
        )
        result = capture_v1_adapter(
            source=parsed.source,
            input_path=materialized.path,
            raw_object=source,
            destination_prefix=parsed.destination_prefix,
            acquired_at=acquired_at,
            image_digest=parsed.image_digest,
            config_digest=config_digest,
            coverage_id=parsed.coverage_id,
            store=store,
            eidr_complete_snapshot=parsed.eidr_complete_snapshot,
            record_shard_bytes=parsed.record_shard_bytes,
        )
    return {
        "batchId": result.batch_manifest.batch_id,
        "batchManifest": result.batch_manifest_object.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "recordSetId": result.record_set_manifest.record_set_id,
        "recordSetManifest": result.record_set_manifest_object.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "recordCount": result.record_set_manifest.record_count,
    }


def main() -> None:
    print(canonical_json(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
