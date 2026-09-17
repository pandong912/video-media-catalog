"""Extraction command-line interface."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from video_media_catalog.canonical import canonical_json
from video_media_catalog.landing import extract_landing
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.runtime_extract import (
    DEFAULT_MAX_EPHEMERAL_BYTES,
    DEFAULT_MAX_MANIFEST_BYTES,
    DEFAULT_MAX_OUTPUT_OBJECT_BYTES,
    DEFAULT_MAX_SOURCE_BYTES,
    RuntimeExtractConfig,
    run_runtime_extract,
)


def build_local_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog extract",
        description="Local development extraction mode.",
    )
    parser.add_argument(
        "--output-uri",
        required=True,
        help="landing root URI (local path or file:// in this release)",
    )
    parser.add_argument(
        "--wikidata-uri",
        help="Wikidata line-oriented JSON dump URI (.json, .gz, or .bz2)",
    )
    parser.add_argument("--eidr-xml-uri", help="offline EIDR XML URI")
    parser.add_argument(
        "--wikidata-sha256", help="optional expected raw source SHA-256"
    )
    parser.add_argument("--eidr-sha256", help="optional expected raw source SHA-256")
    parser.add_argument(
        "--shard-records",
        type=int,
        default=50_000,
        help="maximum records per deterministic shard (default: 50000)",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog",
        description="Strict control-plane media catalog extraction worker.",
    )
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument("--manifest-hash", required=True)
    parser.add_argument("--manifest-version", required=True)
    parser.add_argument("--manifest-etag", required=True)
    parser.add_argument("--manifest-size", required=True, type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-spec-id", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--executor-image", required=True)
    parser.add_argument("--shard-records", type=int, default=50_000)
    parser.add_argument(
        "--max-manifest-bytes",
        type=int,
        default=DEFAULT_MAX_MANIFEST_BYTES,
    )
    parser.add_argument(
        "--max-source-bytes",
        type=int,
        default=DEFAULT_MAX_SOURCE_BYTES,
    )
    parser.add_argument(
        "--max-ephemeral-bytes",
        type=int,
        default=DEFAULT_MAX_EPHEMERAL_BYTES,
    )
    parser.add_argument(
        "--max-output-object-bytes",
        type=int,
        default=DEFAULT_MAX_OUTPUT_OBJECT_BYTES,
    )
    parser.add_argument("--ephemeral-dir", type=Path)
    return parser


def _local_main(argv: list[str]) -> int:
    args = build_local_parser().parse_args(argv)
    summary = extract_landing(
        output_uri=args.output_uri,
        wikidata_uri=args.wikidata_uri,
        eidr_xml_uri=args.eidr_xml_uri,
        shard_records=args.shard_records,
        wikidata_sha256=args.wikidata_sha256,
        eidr_sha256=args.eidr_sha256,
    )
    print(
        canonical_json(
            summary.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["extract"]:
        return _local_main(arguments[1:])
    parsed = build_parser().parse_args(arguments)
    runtime = RuntimeArguments.model_validate(
        {field: getattr(parsed, field) for field in RuntimeArguments.model_fields}
    )
    summary = run_runtime_extract(
        runtime,
        config=RuntimeExtractConfig(
            shard_records=parsed.shard_records,
            max_manifest_bytes=parsed.max_manifest_bytes,
            max_source_bytes=parsed.max_source_bytes,
            max_ephemeral_bytes=parsed.max_ephemeral_bytes,
            max_output_object_bytes=parsed.max_output_object_bytes,
            ephemeral_dir=parsed.ephemeral_dir,
        ),
    )
    print(
        canonical_json(
            summary.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
