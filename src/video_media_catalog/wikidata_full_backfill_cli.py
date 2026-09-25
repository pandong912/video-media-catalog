"""Spark CLI for profiled or confirmed Wikidata full-media backfills."""

from __future__ import annotations

import argparse
import os
import uuid
from collections.abc import Sequence
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.wikidata_backfill_io import (
    dump_reference,
    join_s3,
    load_or_build_normalized_staging,
    normalization_locations,
    parse_s3_prefix,
    s3_client,
    spark_session,
    spark_uri,
)
from video_media_catalog.wikidata_full_backfill import (
    DEFAULT_MAX_SHARD_BYTES,
    DEFAULT_PARTITIONS_PER_EPOCH,
    DEFAULT_SHARDS_PER_PARTITION,
    DEFAULT_TARGET_SHARD_BYTES,
    FullMediaBackfillConfig,
    WikidataFullMediaBackfillCommit,
    WikidataFullMediaProfile,
    full_media_build_digest,
)
from video_media_catalog.wikidata_full_backfill_publish import (
    full_media_output_root,
    load_existing_full_media_commit,
    publish_full_media_backfill,
)
from video_media_catalog.wikidata_full_backfill_spark import (
    build_full_media_backfill,
    write_full_media_staging,
)
from video_media_catalog.wikidata_spark import configure_bfs_materialize_dir
from video_media_catalog.wikidata_sync import DEFAULT_MAX_DUMP_BYTES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-wikidata-full-media",
        description=(
            "Profile or explicitly run the distributed Wikidata full-media "
            "selector and sharded connector backfill."
        ),
    )
    parser.add_argument("--dump-uri", required=True)
    parser.add_argument("--dump-sha256", required=True)
    parser.add_argument("--dump-size", required=True, type=int)
    parser.add_argument("--dump-version", required=True)
    parser.add_argument("--dump-etag", required=True)
    parser.add_argument("--staging-prefix", required=True)
    parser.add_argument(
        "--output-prefix",
        help="required only for mode=backfill",
    )
    parser.add_argument(
        "--mode",
        choices=("profile", "backfill"),
        default="profile",
        help="profile is the safe default and publishes no connector artifacts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="explicit alias for profile mode",
    )
    parser.add_argument(
        "--confirm-full-backfill",
        action="store_true",
        help="mandatory safety gate for mode=backfill",
    )
    parser.add_argument("--image-digest", required=True)
    parser.add_argument(
        "--target-shard-bytes",
        type=int,
        default=DEFAULT_TARGET_SHARD_BYTES,
    )
    parser.add_argument(
        "--max-shard-bytes",
        type=int,
        default=DEFAULT_MAX_SHARD_BYTES,
    )
    parser.add_argument(
        "--max-shards-per-partition",
        type=int,
        default=DEFAULT_SHARDS_PER_PARTITION,
    )
    parser.add_argument(
        "--max-partitions-per-epoch",
        type=int,
        default=DEFAULT_PARTITIONS_PER_EPOCH,
    )
    parser.add_argument("--max-epochs", type=int, default=4096)
    parser.add_argument("--max-closure-iterations", type=int, default=64)
    parser.add_argument(
        "--max-dump-bytes",
        type=int,
        default=DEFAULT_MAX_DUMP_BYTES,
    )
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--master", help="optional Spark master, e.g. local[2]")
    parser.add_argument(
        "--app-name",
        default="video-media-catalog-wikidata-full-media",
    )
    parser.add_argument("--spark-packages")
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("default", "web-identity"),
        default="web-identity",
    )
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    return parser


def _acquired_at(dump_date: str) -> str:
    return f"{dump_date[0:4]}-{dump_date[4:6]}-{dump_date[6:8]}T00:00:00Z"


def _config(parsed: argparse.Namespace) -> FullMediaBackfillConfig:
    return FullMediaBackfillConfig(
        max_closure_iterations=parsed.max_closure_iterations,
        target_shard_bytes=parsed.target_shard_bytes,
        max_shard_bytes=parsed.max_shard_bytes,
        max_shards_per_partition=parsed.max_shards_per_partition,
        max_partitions_per_epoch=parsed.max_partitions_per_epoch,
        max_epochs=parsed.max_epochs,
    )


def _validate_mode(parsed: argparse.Namespace) -> str:
    mode = "profile" if parsed.dry_run else parsed.mode
    if parsed.dry_run and parsed.mode == "backfill":
        raise ValueError("--dry-run cannot be combined with --mode backfill")
    if mode == "backfill":
        if not parsed.confirm_full_backfill:
            raise ValueError(
                "full backfill is disabled without --confirm-full-backfill"
            )
        if not parsed.output_prefix:
            raise ValueError("--output-prefix is required for a full backfill")
    return mode


def run(
    parsed: argparse.Namespace,
    *,
    s3: Any | None = None,
    spark: Any | None = None,
) -> WikidataFullMediaProfile | WikidataFullMediaBackfillCommit:
    mode = _validate_mode(parsed)
    if parsed.max_dump_bytes < 1:
        raise ValueError("max-dump-bytes must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")
    config = _config(parsed)
    client = s3 or s3_client(parsed)
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=client,
    )
    dump, dump_date = dump_reference(parsed, s3=client, store=store)
    build_digest = full_media_build_digest(
        dump=dump,
        config_digest=config.digest,
        image_digest=parsed.image_digest,
    )
    output_root: str | None = None
    if mode == "backfill":
        output_root = full_media_output_root(
            str(parsed.output_prefix),
            dump_date=dump_date,
            build_digest=build_digest,
        )
        existing = load_existing_full_media_commit(
            output_root=output_root,
            expected_build_digest=build_digest,
            store=store,
            s3=client,
        )
        if existing is not None:
            return existing

    staging_prefix = parse_s3_prefix(parsed.staging_prefix)
    normalized_data, normalized_marker = normalization_locations(
        staging_prefix,
        dump,
    )
    attempt = join_s3(
        staging_prefix,
        "full-media",
        f"dump-sha256={dump.checksum.value}",
        f"build-sha256={build_digest.removeprefix('sha256:')}",
        f"attempt={uuid.uuid4().hex}",
    )
    bfs_uri = spark_uri(join_s3(attempt, "bfs-materialize").uri)
    record_staging = join_s3(attempt, "record-shards")
    summary_staging = join_s3(attempt, "shard-summaries")
    owns_spark = spark is None
    session = spark or spark_session(parsed)
    normalized = None
    build = None
    summaries = None
    configure_bfs_materialize_dir(session, bfs_uri)
    try:
        normalized = load_or_build_normalized_staging(
            spark=session,
            s3=client,
            store=store,
            dump=dump,
            data=normalized_data,
            marker=normalized_marker,
        )
        build = build_full_media_backfill(
            session,
            normalized,
            dump=dump,
            config=config,
            image_digest=parsed.image_digest,
            dump_date=dump_date,
            acquired_at=_acquired_at(dump_date),
        )
        if mode == "profile":
            return build.profile
        assert output_root is not None
        summaries = write_full_media_staging(
            build,
            record_uri=spark_uri(record_staging.uri),
            summary_uri=spark_uri(summary_staging.uri),
        )
        store.verify(dump, max_bytes=parsed.max_dump_bytes)
        return publish_full_media_backfill(
            build=build,
            summaries=summaries,
            config=config,
            record_staging_uri=record_staging.uri,
            output_root=output_root,
            control_prefix=str(parsed.output_prefix),
            dump_date=dump_date,
            store=store,
            s3=client,
        )
    finally:
        if summaries is not None:
            summaries.unpersist()
        if build is not None:
            build.unpersist()
        if normalized is not None:
            normalized.unpersist()
        if owns_spark:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    print(
        canonical_json(result.model_dump(mode="json", by_alias=True, exclude_none=True))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
